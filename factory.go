package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type JobAssign struct {
	Job
	CurrentWorker string
}

func (j JobAssign) SameJobSimple(other Job) bool {
	return j.ID == other.ID
}

func (j JobAssign) SameJob(other JobAssign) bool {
	return j.ID == other.ID
}

func (j JobAssign) SameJobAssign(other JobAssign) bool {
	return j.ID == other.ID && j.CurrentWorker == other.CurrentWorker
}

type Worker struct {
	Name       string
	RangeBegin uint64
	RangeEnd   uint64
	Address    string
}

type Factory struct {
	mutex      sync.Mutex
	DB         *leveldb.DB
	Workers    *BinaryTree
	workerList map[string]*Worker
}

func NewFactory() *Factory {
	leveldb, err := leveldb.OpenFile("workers", &opt.Options{WriteBuffer: 1 << 8})
	if err != nil {
		panic(err)
	}

	f := &Factory{
		DB:         leveldb,
		Workers:    NewBinaryTree(),
		workerList: make(map[string]*Worker),
	}
	go f.factoryLoop()
	return f
}

func (f *Factory) factoryLoop() {
	for {
		time.Sleep(time.Second * 30)
		f.printResponsibility()
	}
}
func NewWorker(name string) *Worker {
	return &Worker{
		Name: name,
	}
}

func (f *Factory) AddWorker(rw http.ResponseWriter, rq *http.Request) {
	name := rq.URL.Query().Get("name")
	if name == "" {
		rw.Write([]byte("No name provided"))
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	port := strings.LastIndex(rq.RemoteAddr, ":")
	remStr := rq.RemoteAddr[:port]
	fmt.Printf("Got request to add worker %s. Addr: %s\n", name, remStr)
	if err := f.addWorker(name, remStr); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}
	rw.WriteHeader(http.StatusOK)
	return
}

var ipbracketreplacer *strings.Replacer = strings.NewReplacer("[", "", "]", "")

func (f *Factory) addWorker(name, host string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if _, ok := f.workerList[name]; ok {
		return fmt.Errorf("Worker %s already exists", name)
	}
	ip := net.ParseIP(ipbracketreplacer.Replace(host))
	// fmt.Printf("Parsing IP: %s, Host: %s\n", ip.String(), host)
	worker := Worker{Name: name, Address: ip.String() + ":8081"}
	f.workerList[name] = &worker

	f.updateResponsibility()
	return nil
}

func (f *Factory) RemoveWorker(rw http.ResponseWriter, rq *http.Request) {
	name := rq.URL.Query().Get("name")
	fmt.Printf("Got request to remove worker %s \n", name)
	if name == "" {
		fmt.Printf("RemoveWorker: No name provided!")
		return
	}
	if err := f.removeWorker(name); err != nil {
		fmt.Printf("RemoveWorker: %v", err)
		return
	}
	// TODO: Might be excessive for demo.
	if err := f.reAssignAllJobs(); err != nil {
		fmt.Printf("RemoveWorker.reAssignAllJobs: %v", err)
		return
	}
	if err := f.distributeAllWork(); err != nil {
		fmt.Printf("RemoveWorker.distributeAllWork: %v", err)
		return
	}

	rw.WriteHeader(http.StatusOK)
	return
}

func (f *Factory) removeWorker(name string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if _, ok := f.workerList[name]; !ok {
		return fmt.Errorf("Worker %s doesnt exist", name)
	}

	delete(f.workerList, name)
	f.updateResponsibility()

	return nil
}

func (f *Factory) RemoveJobs(rw http.ResponseWriter, rq *http.Request) {
	if rq.Method != "POST" {
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	data, err := io.ReadAll(rq.Body)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}

	jobs := []Job{}
	err = json.Unmarshal(data, &jobs)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}

	deletedJobs, err := f.deleteJobs(jobs)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}

	if err := f.distributeSomeDeletions(deletedJobs); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}
	rw.WriteHeader(http.StatusOK)
}

func (f *Factory) AddRandomJobs(rw http.ResponseWriter, rq *http.Request) {
	countStr := rq.URL.Query().Get("count")
	count, err := strconv.Atoi(countStr)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}

	rndJobs := createRandomJobs(count)
	if err := f.addJobs(rndJobs); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}
}

func (f *Factory) ClearJobs(rw http.ResponseWriter, rq *http.Request) {
	if err := f.clearJobs(); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}
	f.distributeAllWork()
	rw.WriteHeader(http.StatusOK)
}

func (f *Factory) clearJobs() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	batch := new(leveldb.Batch)
	iter := f.DB.NewIterator(nil, nil)

	defer iter.Release()
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	return f.DB.Write(batch, nil)
}

func (f *Factory) AddJobs(rw http.ResponseWriter, rq *http.Request) {
	if rq.Method != "POST" {
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	data, err := io.ReadAll(rq.Body)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}

	jobs := []Job{}
	err = json.Unmarshal(data, &jobs)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}

	if err := f.addJobs(jobs); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}

	rw.WriteHeader(http.StatusOK)
}

func (f *Factory) addJobs(jobs []Job) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	assignedNewJobs := f.assignJobs(jobs)

	newJobs, err := f.persistJobs(assignedNewJobs)
	if err != nil {
		return err
	}

	return f.distributeSomeJobs(newJobs)
}

func (f *Factory) deleteJobs(jobs []Job) ([]JobAssign, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	deletedJobs := []JobAssign{}
	batch := new(leveldb.Batch)
	for _, job := range jobs {
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, job.ID)

		oldJobByte, err := f.DB.Get(idBytes, nil)
		if err != nil && err != leveldb.ErrNotFound {
			return nil, err
		}
		if oldJobByte != nil {
			oldJob := JobAssign{}
			err = json.Unmarshal(oldJobByte, &oldJob)
			if err != nil {
				deletedJobs = append(deletedJobs, JobAssign{job, ""})
			} else {
				deletedJobs = append(deletedJobs, oldJob)
			}
			batch.Delete(idBytes)
		}
	}
	return deletedJobs, f.DB.Write(batch, nil)
}

func (f *Factory) persistJobs(jobs []JobAssign) (map[string][]Job, error) {
	fmt.Printf("Persisting %d jobs\n", len(jobs))
	newJobs := make(map[string][]Job)
	batch := new(leveldb.Batch)
	for _, newJob := range jobs {
		rndBytes, err := json.Marshal(newJob)
		if err != nil {
			return nil, err
		}
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, newJob.ID)

		oldJobByte, err := f.DB.Get(idBytes, nil)
		if err != nil && err != leveldb.ErrNotFound {
			return nil, err
		}
		if oldJobByte != nil {
			oldJob := &JobAssign{}
			err = json.Unmarshal(oldJobByte, &oldJob)
			if err != nil {
				return nil, err
			}
			if oldJob.SameJobAssign(newJob) {
				//fmt.Printf("Job %d already exists\n", newJob.ID)
				continue
			}
		}

		if _, ok := newJobs[newJob.CurrentWorker]; !ok {
			newJobs[newJob.CurrentWorker] = []Job{newJob.Job}
		} else {
			newJobs[newJob.CurrentWorker] = append(newJobs[newJob.CurrentWorker], newJob.Job)
		}
		batch.Put(idBytes, rndBytes)
	}
	return newJobs, f.DB.Write(batch, nil)
}

func createRandomJobs(entries int) []Job {
	rnd := rand.New(rand.NewSource(time.Now().UnixMilli()))
	jobs := make([]Job, entries)
	for i := 0; i < entries; i++ {
		jobs[i] = Job{rnd.Uint64(), rnd.Int()}
	}

	// leveldb.Put([]byte("key"), []byte("value"), nil)
	return jobs
}

func (f *Factory) distributeSomeDeletions(jobsToDel []JobAssign) error {
	batchedJobs := make(map[string][]Job)
	fmt.Printf("Want to delete %d jobs\n", len(jobsToDel))
	for _, job := range jobsToDel {
		var cw string
		if job.CurrentWorker == "" {
			worker := f.Workers.Find(job.ID)
			if worker != nil {
				cw = worker.Value.Address
			} else {
				continue // Don't know where to send this job
			}

		} else {
			cw = job.CurrentWorker
		}
		if _, ok := batchedJobs[cw]; !ok {
			batchedJobs[cw] = make([]Job, 0)
		}
		batchedJobs[cw] = append(batchedJobs[cw], job.Job)
	}
	for workername, jobs := range batchedJobs {
		worker, ok := f.workerList[workername]
		if !ok {
			fmt.Printf("Worker %s not found\n", workername)
			continue
		}
		jobsBytes, err := json.Marshal(jobs)
		if err != nil {
			fmt.Printf("Error marshalling jobs: %s\n", err.Error())
			continue
		}

		fmt.Printf("Sending %d deletions to %s\n", len(jobs), worker.Name)
		_, err = http.Post("http://"+worker.Address+"/deletejobs", "application/json", bytes.NewBuffer(jobsBytes))
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Factory) distributeSomeJobs(allJobs map[string][]Job) error {
	fmt.Printf("distributeSomeJobs for %d workers\n", len(allJobs))
	for workername, workerjobs := range allJobs {
		worker, ok := f.workerList[workername]
		if ok {
			fmt.Printf("Sending %d jobs to %s. Addr: %s\n", len(workerjobs), workername, worker.Address+"/givejobs")
			jobsBytes, err := json.Marshal(workerjobs)
			if err != nil {
				return err
			}
			resp, err := http.Post("http://"+worker.Address+"/givejobs", "application/json", bytes.NewBuffer(jobsBytes))
			if err != nil {
				return err
			}
			respByte, err := ioutil.ReadAll(resp.Body)
			fmt.Printf("Got Response: %s\n", string(respByte))
		} else {
			return fmt.Errorf("Worker %s not found", workername)
		}
	}
	return nil
}

func (f *Factory) DistributeAllWork(rw http.ResponseWriter, rq *http.Request) {
	if err := f.reAssignAllJobs(); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
	}
	if err := f.distributeAllWork(); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
	}
	rw.WriteHeader(http.StatusOK)
}

func (f *Factory) reAssignAllJobs() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	iter := f.DB.NewIterator(nil, nil)
	defer iter.Release()

	f.updateResponsibility()

	allJobs := make([]Job, 0)
	for iter.Next() {
		jobassign := JobAssign{}
		err := json.Unmarshal(iter.Value(), &jobassign)
		if err != nil {
			return err
		}
		allJobs = append(allJobs, jobassign.Job)
	}
	assignedNewJobs := f.assignJobs(allJobs)
	_, err := f.persistJobs(assignedNewJobs)

	return err
}
func (f *Factory) distributeAllWork() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	batchedJobs := make(map[string][]Job)

	iter := f.DB.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		jobassign := &JobAssign{}
		err := json.Unmarshal(iter.Value(), &jobassign)
		if err != nil {
			return err
		}

		if _, ok := batchedJobs[jobassign.CurrentWorker]; !ok {
			batchedJobs[jobassign.CurrentWorker] = make([]Job, 0)
		}
		batchedJobs[jobassign.CurrentWorker] = append(batchedJobs[jobassign.CurrentWorker], jobassign.Job)
	}

	for workername, worker := range f.workerList {

		jobs, ok := batchedJobs[workername]
		if !ok {
			jobs = []Job{}
		}
		jobsBytes, err := json.Marshal(jobs)
		if err != nil {
			fmt.Printf("Error marshalling jobs: %s\n", err.Error())
			continue
		}

		req, err := http.NewRequest("POST", "http://"+worker.Address+"/givejobs", bytes.NewBuffer(jobsBytes))
		if err != nil {
			return err
		}
		req.Header.Add("FullJobs", "true")
		client := &http.Client{}
		_, err = client.Do(req)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Factory) assignJobs(jobs []Job) []JobAssign {
	assignedJobs := make([]JobAssign, len(jobs))

	for i, job := range jobs {
		worker := f.Workers.Find(job.ID)
		if worker == nil {
			fmt.Printf("Cannot assign job %d\n", job.ID)
			if len(f.workerList) > 0 {
				for workername := range f.workerList {
					assignedJobs[i] = JobAssign{job, workername}
					break
				}
			} else {
				assignedJobs[i] = JobAssign{job, ""}
			}
		} else {
			// fmt.Printf("Assigning job to %s\n", worker.Value.Name)
			assignedJobs[i] = JobAssign{job, worker.Value.Name}
		}
	}

	return assignedJobs
}

func (f *Factory) updateResponsibility() {
	lenW := uint64(len(f.workerList))
	if lenW == 0 {
		return
	}
	f.Workers = NewBinaryTree()

	maxRange := ^uint64(0)
	// maxRange := uint64(1 << 63)
	eachWorker := maxRange / lenW

	wlist := make([]string, lenW)
	var count uint64 = 0
	for workername := range f.workerList {
		wlist[count] = workername
		count++
	}
	sort.Strings(wlist)

	for i := uint64(0); i < lenW; i++ {
		worker := f.workerList[wlist[i]]
		worker.RangeBegin = i * eachWorker
		if i == lenW-1 {
			worker.RangeEnd = maxRange
		} else {
			worker.RangeEnd = (i + 1) * eachWorker //min(maxRange, (i+1)*eachWorker)
		}
		f.Workers.Insert(worker)
	}
}

func (f *Factory) printResponsibility() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	lenW := uint64(len(f.workerList))
	wlist := make([]string, lenW)
	var count uint64 = 0
	for workername := range f.workerList {
		wlist[count] = workername
		count++
	}
	sort.Strings(wlist)

	fmt.Printf("--------- Printing worker responsibilities. Date: %v ----------\n", time.Now())
	for i := uint64(0); i < lenW; i++ {
		worker := f.workerList[wlist[i]]
		fmt.Printf("Name: %s, RangeBegin: %x, RangeEnd: %x\n", worker.Name, worker.RangeBegin, worker.RangeEnd)
	}
	fmt.Printf("--------------------------------------------------------------------\n")
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func (f *Factory) PrintFactory() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	iter := f.DB.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		fmt.Printf("Key: %x, Value: %s\n", iter.Key(), iter.Value())
	}

	return
}
