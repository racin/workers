package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"
)

type Job struct {
	ID    uint64
	Value int
}

func (j Job) String() string {
	return fmt.Sprintf("Value: %d, ID: %x (%d)", j.Value, j.ID, j.ID)
}

type Laborer struct {
	Worker
	jobs []Job
	quit <-chan struct{}
	done chan<- struct{}
}

func NewLaborer(name string, quit <-chan struct{}, done chan<- struct{}) (*Laborer, error) {
	l := &Laborer{
		Worker{
			Name: name,
		},
		[]Job{},
		quit,
		done}
	if err := l.beginProcedure(); err != nil {
		return nil, err
	}
	go l.laborLoop()

	return l, nil
}

func (w *Laborer) IncrementValues() {
	for i := range w.jobs {
		w.jobs[i].Value++
	}
}

func (w *Laborer) laborLoop() {
	for {
		select {
		case <-time.After(time.Second * 1):
			w.IncrementValues()
			w.PrintJobs()
		case <-w.quit:
			w.quitProcedure()
			w.done <- struct{}{}
			return
		}
	}
}

func (w *Laborer) beginProcedure() error {
	resp, err := http.Get(masterAddress + "/addworker?name=" + w.Name)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	fmt.Printf("Worker %v added.\n", w.Name)
	return nil
}

func (w *Laborer) quitProcedure() {
	resp, err := http.Get(masterAddress + "/removeworker?name=" + w.Name)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("Worker %v quitting. Graceful exit.\n", w.Name)
}

func (w *Laborer) deletediffjobs(jobs []Job) {
	toDel := len(jobs)
	fmt.Printf("Have %d jobs. Want to delete %d of them.\n", len(w.jobs), len(jobs))
	for i := 0; i < len(w.jobs); i++ {
		if toDel == 0 {
			break
		}
		for j := 0; j < len(jobs); j++ {
			fmt.Printf("w.jobs[%d].ID: %x, jobs[%d].ID: %x\n", i, w.jobs[i].ID, j, jobs[j].ID)
			if w.jobs[i].ID == jobs[j].ID {
				w.jobs = append(w.jobs[:i], w.jobs[i+1:]...)
				i--
				toDel--
				break
			}
		}
	}
	w.PrintJobs()
}

func (w *Laborer) givediffjobs(jobs []Job) {
	w.jobs = append(w.jobs, jobs...)
	sort.Slice(w.jobs, func(i, j int) bool {
		return w.jobs[i].ID < w.jobs[j].ID
	})
	w.PrintJobs()
}

func (w *Laborer) givefulljobs(jobs []Job) {
	w.jobs = jobs
	sort.Slice(w.jobs, func(i, j int) bool {
		return w.jobs[i].ID < w.jobs[j].ID
	})
	w.PrintJobs()
}

func (w *Laborer) PrintJobs() {
	fmt.Printf("--------- Printing jobs. Date: %v ----------\n", time.Now())
	for _, job := range w.jobs {
		fmt.Println(job)
	}
	fmt.Printf("--------------------------------------------------------------------\n")
}

func (w *Laborer) GiveJobs(rw http.ResponseWriter, rq *http.Request) {
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

	if rq.Header.Get("FullJobs") == "true" {
		w.givefulljobs(jobs)
	} else {
		w.givediffjobs(jobs)
	}
	rw.WriteHeader(http.StatusOK)
}

func (w *Laborer) DeleteJobs(rw http.ResponseWriter, rq *http.Request) {
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

	fmt.Printf("Got jobs to delete: %v\n", jobs)
	w.deletediffjobs(jobs)
	rw.WriteHeader(http.StatusOK)
}
