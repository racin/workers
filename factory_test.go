package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestBuildFactory(t *testing.T) {
	wt := NewFactory()
	defer wt.DB.Close()

	numworkers := 100
	for i := 0; i < numworkers; i++ {
		name := fmt.Sprintf("worker%d", i)
		wt.workerList[name] = NewWorker(name)
	}
	wt.updateResponsibility()
	for i := 0; i < numworkers; i++ {
		name := fmt.Sprintf("worker%d", i)
		wt.Workers.Insert(wt.workerList[name])
	}

	// rand.Shuffle(len(wt.Workers), func(i, j int) { wt.Workers[i], wt.Workers[j] = wt.Workers[j], wt.Workers[i] })
	// for _, worker := range wt.Workers {
	// 	tree.Insert(worker)
	// }

	// wt.Workers.Print()
	for i := 0; i < 1000000; i++ {
		var randID = rand.Uint64()

		node := wt.Workers.Find(randID)
		if node == nil {
			t.Fatalf("Node not found for ID %x\n", randID)
			return
		}
	}
	// fmt.Printf("RandID: %x, Node name: %v, Node begin: %x, Node end: %x\n", randID, node.value.Name, node.value.RangeBegin, node.value.RangeEnd)
}

func TestAssignJob(t *testing.T) {
	wt := NewFactory()
	defer wt.DB.Close()

	wt.clearJobs()
	numworkers := 100
	numjobs := 1000
	/*	for i := 0; i < numworkers; i++ {
			name := fmt.Sprintf("worker%d", i)
			wt.workerList[name] = NewWorker(name)
		}
		wt.updateResponsibility()
		for i := 0; i < numworkers; i++ {
			name := fmt.Sprintf("worker%d", i)
			wt.Workers.Insert(wt.workerList[name])
		}*/
	for i := 0; i < numworkers; i++ {
		name := fmt.Sprintf("worker%d", i)
		wt.addWorker(name, "")
	}

	rndJobs := createRandomJobs(numjobs)

	assignedNewJobs := wt.assignJobs(rndJobs)

	newJobs, err := wt.persistJobs(assignedNewJobs)
	if err != nil {
		t.Fatal(err)
	}

	// Verify some basic statistics of the distribution
	jobsDist := make([]int, 0)
	sumJobDist := 0
	for _, jobs := range newJobs {
		//fmt.Printf("Worker: %v, Jobs: %v\n", workername, len(jobs))
		jobsDist = append(jobsDist, len(jobs))
		sumJobDist += len(jobs)
	}

	sort.Ints(jobsDist)
	min, max, median, avg := jobsDist[0], jobsDist[len(jobsDist)-1], jobsDist[int(len(jobsDist)/2)], numjobs/numworkers

	if min == 0 || max == 0 {
		t.Errorf("Min jobs assigned to worker is 0")
	}
	if median < avg-min || median > avg+min {
		t.Errorf("Not evenly distribtued jobs. Median: %v, Avg: %v, Min: %v\n", median, avg, min)
	}
	if sumJobDist != numjobs {
		t.Errorf("Total jobs assigned to workers is not equal to total jobs. Total: %v, NumJobs: %v\n", sumJobDist, numjobs)
	}
	//fmt.Printf("Min: %v, Max: %v, Median: %v\n", min, max, median)

	// Verify that the jobs are assigned to the correct workers
	for _, jobAssign := range assignedNewJobs {
		worker := wt.workerList[jobAssign.CurrentWorker]
		if worker == nil {
			t.Errorf("Job not assigned: %v\n", jobAssign.ID)
			continue
		}
		if worker.RangeBegin > jobAssign.ID || worker.RangeEnd < jobAssign.ID {
			t.Errorf("Job %x assigned to wrong worker %v. RangeBegin: %x, RangeEnd: %x\n", jobAssign.ID, worker.Name, worker.RangeBegin, worker.RangeEnd)
		}
	}

	// Same verification for whats in the database
	iter := wt.DB.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		jobAssign := &JobAssign{}
		err := json.Unmarshal(iter.Value(), &jobAssign)
		if err != nil {
			t.Errorf("Error unmarshalling jobassign: %v\n", err)
		}
		worker := wt.workerList[jobAssign.CurrentWorker]
		if worker == nil {
			t.Errorf("Job not assigned: %v\n", jobAssign.ID)
			continue
		}
		if worker.RangeBegin > jobAssign.ID || worker.RangeEnd < jobAssign.ID {
			t.Errorf("Job %x assigned to wrong worker %v. RangeBegin: %x, RangeEnd: %x\n", jobAssign.ID, worker.Name, worker.RangeBegin, worker.RangeEnd)
		}
	}

	// Finally same verification for whats returned by persistJobs
	for newWorker, jobs := range newJobs {
		worker := wt.workerList[newWorker]
		if worker == nil {
			t.Errorf("Jobs are not assigned: %v\n", jobs)
			continue
		}
		for _, job := range jobs {
			if worker.RangeBegin > job.ID || worker.RangeEnd < job.ID {
				t.Errorf("Job %x assigned to wrong worker %v. RangeBegin: %x, RangeEnd: %x\n", job.ID, worker.Name, worker.RangeBegin, worker.RangeEnd)
			}
		}
	}
}

func TestDeleteJob(t *testing.T) {
	wt := NewFactory()
	defer wt.DB.Close()

	wt.clearJobs()
	numworkers := 10
	numjobs := 100
	for i := 0; i < numworkers; i++ {
		name := fmt.Sprintf("worker%d", i)
		wt.workerList[name] = NewWorker(name)
	}
	wt.updateResponsibility()
	for i := 0; i < numworkers; i++ {
		name := fmt.Sprintf("worker%d", i)
		wt.Workers.Insert(wt.workerList[name])
	}

	rndJobs := createRandomJobs(numjobs)
	assignedNewJobs := wt.assignJobs(rndJobs)

	newJobs, err := wt.persistJobs(assignedNewJobs)
	if err != nil {
		t.Fatal(err)
	}

	jobsDist := make([]int, 0)
	sumJobDist := 0
	for _, jobs := range newJobs {
		jobsDist = append(jobsDist, len(jobs))
		sumJobDist += len(jobs)
	}

	rndWorker := fmt.Sprintf("worker%d", rand.Intn(numworkers))
	sort.Ints(jobsDist)
	jobsToDel := make([]Job, 0)

	for i := 0; i < jobsDist[0]; i++ {
		jobsToDel = append(jobsToDel, Job{ID: newJobs[rndWorker][i].ID})
	}

	wt.deleteJobs(jobsToDel)

	iter := wt.DB.NewIterator(nil, nil)
	defer iter.Release()
	jobsDistAfterDelMap := make(map[string]int, 0)
	for iter.Next() {
		jobassign := &JobAssign{}
		err := json.Unmarshal(iter.Value(), &jobassign)
		if err != nil {
			t.Error(err)
		}
		jobsDistAfterDelMap[jobassign.CurrentWorker] += 1
	}
	jobsDistAfterDel := make([]int, 0)
	sumJobDistAfterDel := 0
	for _, lenjobs := range jobsDistAfterDelMap {
		jobsDistAfterDel = append(jobsDistAfterDel, lenjobs)
		sumJobDistAfterDel += lenjobs
	}
	sort.Ints(jobsDistAfterDel)

	if sumJobDist != sumJobDistAfterDel+jobsDist[0] {
		t.Errorf("Total jobs assigned to workers is not equal to total jobs. Total: %v, NumJobs: %v\n", sumJobDist, sumJobDistAfterDel+jobsDist[0])
	}
}
