package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

var masterAddress string = "http://localhost:8080"

func initWorker(name string) {
	quit := make(chan struct{})
	done := make(chan struct{}, 1)
	defer close(quit)
	defer close(done)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	masterAddr := os.Getenv("APPMASTERIP")
	if masterAddr != "" {
		masterAddress = "http://" + masterAddr + ":8080"
	}
	labor, err := NewLaborer(name, quit, done)
	if err != nil {
		fmt.Printf("Error initializing worker %s: %s\n", name, err.Error())
		panic(err)
	}
	fmt.Printf("Worker %s initalized. Master addr: %s\n", name, masterAddress)

	go func() {
		http.HandleFunc("/givejobs", labor.GiveJobs)
		http.HandleFunc("/deletejobs", labor.DeleteJobs)
		log.Fatal(http.ListenAndServe(":8081", nil))
	}()

	<-signalCh
	quit <- struct{}{}

	<-done
	fmt.Printf("Finished all tasks. Exiting.\n")
}

func initMaster() {
	factory := NewFactory()
	defer factory.DB.Close()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	fmt.Printf("Master initalized.\n")
	go func() {
		http.HandleFunc("/addworker", factory.AddWorker)
		http.HandleFunc("/removeworker", factory.RemoveWorker)
		http.HandleFunc("/addjobs", factory.AddJobs)
		http.HandleFunc("/clearjobs", factory.ClearJobs)
		http.HandleFunc("/addrandomjobs", factory.AddRandomJobs)
		http.HandleFunc("/removejobs", factory.RemoveJobs)
		http.HandleFunc("/distributeallwork", factory.DistributeAllWork)
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	<-signalCh
	fmt.Printf("Master shutting down.\n")
}

func main() {
	fmt.Println(len(os.Args), os.Args)
	appTask := os.Getenv("APPTASK")
	appName := os.Getenv("APPNAME")
	if len(os.Args) < 1 && appTask == "" {
		fmt.Println("Usage: ./main <master|worker> <workername> . Or use env vars APPTASK and APPNAME.")

		return
	}
	if appTask == "master" || (len(os.Args) > 1 && os.Args[1] == "master") {
		initMaster()
	} else if appTask == "worker" && appName != "" {
		initWorker(appName)
	} else if len(os.Args) > 2 && os.Args[1] == "worker" {
		initWorker(os.Args[2])
	} else {
		fmt.Println("Usage: ./main <master|worker> <workername> . Or use env vars APPTASK and APPNAME.")
	}
}
