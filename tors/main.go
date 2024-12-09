package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"raft/application"
	"strconv"
	"time"
)

func main() {
	args := os.Args
	var id int
	var nodeCount int
	var err error
	if id, err = strconv.Atoi(args[2]); err != nil {
		log.Fatalf("error %v", err)
		return
	}
	if nodeCount, err = strconv.Atoi(args[3]); err != nil {
		log.Fatalf("error %v", err)
		return
	}
	var s *application.Server = application.New(id, nodeCount, args[1])

	go s.Run()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Svr.Shutdown(ctx); err != nil {
		log.Fatal("Server Shutdown:", err)
	}
	log.Println("Server exiting")
}
