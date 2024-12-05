package main

import (
	"log"
	"os"
	"raft/application"
	"strconv"
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
	s.Run()
}
