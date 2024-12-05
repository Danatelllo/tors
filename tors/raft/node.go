package raft

import (
	"fmt"
	"log"
	"raft/raft/storage"
	"raft/raft/storage/proto"
)

type Me int

type NodeType int

const (
	Follower NodeType = iota
	Candidate
	Leader
)

type Log struct {
	Term    int
	Message *proto.Log_LogMessage
}

type Node struct {
	PersistentStoragePath string
	NodeId                int
	NodeCount             int
	NodeAdresses          []string

	CurrentTerm  int
	VotedFor     int
	Log          []Log
	CommitLength int

	CurrentRole   NodeType
	CurrentLeader int
	VotesRecieved map[int]struct{}
	SentLength    map[string]int
	AckedLength   map[string]int

	RoundRobinCounter int
	Logger            *log.Logger
}

func (n *Node) GetNextAddress() string {
	address := n.NodeAdresses[n.RoundRobinCounter%len(n.NodeAdresses)]
	n.RoundRobinCounter++
	return address
}

func (n *Node) FillFieldsFromPersistentState() error {
	storage, err := storage.ReadProtoFromFile(n.PersistentStoragePath)
	if err != nil {
		return err
	}

	n.CurrentTerm = int(storage.CurrentTerm)
	n.VotedFor = int(storage.VotedFor)

	for _, logEntry := range storage.Logs {
		n.Log = append(n.Log, Log{
			Term:    int(logEntry.Term),
			Message: logEntry.Message,
		})
	}

	n.CommitLength = int(storage.CommitLength)

	return nil
}

func (n *Node) NodeIdToAddress(id int) string {
	return fmt.Sprintf("http://127.0.0.%v:8080", id)
}

func NewNode(nodeId int, nodeCount int, persistentStoragePath string) *Node {
	node := &Node{
		PersistentStoragePath: persistentStoragePath,
		NodeId:                nodeId,
		NodeCount:             nodeCount,
		CurrentRole:           Follower,
		CurrentLeader:         0,
		VotesRecieved:         make(map[int]struct{}),
		SentLength:            map[string]int{},
		AckedLength:           map[string]int{},

		Logger: log.Default(),
	}

	node.Logger.SetPrefix(fmt.Sprintf("NodeId %v   ", nodeId))
	node.FillFieldsFromPersistentState()

	for j := 1; j < node.NodeCount+1; j++ {
		if j != node.NodeId {
			node.NodeAdresses = append(node.NodeAdresses, node.NodeIdToAddress(j))
		}
	}

	return node
}
