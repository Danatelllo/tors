package raft

import (
	"errors"
	"fmt"
	"log"
	"raft/raft/storage"
	"raft/raft/storage/proto"
	"sync"
	"sync/atomic"
)

type NodeType int64

const (
	Follower NodeType = iota
	Candidate
	Leader
)

type Log struct {
	Term    int64
	Message *proto.Log_LogMessage
}

type Node struct {
	PersistentStoragePath string
	NodeId                int64
	NodeCount             int
	NodeAddresses         []string

	//////////////////////////////////////

	CurrentTerm  int64
	VotedFor     int64
	CommitLength int64

	CurrentRole   int64
	CurrentLeader int64

	Log      []Log
	LogMutex sync.Mutex

	VotesRecieved      map[int64]struct{}
	VotesRecievedMutex sync.Mutex

	SentLength      map[string]int64
	SentLengthMutex sync.Mutex

	AckedLength      map[string]int64
	AckedLengthMutex sync.Mutex

	//////////////////////////////////////

	RoundRobinCounter      int
	Logger                 *log.Logger
	RoundRobinCounterMutex sync.Mutex

	NodesLiveness      map[string]bool
	NodesLivenessMutex sync.Mutex

	NodesDropTraffic      map[int]struct{}
	NodesDropTrafficMutex sync.Mutex
}

func (n *Node) NeedDropTraffic(id int) bool {
	n.NodesDropTrafficMutex.Lock()
	defer n.NodesDropTrafficMutex.Unlock()

	_, ok := n.NodesDropTraffic[id]
	return ok
}

func (n *Node) AddToDropTraffic(id int) {
	n.NodesDropTrafficMutex.Lock()
	defer n.NodesDropTrafficMutex.Unlock()

	n.NodesDropTraffic[id] = struct{}{}
}

func (n *Node) UndropTraffic() {
	n.NodesDropTrafficMutex.Lock()
	defer n.NodesDropTrafficMutex.Unlock()

	n.NodesDropTraffic = map[int]struct{}{}
}

func (n *Node) IsMaster() bool {
	if atomic.LoadInt64(&n.CurrentRole) == int64(Leader) {
		return true
	}
	return false
}

func (n *Node) WriteState() error {
	var storageData proto.Storage

	storageData.CommitLength = atomic.LoadInt64(&n.CommitLength)
	storageData.CurrentTerm = atomic.LoadInt64(&n.CurrentLeader)
	storageData.CommitLength = atomic.LoadInt64(&n.CommitLength)

	n.LogMutex.Lock()
	defer n.LogMutex.Unlock()
	for _, log := range n.Log {
		var protoLog proto.Log
		protoLog.Message = log.Message
		protoLog.Term = uint64(log.Term)

		storageData.Logs = append(storageData.Logs, &protoLog)
	}

	return storage.WriteProtoToFile(n.PersistentStoragePath, &storageData)
}

func (n *Node) GetNextAddress() (string, error) {
	n.RoundRobinCounterMutex.Lock()
	defer n.RoundRobinCounterMutex.Unlock()

	n.Logger.Printf("alive nodes %v", n.NodesLiveness)

	initialCounter := n.RoundRobinCounter

	for {
		address := n.NodeAddresses[n.RoundRobinCounter%len(n.NodeAddresses)]
		n.RoundRobinCounter++

		if n.NodesLiveness[address] {
			return address, nil
		}

		if n.RoundRobinCounter%len(n.NodeAddresses) == initialCounter {
			break
		}
	}

	return "", errors.New("all nodes are dead")
}

func (n *Node) FillFieldsFromPersistentState() error {
	storage, err := storage.ReadProtoFromFile(n.PersistentStoragePath)
	if err != nil {
		return err
	}

	n.CurrentTerm = int64(storage.CurrentTerm)
	n.VotedFor = int64(storage.VotedFor)

	for _, logEntry := range storage.Logs {
		n.Log = append(n.Log, Log{
			Term:    int64(logEntry.Term),
			Message: logEntry.Message,
		})
	}

	n.CommitLength = int64(storage.CommitLength)

	return nil
}

func (n *Node) NodeIdToAddress(id int64) string {
	return fmt.Sprintf("http://127.0.0.%v:8080", id)
}

func NewNode(nodeId int64, nodeCount int, persistentStoragePath string) *Node {
	node := &Node{
		PersistentStoragePath: persistentStoragePath,
		NodeId:                nodeId,
		NodeCount:             nodeCount,
		CurrentRole:           int64(Follower),
		CurrentLeader:         0,
		VotesRecieved:         make(map[int64]struct{}),
		SentLength:            map[string]int64{},
		AckedLength:           map[string]int64{},
		NodesLiveness:         map[string]bool{},
		NodesDropTraffic:      map[int]struct{}{},

		Logger: log.Default(),
	}

	node.Logger.SetPrefix(fmt.Sprintf("NodeId %v   ", nodeId))
	node.FillFieldsFromPersistentState()

	for j := int64(2); j < int64(node.NodeCount+2); j++ {
		if j != node.NodeId {
			node.NodeAddresses = append(node.NodeAddresses, node.NodeIdToAddress(j))
			node.NodesLiveness[node.NodeIdToAddress(j)] = true
		}
	}

	return node
}
