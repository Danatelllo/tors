package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"raft/raft/storage/proto"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	"golang.org/x/exp/rand"
)

type Value struct {
	Data string `json:"data"`
	Cnt  int    `json:"cnt"`
	Ok   bool   `json:"ok"`
}

type Server struct {
	Node               *Node
	updateReceivedChan chan bool
	Values             map[string]Value
	ValuesMutex        sync.Mutex
}

type VoteReq struct {
	NodeId           int64 `json:"NodeId"`
	CurrentTerm      int64 `json:"CurrentTerm"`
	CurrentLogLength int64 `json:"CurrentLogLength"`
	CurrentLogTerm   int64 `json:"CurrentLogTerm"`
}

type VoteRsp struct {
	NodeId      int64 `json:"NodeId"`
	CurrentTerm int64 `json:"CurrentTerm"`
}

type LogReq struct {
	LeaderId     int64 `json:"LeaderId"`
	CurrentTerm  int64 `json:"CurrentTerm"`
	LogLength    int64 `json:"LogLength"`
	PrevLogTerm  int64 `json:"PrevLogTerm"`
	CommitLength int64 `json:"CommitLength"`
	Entries      []Log `json:"Entries"`
}

type LogRsp struct {
	NodeId      int64 `json:"NodeId"`
	CurrentTerm int64 `json:"CurrentTerm"`
	Ack         int64 `json:"Ack"`
}

func (s *Server) ReplicateLog(followerAddr string, logAckChan chan *resty.Response) {
	var i int64 = s.Node.SentLength[followerAddr]
	var prevLogTerm int64 = 0

	if i > 0 {
		prevLogTerm = int64(s.Node.Log[i-1].Term)
	}

	var entries []Log
	for j := i; j < int64(len(s.Node.Log)); j++ {
		entries = append(entries, s.Node.Log[j])
	}

	client := resty.New()
	client.SetTimeout(1 * time.Second)

	resp, err := client.R().
		SetHeader("Accept", "application/json").
		SetHeader("Content", "application/json").
		SetBody(LogReq{
			LeaderId:     s.Node.NodeId,
			CurrentTerm:  s.Node.CurrentTerm,
			LogLength:    i,
			PrevLogTerm:  prevLogTerm,
			CommitLength: s.Node.CommitLength,
			Entries:      entries,
		}).Post(fmt.Sprintf("%v/log_req", followerAddr))

	if err != nil {
		fmt.Printf("Failed to execute request: %v\n", err)
		s.Node.NodesLivenessMutex.Lock()
		s.Node.NodesLiveness[followerAddr] = false
		s.Node.NodesLivenessMutex.Unlock()
		return
	}
	s.Node.NodesLivenessMutex.Lock()
	s.Node.NodesLiveness[followerAddr] = true
	s.Node.NodesLivenessMutex.Unlock()
	logAckChan <- resp
}

func (s *Server) ReplicateLogsImpl(addresses []string, commitHappened *bool, calledInElections bool) {
	s.Node.Logger.Println("Replicate Log")
	var wg sync.WaitGroup

	var logAckChan chan *resty.Response = make(chan *resty.Response, len(addresses))
	for _, nodeAddr := range addresses {
		if calledInElections {
			s.Node.AckedLengthMutex.Lock()
			s.Node.AckedLength[nodeAddr] = 0
			s.Node.AckedLengthMutex.Unlock()

			s.Node.SentLengthMutex.Lock()
			s.Node.SentLength[nodeAddr] = int64(len(s.Node.Log))
			s.Node.SentLengthMutex.Unlock()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.ReplicateLog(nodeAddr, logAckChan)
		}()
	}

	wg.Wait()
	close(logAckChan)
	var rsp LogRsp
	var addressesForReplicateLogs []string
	for msg := range logAckChan {
		if err := json.Unmarshal(msg.Body(), &rsp); err != nil {
			s.Node.Logger.Printf("error: can not parse body: %v", err)
			continue
		}

		if rsp.CurrentTerm == s.Node.CurrentTerm && s.Node.CurrentRole == int64(Leader) {
			var addr string = s.Node.NodeIdToAddress(rsp.NodeId)
			if rsp.Ack >= s.Node.AckedLength[s.Node.NodeIdToAddress(rsp.NodeId)] && msg.IsSuccess() {
				s.Node.SentLengthMutex.Lock()
				s.Node.SentLength[addr] = rsp.Ack
				s.Node.SentLengthMutex.Unlock()

				s.Node.AckedLengthMutex.Lock()
				s.Node.AckedLength[addr] = rsp.Ack
				s.Node.AckedLengthMutex.Unlock()
				s.CommitLogEntries()
				*commitHappened = true
			} else if s.Node.SentLength[addr] > 0 {
				s.Node.SentLength[addr] = s.Node.SentLength[addr] - 1
				addressesForReplicateLogs = append(addressesForReplicateLogs, addr)
			}
		} else if rsp.CurrentTerm > s.Node.CurrentTerm {
			atomic.StoreInt64(&s.Node.CurrentTerm, rsp.CurrentTerm)
			atomic.StoreInt64(&s.Node.CurrentRole, int64(Follower))
			atomic.StoreInt64(&s.Node.VotedFor, 0)
			s.Node.Logger.Printf("I am follower now, found new leader")
			return
		}
	}
	if len(addressesForReplicateLogs) > 0 {
		s.ReplicateLogsImpl(addressesForReplicateLogs, commitHappened, calledInElections)
	}
}

func (s *Server) ReplicateLogs(addresses []string, calledInElections bool) bool {
	var commitHappened bool = false
	s.ReplicateLogsImpl(addresses, &commitHappened, calledInElections)
	return commitHappened
}

func (s *Server) HandleBackgroundReplicateLog() {
	for {
		if atomic.LoadInt64(&s.Node.CurrentRole) == int64(Leader) {
			s.Node.Logger.Print("HandleBackgroundReplicateLog")
			s.ReplicateLogs(s.Node.NodeAddresses, false)
			time.Sleep(time.Second)
		}
	}
}

func (s *Server) Acks(length int64) int {
	count := 0
	for _, nodeLength := range s.Node.AckedLength {
		if nodeLength >= length {
			count++
		}
	}
	return count
}

func (s *Server) CommitLogEntries() {
	var minAcks = (s.Node.NodeCount + 2) / 2
	var ready []int64

	var maxReady int64 = 0
	s.Node.AckedLengthMutex.Lock()
	for j := int64(1); j < int64(len(s.Node.Log)+1); j++ {
		if s.Acks(j) >= minAcks {
			ready = append(ready, j)
			maxReady = max(maxReady, j)
		}
	}
	s.Node.AckedLengthMutex.Unlock()

	if len(ready) != 0 && maxReady > s.Node.CommitLength && s.Node.Log[maxReady-1].Term == s.Node.CurrentTerm {
		for j := s.Node.CommitLength; j < maxReady; j++ {
			switch s.Node.Log[j].Message.QueryType {
			case proto.Log_LogMessage_CREATE:
				var v Value
				v.Cnt = int(s.Node.Log[j].Message.Value.Cnt)
				v.Data = s.Node.Log[j].Message.Value.Data
				v.Ok = s.Node.Log[j].Message.Value.Ok

				s.ValuesMutex.Lock()
				s.Values[s.Node.Log[j].Message.Key] = v
				s.ValuesMutex.Unlock()
			case proto.Log_LogMessage_UPDATE:
				var v Value
				v.Cnt = int(s.Node.Log[j].Message.Value.Cnt)
				v.Data = s.Node.Log[j].Message.Value.Data
				v.Ok = s.Node.Log[j].Message.Value.Ok

				s.Node.Logger.Printf("Update %v, Key %v", v, s.Node.Log[j].Message.Key)
				s.ValuesMutex.Lock()
				_, ok := s.Values[s.Node.Log[j].Message.Key]
				if ok {
					s.Values[s.Node.Log[j].Message.Key] = v
				}
				s.ValuesMutex.Unlock()
			case proto.Log_LogMessage_DELETE:
				s.ValuesMutex.Lock()
				_, ok := s.Values[s.Node.Log[j].Message.Key]
				if ok {
					delete(s.Values, s.Node.Log[j].Message.Key)
				}
				s.ValuesMutex.Unlock()
			case proto.Log_LogMessage_PATCH:
				var v Value
				v.Cnt = int(s.Node.Log[j].Message.Value.Cnt)
				v.Data = s.Node.Log[j].Message.Value.Data
				v.Ok = s.Node.Log[j].Message.Value.Ok

				s.ValuesMutex.Lock()
				_, ok := s.Values[s.Node.Log[j].Message.Key]
				if ok {
					s.Values[s.Node.Log[j].Message.Key] = v
				}
				s.ValuesMutex.Unlock()
			}
		}
		s.Node.CommitLength = maxReady
	}

	s.Node.Logger.Printf("New log %v", s.Node.Log)
}

// LogLength = master.SentLength
// CommitLength = master.CommitLength
// entries = log [master.SentLength .... len(master.Log)]
func (s *Server) AppendEntries(logLength int64, leaderCommit int64, entries []Log) {
	s.Node.LogMutex.Lock()
	defer s.Node.LogMutex.Unlock()
	// if terms are not equal previous master need cat s.Node.Log because new master has others data
	if len(entries) > 0 && len(s.Node.Log) > int(logLength) {
		if s.Node.Log[logLength].Term != entries[0].Term {
			s.Node.Log = s.Node.Log[:logLength]
		}
	}
	if logLength+int64(len(entries)) > int64(len(s.Node.Log)) {
		for j := int64(len(s.Node.Log)) - logLength; j < int64(len(entries)); j++ {
			s.Node.Log = append(s.Node.Log, entries[j])
		}
	}
	var commitLength int64 = atomic.LoadInt64(&s.Node.CommitLength)
	if leaderCommit > commitLength {
		for j := commitLength; j < leaderCommit; j++ {
			switch s.Node.Log[j].Message.QueryType {
			case proto.Log_LogMessage_CREATE:
				var v Value
				v.Cnt = int(s.Node.Log[j].Message.Value.Cnt)
				v.Data = s.Node.Log[j].Message.Value.Data
				v.Ok = s.Node.Log[j].Message.Value.Ok

				s.ValuesMutex.Lock()
				s.Values[s.Node.Log[j].Message.Key] = v
				s.ValuesMutex.Unlock()
			case proto.Log_LogMessage_UPDATE:
				var v Value
				v.Cnt = int(s.Node.Log[j].Message.Value.Cnt)
				v.Data = s.Node.Log[j].Message.Value.Data
				v.Ok = s.Node.Log[j].Message.Value.Ok

				s.ValuesMutex.Lock()
				_, ok := s.Values[s.Node.Log[j].Message.Key]
				if ok {
					s.Values[s.Node.Log[j].Message.Key] = v
				}
				s.Node.Logger.Printf("Update %v, Key %v, OK: %v", v, s.Node.Log[j].Message.Key, ok)
				s.ValuesMutex.Unlock()
			case proto.Log_LogMessage_DELETE:
				s.ValuesMutex.Lock()
				_, ok := s.Values[s.Node.Log[j].Message.Key]
				if ok {
					delete(s.Values, s.Node.Log[j].Message.Key)
				}
				s.ValuesMutex.Unlock()
			case proto.Log_LogMessage_PATCH:
				var v Value
				v.Cnt = int(s.Node.Log[j].Message.Value.Cnt)
				v.Data = s.Node.Log[j].Message.Value.Data
				v.Ok = s.Node.Log[j].Message.Value.Ok

				s.ValuesMutex.Lock()
				_, ok := s.Values[s.Node.Log[j].Message.Key]
				if ok {
					s.Values[s.Node.Log[j].Message.Key] = v
				}
				s.ValuesMutex.Unlock()
			}
		}
		atomic.StoreInt64(&s.Node.CommitLength, leaderCommit)
	}
}

func (s *Server) HandleVoteReq(c *gin.Context) {
	var req VoteReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	if s.Node.NeedDropTraffic(int(req.NodeId)) {
		time.Sleep(1 * time.Second)
		return
	}

	s.Node.Logger.Printf("Recieved elect message from %v", req.NodeId)

	var myLogTerm int64 = 0
	var nodeLength int64 = 0
	if len(s.Node.Log) > 0 {
		s.Node.LogMutex.Lock()
		nodeLength = int64(len(s.Node.Log))
		myLogTerm = s.Node.Log[len(s.Node.Log)-1].Term
		s.Node.LogMutex.Unlock()
	}

	// log term of new candidate more or not less than follower
	var logOk bool = (req.CurrentLogTerm > myLogTerm) || (req.CurrentLogTerm == myLogTerm && req.CurrentLogLength >= nodeLength)

	// term of new candidate more or this follower already hasf voted for him
	var termOk bool = (req.CurrentTerm > atomic.LoadInt64(&s.Node.CurrentTerm)) || (req.CurrentTerm == atomic.LoadInt64(&s.Node.CurrentTerm) && req.NodeId == atomic.LoadInt64(&s.Node.VotedFor))

	if logOk && termOk {
		atomic.StoreInt64(&s.Node.CurrentTerm, req.CurrentTerm)
		atomic.StoreInt64(&s.Node.CurrentRole, int64(Follower))
		s.Node.VotedFor = req.NodeId

		fmt.Printf("Node %v accepted new candidate %v", s.Node.NodeId, req.NodeId)

		c.IndentedJSON(http.StatusOK, VoteRsp{s.Node.NodeId, s.Node.CurrentTerm})
		return
	}

	fmt.Printf("Node %v did not accept new candidate %v, logOk% v, termOk: %v", atomic.LoadInt64(&s.Node.NodeId), req.NodeId, logOk, termOk)
	c.IndentedJSON(http.StatusNotAcceptable, VoteRsp{s.Node.NodeId, atomic.LoadInt64(&s.Node.CurrentTerm)})
}

func (s *Server) HandleIsMaster(c *gin.Context) {
	if s.Node.IsMaster() {
		c.IndentedJSON(http.StatusOK, nil)
		return
	}
	c.IndentedJSON(http.StatusNotFound, nil)
}

func (s *Server) HandleLogReq(c *gin.Context) {
	var req LogReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	if s.Node.NeedDropTraffic(int(req.LeaderId)) {
		time.Sleep(1 * time.Second)
		return
	}

	s.Node.Logger.Printf("Log request colled LeaderId: %v", req.LeaderId)

	if req.CurrentTerm > atomic.LoadInt64(&s.Node.CurrentTerm) {
		atomic.StoreInt64(&s.Node.CurrentTerm, req.CurrentTerm)
		atomic.StoreInt64(&s.Node.CurrentRole, int64(Follower))
		atomic.StoreInt64(&s.Node.VotedFor, 0)
		atomic.StoreInt64(&s.Node.CurrentLeader, req.LeaderId)
	} else if req.CurrentTerm == s.Node.CurrentTerm && atomic.LoadInt64(&s.Node.CurrentRole) == int64(Candidate) {
		s.Node.CurrentRole = int64(Follower)
		s.Node.CurrentLeader = req.LeaderId
	}

	var currentLogTerm int64 = 0
	var nodeLength int64 = 0
	if len(s.Node.Log) > 0 {
		s.Node.LogMutex.Lock()
		currentLogTerm = s.Node.Log[len(s.Node.Log)-1].Term
		nodeLength = int64(len(s.Node.Log))
		s.Node.LogMutex.Unlock()
	}

	var logOk bool = (nodeLength >= req.LogLength) && (req.LogLength == 0 || req.PrevLogTerm == currentLogTerm)

	// LogLength = master.SentLength
	// CommitLength = master.CommitLength
	// entries = log [master.SentLength .... len(master.Log)]

	if req.CurrentTerm == s.Node.CurrentTerm && logOk {
		// why we append all master log and so calculate ack
		s.Node.Logger.Printf("AppendErntries %v", s.Values)
		s.AppendEntries(req.LogLength, req.CommitLength, req.Entries)
		var ack int64 = req.LogLength + int64(len(req.Entries))
		c.IndentedJSON(http.StatusOK, LogRsp{s.Node.NodeId, atomic.LoadInt64(&s.Node.CurrentTerm), ack})
	} else {
		s.Node.Logger.Printf("reqTerm: %v, nodeTerm: %v", req.CurrentTerm, s.Node.CurrentTerm)
		c.IndentedJSON(http.StatusConflict, LogRsp{s.Node.NodeId, atomic.LoadInt64(&s.Node.CurrentTerm), 0})
	}

	s.updateReceivedChan <- true

	s.ValuesMutex.Lock()
	s.Node.Logger.Printf("Valeus %v", s.Values)
	s.ValuesMutex.Unlock()
}

func (s *Server) HandleElections() {
	rand.Seed(uint64(s.Node.NodeId))

	// Генерация случайного смещения от 0 до 3 секунд
	randomOffset := time.Duration(rand.Int63n(3 * int64(time.Second)))

	// Ждем смещение перед началом выборов
	time.Sleep(randomOffset)

	var ticker *time.Ticker = time.NewTicker(3 * time.Second)
	for {
		select {
		case <-s.updateReceivedChan:
			s.Node.Logger.Println("Update received, resetting timer")
			ticker.Stop()
			ticker = time.NewTicker(3 * time.Second)
		case <-ticker.C:
			if atomic.LoadInt64(&s.Node.CurrentRole) == int64(Leader) {
				break
			}

			s.Node.Logger.Println("No update received for 3 seconds, start elections")

			var wg sync.WaitGroup

			resultChan := make(chan *resty.Response, len(s.Node.NodeAddresses))

			atomic.AddInt64(&s.Node.CurrentTerm, 1)
			atomic.StoreInt64(&s.Node.CurrentRole, int64(Candidate))
			atomic.StoreInt64(&s.Node.VotedFor, s.Node.NodeId)

			s.Node.VotesRecievedMutex.Lock()
			s.Node.VotesRecieved[s.Node.NodeId] = struct{}{}
			s.Node.VotesRecievedMutex.Unlock()

			s.Node.SentLengthMutex.Lock()
			s.Node.SentLength = map[string]int64{}
			s.Node.SentLengthMutex.Unlock()

			s.Node.AckedLengthMutex.Lock()
			s.Node.AckedLength = map[string]int64{}
			s.Node.AckedLengthMutex.Unlock()

			s.Node.NodesLivenessMutex.Lock()
			for j := int64(1); j < int64(s.Node.NodeCount+1); j++ {
				if j != s.Node.NodeId {
					s.Node.NodesLiveness[s.Node.NodeIdToAddress(j)] = true
				}
			}
			s.Node.NodesLivenessMutex.Unlock()

			var lastTerm int64 = 0
			var nodeLength int64 = 0
			s.Node.LogMutex.Lock()
			if len(s.Node.Log) > 0 {
				lastTerm = s.Node.Log[len(s.Node.Log)-1].Term
				nodeLength = int64(len(s.Node.Log))
			}
			s.Node.LogMutex.Unlock()

			s.Node.Logger.Printf("Sending elect messages %v", s.Node.NodeAddresses)
			for _, nodeAddr := range s.Node.NodeAddresses {
				s.Node.Logger.Printf("Send elect message for %v", fmt.Sprintf("%v/vote_req", nodeAddr))
				wg.Add(1)
				go func() {
					defer wg.Done()
					client := resty.New()
					client.SetTimeout(1 * time.Second)
					resp, err := client.R().
						SetHeader("Accept", "application/json").
						SetHeader("Content", "application/json").
						SetBody(VoteReq{
							NodeId:           s.Node.NodeId,
							CurrentTerm:      atomic.LoadInt64(&s.Node.CurrentTerm),
							CurrentLogLength: nodeLength,
							CurrentLogTerm:   lastTerm,
						}).Post(fmt.Sprintf("%v/vote_req", nodeAddr))
					if err != nil {
						s.Node.Logger.Printf("Failed to execute request: %v\n", err)
						return
					}
					resultChan <- resp
				}()
			}

			wg.Wait()
			close(resultChan)

			s.Node.Logger.Println("Finished getting answers from nodes")
			for resp := range resultChan {
				var rsp VoteRsp
				if err := json.Unmarshal(resp.Body(), &rsp); err != nil {
					s.Node.Logger.Printf("error: can not parse body: %v", err)
					continue
				}

				if resp.StatusCode() == http.StatusOK && atomic.LoadInt64(&s.Node.CurrentRole) == int64(Candidate) && atomic.LoadInt64(&s.Node.CurrentTerm) == rsp.CurrentTerm {
					s.Node.Logger.Printf("%v Node voted for me", rsp.NodeId)
					s.Node.VotesRecievedMutex.Lock()
					s.Node.VotesRecieved[rsp.NodeId] = struct{}{}
					s.Node.VotesRecievedMutex.Unlock()
					if len(s.Node.VotesRecieved) >= int(s.Node.NodeCount+2)/2 {
						atomic.StoreInt64(&s.Node.CurrentRole, int64(Leader))
						atomic.StoreInt64(&s.Node.CurrentLeader, s.Node.NodeId)

						s.ReplicateLogs(s.Node.NodeAddresses, true)
					}
				} else if rsp.CurrentTerm > s.Node.CurrentTerm {
					s.Node.Logger.Println("Found another leader, abort elections")
					atomic.StoreInt64(&s.Node.CurrentTerm, rsp.CurrentTerm)
					atomic.StoreInt64(&s.Node.CurrentRole, int64(Follower))
					atomic.StoreInt64(&s.Node.VotedFor, int64(Follower))
					break
				}
			}
			s.Node.Logger.Println("Finish elections")
			randomOffset := time.Duration(rand.Int63n(3 * int64(time.Second)))
			time.Sleep(randomOffset)
		}
	}
}

func (s *Server) HandleNewLogEntry(newLog *proto.Log_LogMessage, c *gin.Context) bool {
	s.Node.LogMutex.Lock()
	defer s.Node.LogMutex.Unlock()
	s.Node.Log = append(s.Node.Log, Log{s.Node.CurrentTerm, newLog})

	s.Node.Logger.Printf("HandleNewLogEntry %v", newLog)

	s.Node.AckedLength[s.Node.NodeIdToAddress(s.Node.NodeId)] = int64(len(s.Node.Log))
	var replicateLogRes bool = s.ReplicateLogs(s.Node.NodeAddresses, false)

	return replicateLogRes
}

func NewServer(nodeId int, nodeCount int, storagePath string) *Server {
	log.Printf("Starting server nodeId %v, nodeCount %v", nodeId, nodeCount)

	server := &Server{
		Node:               NewNode(int64(nodeId), nodeCount, storagePath),
		updateReceivedChan: make(chan bool),
		Values:             map[string]Value{},
	}

	go server.HandleElections()

	go server.HandleBackgroundReplicateLog()

	return server
}
