package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"raft/raft/storage/proto"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
)

type Value struct {
	Data string `json:"data"`
	Cnt  int    `json:"cnt"`
	Ok   bool   `json:"ok"`
}

type Server struct {
	Node               *Node
	NodeMutex          sync.Mutex
	updateReceivedChan chan bool
	Values             map[string]Value
	ValuesMutex        sync.Mutex
}

type VoteReq struct {
	NodeId           int `json:"NodeId"`
	CurrentTerm      int `json:"CurrentTerm"`
	CurrentLogLength int `json:"CurrentLogLength"`
	CurrentLogTerm   int `json:"CurrentLogTerm"`
}

type VoteRsp struct {
	NodeId      int `json:"NodeId"`
	CurrentTerm int `json:"CurrentTerm"`
}

type LogReq struct {
	LeaderId     int   `json:"LeaderId"`
	CurrentTerm  int   `json:"CurrentTerm"`
	LogLength    int   `json:"LogLength"`
	PrevLogTerm  int   `json:"PrevLogTerm"`
	CommitLength int   `json:"CommitLength"`
	Entries      []Log `json:"Entries"`
}

type LogRsp struct {
	NodeId      int `json:"NodeId"`
	CurrentTerm int `json:"CurrentTerm"`
	Ack         int `json:"Ack"`
}

func (s *Server) ReplicateLog(followerAddr string, logAckChan chan *resty.Response) {
	var i int = s.Node.SentLength[followerAddr]
	var prevLogTerm int = 0

	if i > 0 {
		prevLogTerm = int(s.Node.Log[i-1].Term)
	}

	var entries []Log
	for j := i; j < int(len(s.Node.Log)); j++ {
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
		return
	}
	logAckChan <- resp
}

func (s *Server) ReplicateLogsImpl(addresses []string, commitHappened *bool, calledInElections bool) {
	s.Node.Logger.Println("Replicate Log")
	var wg sync.WaitGroup

	var logAckChan chan *resty.Response = make(chan *resty.Response, len(addresses))
	for _, nodeAddr := range addresses {
		if calledInElections {
			s.Node.AckedLength[nodeAddr] = 0
			s.Node.SentLength[nodeAddr] = len(s.Node.Log)
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

		if rsp.CurrentTerm == s.Node.CurrentTerm && s.Node.CurrentRole == Leader {
			var addr string = s.Node.NodeIdToAddress(rsp.NodeId)
			if rsp.Ack >= s.Node.AckedLength[s.Node.NodeIdToAddress(rsp.NodeId)] && msg.IsSuccess() {
				s.Node.SentLength[addr] = rsp.Ack
				s.Node.AckedLength[addr] = rsp.Ack
				s.CommitLogEntries()
				*commitHappened = true
			} else if s.Node.SentLength[addr] > 0 {
				s.Node.SentLength[addr] = s.Node.SentLength[addr] - 1
				addressesForReplicateLogs = append(addressesForReplicateLogs, addr)
			}
		} else if rsp.CurrentTerm > s.Node.CurrentTerm {
			s.Node.CurrentTerm = rsp.CurrentTerm
			s.Node.CurrentRole = Follower
			s.Node.VotedFor = 0
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
		if s.Node.CurrentRole == Leader {
			s.Node.Logger.Print("HandleBackgroundReplicateLog")
			s.NodeMutex.Lock()
			s.ReplicateLogs(s.Node.NodeAdresses, false)
			s.NodeMutex.Unlock()
			time.Sleep(time.Second)
		}
	}
}

func (s *Server) Acks(length int) int {
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

	var maxReady int
	for len := len(s.Node.Log) - 1; len > 0; len-- {
		acks := s.Acks(len)
		if acks > minAcks {
			maxReady = acks
			break
		}
	}

	if maxReady != 0 && maxReady > s.Node.CommitLength && s.Node.Log[maxReady].Term == s.Node.CurrentTerm {
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
			}
		}
		s.Node.CommitLength = maxReady
	}
	s.Node.Logger.Printf("New log %v", s.Node.Log)
}

// LogLength = master.SentLength
// CommitLength = master.CommitLength
// entries = log [master.SentLength .... len(master.Log)]
func (s *Server) AppendEntries(logLength int, leaderCommit int, entries []Log) {
	// if terms are not equal previous master need cat s.Node.Log because new master has others data
	if len(entries) > 0 && len(s.Node.Log) > int(logLength) {
		if s.Node.Log[logLength].Term != entries[0].Term {
			s.Node.Log = s.Node.Log[:logLength]
		}
	}
	if logLength+int(len(entries)) > int(len(s.Node.Log)) {
		for j := int(len(s.Node.Log)) - logLength; j < int(len(entries)); j++ {
			s.Node.Log = append(s.Node.Log, entries[j])
		}
	}
	if leaderCommit > s.Node.CommitLength {
		for j := int(s.Node.CommitLength); j < leaderCommit; j++ {
			switch s.Node.Log[j].Message.QueryType {
			case proto.Log_LogMessage_CREATE:
				var v Value
				v.Cnt = int(s.Node.Log[j].Message.Value.Cnt)
				v.Data = s.Node.Log[j].Message.Value.Data
				v.Ok = s.Node.Log[j].Message.Value.Ok
				s.ValuesMutex.Lock()
				defer s.ValuesMutex.Unlock()
				s.Values[s.Node.Log[j].Message.Key] = v
			}
		}
		s.Node.CommitLength = leaderCommit
	}
}

func (s *Server) HandleVoteReq(c *gin.Context) {
	var req VoteReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}
	s.Node.Logger.Printf("Recieved elect message from %v", req.NodeId)

	s.NodeMutex.Lock()
	defer s.NodeMutex.Unlock()
	var myLogTerm int = 0
	if len(s.Node.Log) > 0 {
		myLogTerm = s.Node.Log[len(s.Node.Log)-1].Term
	}

	// log term of new candidate more or not less than follower
	var logOk bool = (req.CurrentLogTerm > myLogTerm) || (req.CurrentLogTerm == myLogTerm && req.CurrentLogLength >= int(len(s.Node.Log)))

	// term of new candidate more or this follower already hasf voted for him
	var termOk bool = (req.CurrentTerm > s.Node.CurrentTerm) || (req.CurrentTerm == s.Node.CurrentTerm && req.NodeId == s.Node.VotedFor)

	if logOk && termOk {
		s.Node.CurrentTerm = req.CurrentTerm
		s.Node.CurrentRole = Follower
		s.Node.VotedFor = req.NodeId

		fmt.Printf("Node %v accepted new candidate %v", s.Node.NodeId, req.NodeId)

		c.IndentedJSON(http.StatusOK, VoteRsp{s.Node.NodeId, s.Node.CurrentTerm})
		return
	}

	fmt.Printf("Node %v did not accept new candidate %v", s.Node.NodeId, req.NodeId)
	c.IndentedJSON(http.StatusNotAcceptable, VoteRsp{s.Node.NodeId, s.Node.CurrentTerm})
}

func (s *Server) HandleIsMaster(c *gin.Context) {
	s.NodeMutex.Lock()
	defer s.NodeMutex.Unlock()
	if s.Node.CurrentRole == Leader {
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

	s.Node.Logger.Printf("Log request colled LeaderId: %v", req.LeaderId)

	s.NodeMutex.Lock()
	defer s.NodeMutex.Unlock()
	if req.CurrentTerm > s.Node.CurrentTerm {
		s.Node.CurrentTerm = req.CurrentTerm
		s.Node.CurrentRole = Follower
		s.Node.VotedFor = 0
		s.Node.CurrentLeader = req.LeaderId
	} else if req.CurrentTerm == s.Node.CurrentTerm && s.Node.CurrentRole == Candidate {
		s.Node.CurrentRole = Follower
		s.Node.CurrentLeader = req.LeaderId
	}

	var currentLogTerm int = 0
	if len(s.Node.Log) > 0 {
		currentLogTerm = s.Node.Log[len(s.Node.Log)-1].Term
	}

	var logOk bool = (len(s.Node.Log) >= req.LogLength) && (req.LogLength == 0 || req.PrevLogTerm == currentLogTerm)

	// LogLength = master.SentLength
	// CommitLength = master.CommitLength
	// entries = log [master.SentLength .... len(master.Log)]

	if req.CurrentTerm == s.Node.CurrentTerm && logOk {
		// why we append all master log and so calculate ack
		s.AppendEntries(req.LogLength, req.CommitLength, req.Entries)
		var ack int = req.LogLength + int(len(req.Entries))
		c.IndentedJSON(http.StatusOK, LogRsp{s.Node.NodeId, s.Node.CurrentTerm, ack})
	} else {
		s.Node.Logger.Printf("reqTerm: %v, nodeTerm: %v", req.CurrentTerm, s.Node.CurrentTerm)
		c.IndentedJSON(http.StatusConflict, LogRsp{s.Node.NodeId, s.Node.CurrentTerm, 0})
	}

	s.updateReceivedChan <- true
}

func (s *Server) HandleElections() {
	// TODO: Think about election timer
	for {
		var ticker *time.Ticker = time.NewTicker(3 * time.Second)
		select {
		case <-s.updateReceivedChan:
			s.Node.Logger.Println("Update received, resetting timer")
		case <-ticker.C:
			s.NodeMutex.Lock()
			if s.Node.CurrentRole == Leader {
				s.NodeMutex.Unlock()
				break
			}
			s.Node.Logger.Println("No update received for 3 seconds, start elections")

			var wg sync.WaitGroup

			resultChan := make(chan *resty.Response, len(s.Node.NodeAdresses))

			s.Node.CurrentTerm++
			s.Node.CurrentRole = Candidate
			s.Node.VotedFor = s.Node.NodeId
			s.Node.VotesRecieved[s.Node.NodeId] = struct{}{}
			s.Node.SentLength = map[string]int{}
			s.Node.AckedLength = map[string]int{}
			var lastTerm int = 0
			if len(s.Node.Log) > 0 {
				lastTerm = s.Node.Log[len(s.Node.Log)-1].Term
			}
			s.Node.Logger.Printf("Sending elect messages %v", s.Node.NodeAdresses)
			for _, nodeAddr := range s.Node.NodeAdresses {
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
							CurrentTerm:      s.Node.CurrentTerm,
							CurrentLogLength: int(len(s.Node.Log)),
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

				if resp.StatusCode() == http.StatusOK && s.Node.CurrentRole == Candidate && s.Node.CurrentTerm == rsp.CurrentTerm {
					s.Node.Logger.Printf("%v Node voted for me", rsp.NodeId)
					s.Node.VotesRecieved[rsp.NodeId] = struct{}{}
					if len(s.Node.VotesRecieved) >= int(s.Node.NodeCount+2)/2 {
						s.Node.CurrentRole = Leader
						s.Node.CurrentLeader = s.Node.NodeId
						s.ReplicateLogs(s.Node.NodeAdresses, true)
					}
				} else if rsp.CurrentTerm > s.Node.CurrentTerm {
					s.Node.Logger.Println("Found another leader, abort elections")
					s.Node.CurrentTerm = rsp.CurrentTerm
					s.Node.CurrentRole = Follower
					s.Node.VotedFor = 0
					s.NodeMutex.Unlock()
					return
				}
			}
			s.Node.Logger.Println("Finish elections")
			s.NodeMutex.Unlock()
		}
	}
}

func (s *Server) HandleNewLogEntry(newLog *proto.Log_LogMessage, c *gin.Context) bool {
	s.NodeMutex.Lock()
	defer s.NodeMutex.Unlock()
	s.Node.Log = append(s.Node.Log, Log{s.Node.CurrentTerm, newLog})
	s.Node.AckedLength[s.Node.NodeIdToAddress(s.Node.NodeId)] = len(s.Node.Log)
	return s.ReplicateLogs(s.Node.NodeAdresses, false)
}

func NewServer(nodeId int, nodeCount int, storagePath string) *Server {
	log.Printf("Starting server nodeId %v, nodeCount %v", nodeId, nodeCount)

	server := &Server{
		Node:               NewNode(nodeId, nodeCount, storagePath),
		updateReceivedChan: make(chan bool),
		Values:             map[string]Value{},
	}

	go server.HandleElections()

	go server.HandleBackgroundReplicateLog()

	return server
}
