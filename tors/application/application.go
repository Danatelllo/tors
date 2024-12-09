package application

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"raft/raft"
	"raft/raft/storage/proto"

	"github.com/gin-gonic/gin"
)

type CreateReq struct {
	Value raft.Value `json:"value"`
	Key   string     `json:"key"`
}

type UpdateReq struct {
	Value raft.Value `json:"value"`
	Key   string     `json:"key"`
}

type GetReq struct {
	Key string `form:"key" binding:"required"`
}

type PatchReq struct {
	Data *string `json:"data"`
	Cnt  *int32  `json:"cnt"`
	Ok   *bool   `json:"ok"`
	Key  string  `json:"key"`
}

type DeleteReq struct {
	Key string `json:"key"`
}

type DropTrafficReq struct {
	NodeId int `json:"id"`
}

func (s *Server) readValue(c *gin.Context) {
	var req GetReq
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	if s.Raft.Node.IsMaster() {
		addr, err := s.Raft.Node.GetNextAddress()
		if err != nil {
			c.IndentedJSON(http.StatusNotFound, gin.H{"error": "not found alive follower"})
			return
		}
		c.Header("Location", addr+"/read?key="+req.Key)

		c.IndentedJSON(http.StatusFound, gin.H{"message": "not a master"})

		s.Raft.Node.Logger.Printf("NextAddress %v", addr)
		return
	}

	s.Raft.ValuesMutex.Lock()
	defer s.Raft.ValuesMutex.Unlock()

	s.Raft.Node.Logger.Printf("%v Values, %v key, %v log, %v values", s.Raft.Values[req.Key], req.Key, s.Raft.Node.Log, s.Raft.Values)

	val, ok := s.Raft.Values[req.Key]

	s.Raft.Node.Logger.Printf("Val %v", val)

	if ok {
		c.IndentedJSON(http.StatusOK, val)
		return
	}

	c.IndentedJSON(http.StatusNotFound, gin.H{"message": "value not found"})
}

func (s *Server) createValue(c *gin.Context) {
	if !s.Raft.Node.IsMaster() {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Is not a master"})
		return
	}

	var req CreateReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	s.Raft.ValuesMutex.Lock()
	_, ok := s.Raft.Values[req.Key]
	if ok {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "can not create exists object"})
		s.Raft.ValuesMutex.Unlock()
	}

	s.Raft.ValuesMutex.Unlock()

	var newLog proto.Log_LogMessage
	newLog.Value = &proto.Log_LogMessage_Value{}

	newLog.QueryType = proto.Log_LogMessage_CREATE
	newLog.Key = req.Key

	newLog.Value.Cnt = int32(req.Value.Cnt)
	newLog.Value.Data = req.Value.Data
	newLog.Value.Ok = req.Value.Ok

	var commitHappened bool = s.Raft.HandleNewLogEntry(&newLog, c)
	if !commitHappened {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create value, unable to commit message to log"})
		return
	}

	c.JSON(http.StatusOK, req.Value)
}

func (s *Server) updateValue(c *gin.Context) {
	var req UpdateReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	s.Raft.ValuesMutex.Lock()
	_, ok := s.Raft.Values[req.Key]
	if !ok {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "value not found"})
		s.Raft.ValuesMutex.Unlock()
	}
	s.Raft.ValuesMutex.Unlock()

	var newLog proto.Log_LogMessage
	newLog.Value = &proto.Log_LogMessage_Value{}

	newLog.QueryType = proto.Log_LogMessage_UPDATE
	newLog.Key = req.Key

	newLog.Value.Cnt = int32(req.Value.Cnt)
	newLog.Value.Data = req.Value.Data
	newLog.Value.Ok = req.Value.Ok

	s.Raft.Node.Logger.Printf("UPDATE CALL %v, %v", newLog, req)
	var commitHappened bool = s.Raft.HandleNewLogEntry(&newLog, c)
	if !commitHappened {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update value, unable to commit message to log"})
		return
	}

	c.JSON(http.StatusOK, req.Value)
}

func (s *Server) patchValue(c *gin.Context) {
	var req PatchReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	var newLog proto.Log_LogMessage
	newLog.Value = &proto.Log_LogMessage_Value{}

	newLog.QueryType = proto.Log_LogMessage_PATCH
	newLog.Key = req.Key

	s.Raft.ValuesMutex.Lock()
	value, ok := s.Raft.Values[req.Key]
	if !ok {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "value not found"})
		s.Raft.ValuesMutex.Unlock()
	}
	s.Raft.ValuesMutex.Unlock()

	newLog.Value.Cnt = int32(value.Cnt)
	newLog.Value.Data = value.Data
	newLog.Value.Ok = value.Ok
	if ok {
		if req.Data != nil {
			newLog.Value.Data = *req.Data
		}

		if req.Cnt != nil {
			newLog.Value.Cnt = *req.Cnt
		}

		if req.Ok != nil {
			newLog.Value.Ok = *req.Ok
		}
	}

	var commitHappened bool = s.Raft.HandleNewLogEntry(&newLog, c)
	if !commitHappened {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update value, unable to commit message to log"})
		return
	}

	c.JSON(http.StatusOK, newLog.Value)
}

func (s *Server) deleteValue(c *gin.Context) {
	var req DeleteReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	s.Raft.ValuesMutex.Lock()
	value, ok := s.Raft.Values[req.Key]
	if !ok {
		c.IndentedJSON(http.StatusNotFound, gin.H{"message": "value not found"})
		s.Raft.ValuesMutex.Unlock()
		return
	}
	s.Raft.ValuesMutex.Unlock()

	var newLog proto.Log_LogMessage
	newLog.Value = &proto.Log_LogMessage_Value{}

	newLog.QueryType = proto.Log_LogMessage_DELETE
	newLog.Key = req.Key

	var commitHappened bool = s.Raft.HandleNewLogEntry(&newLog, c)

	if !commitHappened {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update value, unable to commit message to log"})
		return
	}

	c.JSON(http.StatusOK, value)
}

type Server struct {
	addr   string
	Router *gin.Engine
	Raft   *raft.Server
	Svr    *http.Server
}

func (s *Server) Shutdown(c *gin.Context) {
	s.Svr.Shutdown(context.Background())
	c.IndentedJSON(http.StatusAccepted, nil)
}

func (s *Server) RunAgain(c *gin.Context) {
	go s.Run()
}

func (s *Server) DropTraffic(c *gin.Context) {
	var req DropTrafficReq
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	s.Raft.Node.AddToDropTraffic(req.NodeId)
	c.IndentedJSON(http.StatusOK, nil)
}

func (s *Server) UndropTraffic(c *gin.Context) {
	s.Raft.Node.UndropTraffic()
	c.IndentedJSON(http.StatusOK, nil)
}

func New(nodeId int, nodeCount int, storagePath string) *Server {
	router := gin.Default()

	var s *Server = &Server{
		addr:   fmt.Sprintf("127.0.0.%v:8080", nodeId),
		Router: router,
		Raft:   raft.NewServer(nodeId, nodeCount, storagePath),
	}

	// TODO: need create router in raft and then forward to the application
	router.POST("/vote_req", s.Raft.HandleVoteReq)
	router.POST("/log_req", s.Raft.HandleLogReq)
	router.GET("/is_master", s.Raft.HandleIsMaster)

	router.POST("/drop_traffic", s.DropTraffic)
	router.POST("/unblock_drop_traffic", s.UndropTraffic)

	router.POST("/create", s.createValue)
	router.GET("/read", s.readValue)
	router.PUT("/update", s.updateValue)
	router.PATCH("/patch", s.patchValue)
	router.DELETE("/delete", s.deleteValue)

	s.Svr = &http.Server{
		Addr:    s.addr,
		Handler: s.Router,
	}

	return s
}

func (s *Server) Run() {
	if err := s.Svr.ListenAndServe(); err != nil {
		log.Printf("listen: %s\n", err)
	}
}
