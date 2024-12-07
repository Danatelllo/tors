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
	Cnt  *int    `json:"cnt"`
	Ok   *bool   `json:"ok"`
	Key  string  `json:"key"`
}

type DeleteReq struct {
	Key string `json:"key"`
}

func (s *Server) readValue(c *gin.Context) {
	var req GetReq
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	if s.Raft.Node.IsMaster() {
		c.Header("Location", s.Raft.Node.GetNextAddress()+"/read?key="+req.Key)

		c.IndentedJSON(http.StatusFound, gin.H{"message": "not a master"})

		s.Raft.Node.Logger.Printf("NextAddress %v", s.Raft.Node.GetNextAddress())
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

// func (s *Server) updateValue(c *gin.Context) {
// 	var req UpdateReq
// 	if err := c.BindJSON(&req); err != nil {
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
// 		return
// 	}

// 	_, ok := s.Values[req.Key]
// 	if ok {
// 		var newLog proto.Log
// 		newLog.QueryType = proto.Log.LogMessage.QueryType.CREATE
// 		newLog.Key = req.Key
// 		newLog.Value = req.Value

// 		s.Raft.HandleNewLogEntry(newLog, c)
// 		return
// 	}

// 	c.IndentedJSON(http.StatusNotFound, gin.H{"message": "value not found"})
// }

// func (s *Server) patchValue(c *gin.Context) {
// 	var req PatchReq
// 	if err := c.BindJSON(&req); err != nil {
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
// 		return
// 	}

// 	value, ok := s.Values[req.Key]
// 	if ok {
// 		if req.Data != nil {
// 			value.Data = *req.Data
// 		}

// 		if req.Cnt != nil {
// 			value.Cnt = *req.Cnt
// 		}

// 		if req.Ok != nil {
// 			value.Ok = *req.Ok
// 		}

// 		s.Raft.Values[req.Key] = value
// 		c.IndentedJSON(http.StatusOK, value)
// 		return
// 	}

// 	c.IndentedJSON(http.StatusNotFound, gin.H{"message": "value not found"})
// }

// func (s *Server) deleteValue(c *gin.Context) {
// 	var req DeleteReq
// 	if err := c.BindJSON(&req); err != nil {
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
// 		return
// 	}

// 	value, ok := s.Values[req.Key]
// 	if ok {
// 		delete(s.Values, req.Key)
// 		c.IndentedJSON(http.StatusOK, value)
// 		return
// 	}

// 	c.IndentedJSON(http.StatusNotFound, gin.H{"message": "value not found"})
// }

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

	router.POST("/shutdown", s.Shutdown)
	router.POST("/run_again", s.RunAgain)

	router.POST("/create", s.createValue)
	router.GET("/read", s.readValue)
	// router.PUT("/update", s.updateValue)
	// router.PATCH("/patch", s.patchValue)
	// router.DELETE("/delete", s.deleteValue)

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
