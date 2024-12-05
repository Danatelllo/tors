package application

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"raft/raft"
	"raft/raft/storage/proto"
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
	if s.Raft.Node.CurrentRole == raft.Leader {
		c.IndentedJSON(http.StatusFound, gin.H{"Location": s.Raft.Node.GetNextAddress()})
		return
	}
	var req GetReq
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("can not parse body: %v", err)})
		return
	}

	s.Raft.ValuesMutex.Lock()
	defer s.Raft.ValuesMutex.Unlock()
	val, ok := s.Raft.Values[req.Key]
	if ok {
		c.IndentedJSON(http.StatusOK, val)
		return
	}

	c.IndentedJSON(http.StatusNotFound, gin.H{"message": "value not found"})
}

func (s *Server) createValue(c *gin.Context) {
	if s.Raft.Node.CurrentRole != raft.Leader {
		c.IndentedJSON(http.StatusFound, gin.H{"Location": s.Raft.Node.GetNextAddress()})
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
}

func New(nodeId int, nodeCount int, storagePath string) *Server {

	router := gin.Default()

	var s *Server = &Server{
		addr:   fmt.Sprintf("127.0.0.%v:8080", nodeId),
		Router: router,
		Raft:   raft.NewServer(nodeId, nodeCount, storagePath),
	}

	router.POST("/vote_req", s.Raft.HandleVoteReq)
	router.POST("/log_req", s.Raft.HandleLogReq)
	router.GET("/is_master", s.Raft.HandleIsMaster)

	router.POST("/create", s.createValue)
	router.GET("/read", s.readValue)
	// router.PUT("/update", s.updateValue)
	// router.PATCH("/patch", s.patchValue)
	// router.DELETE("/delete", s.deleteValue)

	return s
}

func (s *Server) Run() {
	s.Router.Run(s.addr)
}
