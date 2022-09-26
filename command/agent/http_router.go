package agent

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"os"
	"time"
)

func (s HTTPServer) router() http.Handler {
	engine := gin.New()
	if s.agent.config.DevMode {
		gin.SetMode(gin.DebugMode)
		// use the log for gin
		engine.Use(gin.Recovery())
	} else {
		gin.SetMode(gin.ReleaseMode)
		logfile, err := os.Create("./jobpool_http_router.log")
		if err != nil {
			s.logger.Warn("Could not create http router log file", "error", err)
		}
		gin.DefaultWriter = io.MultiWriter(logfile)
		engine.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
			return fmt.Sprintf("%s - [%s] %d \"%s %s %s %s \"%s\" %s\"\n",
				param.ClientIP,
				param.TimeStamp.Format(time.RFC3339),
				param.StatusCode,
				param.Method,
				param.Path,
				param.Request.Proto,
				param.Latency,
				param.Request.UserAgent(),
				param.ErrorMessage,
			)
		}))
		engine.Use(gin.Recovery())
	}
	engine.GET("/", func(c *gin.Context) {
		c.JSON(
			http.StatusOK,
			gin.H{
				"code":    http.StatusOK,
				"message": "Welcome to jobpool!",
			},
		)
	})
	engine.GET("/v1/agent/members", func(c *gin.Context) {
		s.deal(c, s.AgentMembersRequest)
	})
	engine.POST("/v1/agent/force-leave", func(c *gin.Context) {
		s.deal(c, s.AgentForceLeaveRequest)
	})
	engine.POST("/v1/agent/join", func(c *gin.Context) {
		s.deal(c, s.AgentJoinRequest)
	})
	engine.GET("/v1/status/leader", func(c *gin.Context) {
		s.deal(c, s.StatusLeaderRequest)
	})
	engine.GET("/v1/regions", func(c *gin.Context) {
		s.deal(c, s.RegionListRequest)
	})
	engine.GET("/v1/nodes", func(c *gin.Context) {
		s.deal(c, s.NodesRequest)
	})
	engine.GET("/v1/nodes/health", func(c *gin.Context) {
		s.deal(c, s.NodesHealth)
	})
	engine.GET("/v1/metrics", func(c *gin.Context) {
		s.deal(c, s.MetricsRequest)
	})
	engine.GET("/v1/client/stats", func(c *gin.Context) {
		s.deal(c, s.ClientStatsRequest)
	})
	engine.GET("/v1/namespaces", func(c *gin.Context) {
		s.deal(c, s.NamespacesRequest)
	})
	engine.POST("/v1/namespaces", func(c *gin.Context) {
		s.deal(c, s.NamespacesAdd)
	})

	kvGroup := engine.Group("/v1/kvs")
	kvGroup.GET("", func(c *gin.Context) {
		s.deal(c, s.KvsRequest)
	})
	kvGroup.POST("", func(c *gin.Context) {
		s.deal(c, s.KvAdd)
	})
	kvGroup.GET("/:key", func(c *gin.Context) {
		s.dealWithParams(c, s.KvDetail)
	})
	kvGroup.POST("/:key", func(c *gin.Context) {
		s.dealWithParams(c, s.KvUpdate)
	})
	kvGroup.DELETE("/:key", func(c *gin.Context) {
		s.dealWithParams(c, s.KvDelete)
	})

	planGroup := engine.Group("/v1/plans")
	planGroup.GET("", func(c *gin.Context) {
		s.deal(c, s.PlanList)
	})
	planGroup.POST("", func(c *gin.Context) {
		s.deal(c, s.PlanSaveOrUpdate)
	})
	planGroup.DELETE("/:planId", func(c *gin.Context) {
		s.dealWithParams(c, s.PlanDelete)
	})
	planGroup.POST("/:planId/online", func(c *gin.Context) {
		s.dealWithParams(c, s.PlanOnline)
	})
	planGroup.POST("/:planId/offline", func(c *gin.Context) {
		s.dealWithParams(c, s.PlanOffline)
	})
	planGroup.GET("/:planId", func(c *gin.Context) {
		s.dealWithParams(c, s.PlanDetail)
	})

	jobGroup := engine.Group("/v1/jobs")
	jobGroup.GET("", func(c *gin.Context) {
		s.deal(c, s.JobList)
	})
	jobGroup.DELETE("/:jobId", func(c *gin.Context) {
		s.dealWithParams(c, s.JobDelete)
	})
	jobGroup.GET("maps/slots", func(c *gin.Context) {
		s.deal(c, s.JobMap)
	})

	engine.GET("/v1/evals", func(c *gin.Context) {
		s.deal(c, s.EvalsRequest)
	})
	engine.GET("/v1/evals/queue", func(c *gin.Context) {
		s.deal(c, s.GetEvalStat)
	})

	engine.GET("/v1/allocations", func(c *gin.Context) {
		s.deal(c, s.AllocationList)
	})
	engine.POST("/v1/allocations/update-status-by-job", func(c *gin.Context) {
		s.deal(c, s.AllocationUpdate)
	})

	engine.GET("/v1/operator/raft/configuration", func(c *gin.Context) {
		s.deal(c, s.OperatorRaftConfiguration)
	})
	return engine
}
