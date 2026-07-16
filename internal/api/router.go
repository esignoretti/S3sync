package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/esignoretti/bucketsync/internal/config"
)

type Server struct {
	repo *config.Repository
}

func NewServer(repo *config.Repository) *Server {
	return &Server{repo: repo}
}

func (s *Server) Router() *gin.Engine {
	r := gin.Default()

	api := r.Group("/api")
	{
		api.POST("/buckets", s.createBucket)
		api.GET("/buckets", s.listBuckets)
		api.GET("/buckets/:id", s.getBucket)
		api.PUT("/buckets/:id", s.updateBucket)
		api.DELETE("/buckets/:id", s.deleteBucket)

		api.POST("/sync-pairs", s.createSyncPair)
		api.GET("/sync-pairs", s.listSyncPairs)
		api.GET("/sync-pairs/:id", s.getSyncPair)
		api.PUT("/sync-pairs/:id", s.updateSyncPair)
		api.DELETE("/sync-pairs/:id", s.deleteSyncPair)
		api.POST("/sync-pairs/:id/sync", s.triggerSync)
		api.GET("/sync-pairs/:id/status", s.syncStatus)

		api.GET("/health", s.health)
		api.GET("/version", s.version)
	}

	r.GET("/", s.serveWeb)
	r.StaticFS("/static", http.FS(webStatic))

	return r
}
