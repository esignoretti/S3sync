package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/esignoretti/S3sync/internal/config"
	"github.com/esignoretti/S3sync/internal/sync"
)

type Server struct {
	repo        *config.Repository
	cacheDir    string
	setupStates map[string]*config.SetupState
	engines     map[string]*sync.Engine
}

func NewServer(repo *config.Repository, cacheDir string) *Server {
	return &Server{repo: repo, cacheDir: cacheDir, setupStates: make(map[string]*config.SetupState), engines: make(map[string]*sync.Engine)}
}

func (s *Server) RegisterEngine(pairID string, e *sync.Engine) {
	s.engines[pairID] = e
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
		api.POST("/sync-pairs/:id/disable", s.disableSyncPair)
		api.GET("/sync-pairs/:id/status", s.syncStatus)

		api.GET("/health", s.health)
		api.GET("/version", s.version)

		// Setup wizard
		api.POST("/setup", s.setup)
		api.GET("/setup", s.setupState)
	}

	r.GET("/", s.serveDashboard)
	r.GET("/setup", s.serveSetup)
	r.StaticFS("/static", http.FS(webStatic))

	return r
}
