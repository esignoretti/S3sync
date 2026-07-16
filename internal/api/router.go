package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/esignoretti/S3sync/internal/cache"
	"github.com/esignoretti/S3sync/internal/config"
	"github.com/esignoretti/S3sync/internal/s3client"
	"github.com/esignoretti/S3sync/internal/sync"
)

type Server struct {
	repo        *config.Repository
	cache       *cache.Store
	setupStates map[string]*config.SetupState
	engines     map[string]*sync.Engine
	rootCtx     context.Context
}

func NewServer(repo *config.Repository, cachePath string) (*Server, error) {
	c, err := cache.Open(cachePath)
	if err != nil {
		return nil, fmt.Errorf("open cache: %w", err)
	}
	return &Server{
		repo:        repo,
		cache:       c,
		setupStates: make(map[string]*config.SetupState),
		engines:     make(map[string]*sync.Engine),
	}, nil
}

func (s *Server) Close() {
	s.cache.Close()
}

func (s *Server) SetRootContext(ctx context.Context) {
	s.rootCtx = ctx
}

func (s *Server) RegisterEngine(pairID string, e *sync.Engine) {
	s.engines[pairID] = e
}

// createEngine builds an Engine from the pair ID, registers it, and returns it.
// Returns an error if bucket config or S3 client creation fails.
func (s *Server) createEngine(pairID string) (*sync.Engine, error) {
	pair, err := s.repo.GetSyncPair(pairID)
	if err != nil {
		return nil, fmt.Errorf("get pair: %w", err)
	}
	src, err := s.repo.GetBucket(pair.SourceBucketID)
	if err != nil {
		return nil, fmt.Errorf("get source bucket: %w", err)
	}
	tgt, err := s.repo.GetBucket(pair.TargetBucketID)
	if err != nil {
		return nil, fmt.Errorf("get target bucket: %w", err)
	}
	srcS3, err := s3client.NewClient(src)
	if err != nil {
		return nil, fmt.Errorf("create s3 client for source: %w", err)
	}
	tgtS3, err := s3client.NewClient(tgt)
	if err != nil {
		return nil, fmt.Errorf("create s3 client for target: %w", err)
	}

	engine := sync.NewEngine(pair, src, tgt, srcS3, tgtS3, s.cache)
	s.RegisterEngine(pairID, engine)
	return engine, nil
}

// runPairSync runs one sync cycle for the given pair using the shared cache.
// Creates and registers an engine if one does not already exist.
func (s *Server) runPairSync(ctx context.Context, pairID string) error {
	eng, ok := s.engines[pairID]
	if !ok {
		var err error
		eng, err = s.createEngine(pairID)
		if err != nil {
			return fmt.Errorf("create engine: %w", err)
		}
	}
	return eng.RunOnce(ctx)
}

// StartEngineLoop creates an engine for the given pair and runs its
// periodic-sync loop until the pair is disabled or ctx is cancelled.
func (s *Server) StartEngineLoop(ctx context.Context, p config.SyncPair) error {
	engine, err := s.createEngine(p.ID)
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}

	ticker := time.NewTicker(time.Duration(p.SyncInterval) * time.Second)
	slog.Info("engine loop started", "pair", p.Name, "interval", p.SyncInterval)

	go func() {
		defer ticker.Stop()
		defer engine.Stop()

		// Run once immediately on start
		engine.RunOnce(ctx)
		_, _, status, _, _ := engine.Status()
		if pair, err := s.repo.GetSyncPair(p.ID); err == nil {
			pair.LastSyncStatus = status
			s.repo.UpdateSyncPair(pair)
		}

		for {
			select {
			case <-ticker.C:
				currentPair, err := s.repo.GetSyncPair(p.ID)
				if err != nil || !currentPair.Enabled {
					slog.Info("engine loop: pair disabled", "pair", p.Name)
					return
				}
				if err := engine.RunOnce(ctx); err != nil {
					slog.Error("engine loop: sync failed", "pair", p.Name, "error", err)
				}
				_, _, status, _, _ := engine.Status()
				if pair, err := s.repo.GetSyncPair(p.ID); err == nil {
					pair.LastSyncStatus = status
					s.repo.UpdateSyncPair(pair)
				}
			case <-ctx.Done():
				slog.Info("engine loop: shutting down", "pair", p.Name)
				return
			}
		}
	}()

	return nil
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
