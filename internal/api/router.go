package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	gosync "sync"
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
	engineMu    gosync.RWMutex
	setupMu     gosync.RWMutex
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

func (s *Server) GetEngine(pairID string) (*sync.Engine, bool) {
	s.engineMu.RLock()
	defer s.engineMu.RUnlock()
	e, ok := s.engines[pairID]
	return e, ok
}

func (s *Server) SetEngine(pairID string, e *sync.Engine) {
	s.engineMu.Lock()
	defer s.engineMu.Unlock()
	s.engines[pairID] = e
}

func (s *Server) DeleteEngine(pairID string) {
	s.engineMu.Lock()
	defer s.engineMu.Unlock()
	delete(s.engines, pairID)
}

func (s *Server) HasEngine(pairID string) bool {
	s.engineMu.RLock()
	defer s.engineMu.RUnlock()
	_, ok := s.engines[pairID]
	return ok
}

func (s *Server) GetSetupState(sessionID string) (*config.SetupState, bool) {
	s.setupMu.RLock()
	defer s.setupMu.RUnlock()
	st, ok := s.setupStates[sessionID]
	return st, ok
}

func (s *Server) SetSetupState(sessionID string, st *config.SetupState) {
	s.setupMu.Lock()
	defer s.setupMu.Unlock()
	s.setupStates[sessionID] = st
}

// GetOrCreateSetupState returns the existing state for a session or creates a new one.
func (s *Server) GetOrCreateSetupState(sessionID string) *config.SetupState {
	s.setupMu.Lock()
	defer s.setupMu.Unlock()
	st, ok := s.setupStates[sessionID]
	if !ok {
		st = config.NewSetupState()
		s.setupStates[sessionID] = st
	}
	return st
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
	s.SetEngine(pairID, engine)
	return engine, nil
}

// runPairSync runs one sync cycle for the given pair using the shared cache.
// Creates and registers an engine if one does not already exist.
func (s *Server) runPairSync(ctx context.Context, pairID string) error {
	eng, ok := s.GetEngine(pairID)
	if !ok {
		var err error
		eng, err = s.createEngine(pairID)
		if err != nil {
			return fmt.Errorf("create engine: %w", err)
		}
	}
	return eng.RunOnce(ctx)
}

// afterSync persists sync results to DB: status, consecutive errors, sync log.
func (s *Server) afterSync(pairID string, startedAt time.Time, syncErr error) {
	if eng, ok := s.GetEngine(pairID); ok {
		_, _, status, lastError, _ := eng.Status()
		succeeded, failed := eng.LastResult()
		pair, err := s.repo.GetSyncPair(pairID)
		if err != nil {
			return
		}
		now := time.Now().UTC()
		pair.LastSyncAt = &now
		pair.LastSyncStatus = status
		if status == "error" {
			pair.ConsecutiveErrors++
		} else {
			pair.ConsecutiveErrors = 0
		}
		s.repo.UpdateSyncPair(pair)

		errMsg := lastError
		if syncErr != nil {
			errMsg = syncErr.Error()
		}
		s.repo.CreateSyncLog(&config.SyncLogEntry{
			PairID:      pairID,
			Status:      status,
			ErrorMsg:    errMsg,
			Succeeded:   succeeded,
			Failed:      failed,
			StartedAt:   startedAt,
			CompletedAt: now,
		})
	}
}

// StartEngineLoop creates an engine for the given pair and runs its
// periodic-sync loop until the pair is disabled or ctx is cancelled.
// Returns nil if an engine loop is already running for this pair.
func (s *Server) StartEngineLoop(ctx context.Context, p config.SyncPair) error {
	if s.HasEngine(p.ID) {
		slog.Debug("engine loop already running", "pair", p.Name)
		return nil
	}

	engine, err := s.createEngine(p.ID)
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}

	slog.Info("engine loop started", "pair", p.Name, "interval", p.SyncInterval)

	go func() {
		defer engine.Stop()
		defer s.DeleteEngine(p.ID)

		interval := time.Duration(p.SyncInterval) * time.Second

		// First run immediately
		started := time.Now()
		runErr := engine.RunOnce(ctx)
		s.afterSync(p.ID, started, runErr)

		// Then run every interval after each completion
		for {
			timer := time.NewTimer(interval)
			select {
			case <-timer.C:
				currentPair, err := s.repo.GetSyncPair(p.ID)
				if err != nil || !currentPair.Enabled {
					timer.Stop()
					slog.Info("engine loop: pair disabled", "pair", p.Name)
					return
				}
				started := time.Now()
				runErr := engine.RunOnce(ctx)
				if runErr != nil {
					slog.Error("engine loop: sync failed", "pair", p.Name, "error", runErr)
				}
				s.afterSync(p.ID, started, runErr)
			case <-ctx.Done():
				timer.Stop()
				slog.Info("engine loop: shutting down", "pair", p.Name)
				return
			}
		}
	}()

	return nil
}

func (s *Server) recoverCrashedPairs() {
	pairs, err := s.repo.ListSyncPairs()
	if err != nil {
		slog.Warn("recover: list pairs", "error", err)
		return
	}
	for _, p := range pairs {
		if !p.Enabled {
			continue
		}
		if p.LastSyncStatus == "" && p.ConsecutiveErrors > 0 {
			slog.Info("recover: resetting stale pair", "pair", p.Name)
			p.ConsecutiveErrors = 0
			p.LastSyncStatus = "ok"
			if err := s.repo.UpdateSyncPair(&p); err != nil {
				slog.Warn("recover: update pair", "pair", p.Name, "error", err)
			}
		}
	}
}

func (s *Server) Router() *gin.Engine {
	s.recoverCrashedPairs()
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
		api.POST("/sync-pairs/:id/reset", s.resetSyncPair)
		api.GET("/sync-pairs/:id/status", s.syncStatus)
		api.GET("/sync-pairs/:id/logs", s.syncLogs)

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
