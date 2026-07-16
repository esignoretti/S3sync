package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/esignoretti/S3sync/internal/config"
	"github.com/esignoretti/S3sync/internal/sync"
)

type apiResponse struct {
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
}

func respond(c *gin.Context, status int, data interface{}) {
	c.JSON(status, apiResponse{Data: data})
}

func respondError(c *gin.Context, status int, msg string) {
	c.JSON(status, apiResponse{Error: msg})
}

// --- Buckets ---

func (s *Server) createBucket(c *gin.Context) {
	var b config.Bucket
	if err := c.ShouldBindJSON(&b); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.repo.CreateBucket(&b); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusCreated, b)
}

func (s *Server) listBuckets(c *gin.Context) {
	buckets, err := s.repo.ListBuckets()
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	if buckets == nil {
		buckets = []config.Bucket{}
	}
	respond(c, http.StatusOK, buckets)
}

func (s *Server) getBucket(c *gin.Context) {
	b, err := s.repo.GetBucket(c.Param("id"))
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}
	respond(c, http.StatusOK, b)
}

func (s *Server) updateBucket(c *gin.Context) {
	var b config.Bucket
	if err := c.ShouldBindJSON(&b); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	b.ID = c.Param("id")
	if err := s.repo.UpdateBucket(&b); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, b)
}

func (s *Server) deleteBucket(c *gin.Context) {
	if err := s.repo.DeleteBucket(c.Param("id")); err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}
	respond(c, http.StatusOK, gin.H{"deleted": true})
}

// --- Sync Pairs ---

func (s *Server) createSyncPair(c *gin.Context) {
	var p config.SyncPair
	if err := c.ShouldBindJSON(&p); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.repo.CreateSyncPair(&p); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusCreated, p)
}

type syncPairResponse struct {
	config.SyncPair
	SourceBucket string         `json:"source_bucket"`
	TargetBucket string         `json:"target_bucket"`
	SourceName   string         `json:"source_name"`
	TargetName   string         `json:"target_name"`
	SourceURL    string         `json:"source_url"`
	TargetURL    string         `json:"target_url"`
	Progress     *sync.Progress `json:"progress"`
	Running      bool           `json:"running"`
	LastError    string         `json:"last_error"`
}

func enrichPair(repo *config.Repository, p config.SyncPair) syncPairResponse {
	r := syncPairResponse{SyncPair: p}
	if src, err := repo.GetBucket(p.SourceBucketID); err == nil {
		r.SourceBucket = src.BucketName
		r.SourceName = src.Name
		r.SourceURL = src.Endpoint + "/" + src.BucketName
	}
	if tgt, err := repo.GetBucket(p.TargetBucketID); err == nil {
		r.TargetBucket = tgt.BucketName
		r.TargetName = tgt.Name
		r.TargetURL = tgt.Endpoint + "/" + tgt.BucketName
	}
	return r
}

func (s *Server) listSyncPairs(c *gin.Context) {
	pairs, err := s.repo.ListSyncPairs()
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	if pairs == nil {
		pairs = []config.SyncPair{}
	}
	resp := make([]syncPairResponse, len(pairs))
	for i, p := range pairs {
		r := enrichPair(s.repo, p)
		if eng, ok := s.engines[p.ID]; ok {
			running, lastRun, status, lastError, prog := eng.Status()
			r.Running = running
			r.LastError = lastError
			r.Progress = &prog
			// Override stale DB status with engine live status
			r.LastSyncStatus = status
			if !lastRun.IsZero() {
				r.LastSyncAt = &lastRun
			}
		}
		resp[i] = r
	}
	respond(c, http.StatusOK, resp)
}

func (s *Server) getSyncPair(c *gin.Context) {
	p, err := s.repo.GetSyncPair(c.Param("id"))
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}
	respond(c, http.StatusOK, p)
}

func (s *Server) updateSyncPair(c *gin.Context) {
	id := c.Param("id")
	var in struct {
		SyncInterval *int `json:"sync_interval"`
		WorkerCount  *int `json:"worker_count"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	p, err := s.repo.GetSyncPair(id)
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}
	if in.SyncInterval != nil {
		p.SyncInterval = *in.SyncInterval
	}
	if in.WorkerCount != nil {
		p.WorkerCount = *in.WorkerCount
	}
	if err := s.repo.UpdateSyncPair(p); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, p)
}

func (s *Server) deleteSyncPair(c *gin.Context) {
	id := c.Param("id")
	if eng, ok := s.engines[id]; ok {
		eng.Stop()
		delete(s.engines, id)
	}
	if err := s.repo.DeleteSyncPair(id); err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}
	respond(c, http.StatusOK, gin.H{"deleted": true})
}

func (s *Server) disableSyncPair(c *gin.Context) {
	p, err := s.repo.GetSyncPair(c.Param("id"))
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}
	p.Enabled = !p.Enabled

	if p.Enabled {
		// Start the periodic engine loop if not already running
		if _, ok := s.engines[p.ID]; !ok {
			if err := s.StartEngineLoop(s.rootCtx, *p); err != nil {
				slog.Error("disableSyncPair: start engine loop", "pair", p.ID, "error", err)
			}
		}
	} else {
		// Stop the running engine
		if eng, ok := s.engines[p.ID]; ok {
			eng.Stop()
			delete(s.engines, p.ID)
		}
	}

	if err := s.repo.UpdateSyncPair(p); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, gin.H{"enabled": p.Enabled})
}

func (s *Server) triggerSync(c *gin.Context) {
	pairID := c.Param("id")

	go func() {
		if err := s.runPairSync(context.Background(), pairID); err != nil {
			slog.Warn("trigger sync", "pair", pairID, "error", err)
		}
		if eng, ok := s.engines[pairID]; ok {
			_, _, status, _, _ := eng.Status()
			if pair, err := s.repo.GetSyncPair(pairID); err == nil {
				now := time.Now().UTC()
				pair.LastSyncAt = &now
				pair.LastSyncStatus = status
				if status == "error" {
					pair.ConsecutiveErrors++
				} else {
					pair.ConsecutiveErrors = 0
				}
				s.repo.UpdateSyncPair(pair)
			}
		}
	}()
	respond(c, http.StatusAccepted, gin.H{"message": "sync triggered"})
}

func (s *Server) syncStatus(c *gin.Context) {
	p, err := s.repo.GetSyncPair(c.Param("id"))
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}
	resp := gin.H{
		"status":             p.LastSyncStatus,
		"last_sync_at":       p.LastSyncAt,
		"consecutive_errors": p.ConsecutiveErrors,
	}
	if eng, ok := s.engines[c.Param("id")]; ok {
		_, lastRun, status, lastError, prog := eng.Status()
		if !lastRun.IsZero() {
			resp["last_sync_at"] = lastRun
		}
		resp["status"] = status
		resp["last_error"] = lastError
		resp["progress"] = prog
	}
	respond(c, http.StatusOK, resp)
}

func (s *Server) setup(c *gin.Context) {
	var in config.SetupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}

	sessionID := c.GetHeader("X-Setup-Session")
	if sessionID == "" {
		sessionID = uuid.New().String()
	}

	state, exists := s.setupStates[sessionID]
	if !exists {
		state = config.NewSetupState()
		s.setupStates[sessionID] = state
	}

	if err := state.Apply(s.repo, &in); err != nil {
		state.Error = err.Error()
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}

	state.Error = ""
	respond(c, http.StatusOK, gin.H{
		"session":  sessionID,
		"step":     state.Step.String(),
		"step_num": int(state.Step),
		"done":     state.Step >= config.StepDone,
	})
}

func (s *Server) setupState(c *gin.Context) {
	sessionID := c.Query("session")
	if sessionID == "" {
		respondError(c, http.StatusBadRequest, "X-Setup-Session header or ?session= query required")
		return
	}
	state, exists := s.setupStates[sessionID]
	if !exists {
		respondError(c, http.StatusNotFound, "setup session not found")
		return
	}
	respond(c, http.StatusOK, gin.H{
		"session":  sessionID,
		"step":     state.Step.String(),
		"step_num": int(state.Step),
		"done":     state.Step >= config.StepDone,
	})
}

func (s *Server) health(c *gin.Context) {
	respond(c, http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) version(c *gin.Context) {
	respond(c, http.StatusOK, gin.H{"version": "0.1.0"})
}
