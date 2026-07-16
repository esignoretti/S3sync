package api

import (
	"log/slog"
	"net/http"

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

func (s *Server) listSyncPairs(c *gin.Context) {
	pairs, err := s.repo.ListSyncPairs()
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	if pairs == nil {
		pairs = []config.SyncPair{}
	}
	respond(c, http.StatusOK, pairs)
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
	var p config.SyncPair
	if err := c.ShouldBindJSON(&p); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	p.ID = c.Param("id")
	if err := s.repo.UpdateSyncPair(&p); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, p)
}

func (s *Server) deleteSyncPair(c *gin.Context) {
	if err := s.repo.DeleteSyncPair(c.Param("id")); err != nil {
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
	if err := s.repo.UpdateSyncPair(p); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, gin.H{"enabled": p.Enabled})
}

func (s *Server) triggerSync(c *gin.Context) {
	go func() {
		if err := sync.RunOneShot(c.Request.Context(), s.repo, c.Param("id"), s.cacheDir); err != nil {
			slog.Warn("trigger sync", "pair", c.Param("id"), "error", err)
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
	respond(c, http.StatusOK, gin.H{
		"status":            p.LastSyncStatus,
		"last_sync_at":      p.LastSyncAt,
		"consecutive_errors": p.ConsecutiveErrors,
	})
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
