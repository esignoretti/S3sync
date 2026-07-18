package api

import (
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

type bucketInput struct {
	Name          string `json:"name"`
	Endpoint      string `json:"endpoint"`
	Region        string `json:"region"`
	AccessKey     string `json:"access_key"`
	SecretKey     string `json:"secret_key"`
	BucketName    string `json:"bucket_name"`
	ObjectLock    bool   `json:"object_lock"`
	Versioning    bool   `json:"versioning"`
	RetentionMode string `json:"retention_mode,omitempty"`
	RetentionDays int    `json:"retention_days,omitempty"`
}

func (s *Server) createBucket(c *gin.Context) {
	var in bucketInput
	if err := c.ShouldBindJSON(&in); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	b := &config.Bucket{
		Name: in.Name, Endpoint: in.Endpoint, Region: in.Region,
		AccessKey: in.AccessKey, SecretKey: in.SecretKey,
		BucketName: in.BucketName, ObjectLock: in.ObjectLock, Versioning: in.Versioning,
		RetentionMode: in.RetentionMode, RetentionDays: in.RetentionDays,
	}
	if err := s.repo.CreateBucket(b); err != nil {
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
	var in bucketInput
	if err := c.ShouldBindJSON(&in); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	existing, err := s.repo.GetBucket(c.Param("id"))
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}
	existing.Name = in.Name
	existing.Endpoint = in.Endpoint
	existing.Region = in.Region
	existing.BucketName = in.BucketName
	existing.ObjectLock = in.ObjectLock
	existing.Versioning = in.Versioning
	existing.RetentionMode = in.RetentionMode
	existing.RetentionDays = in.RetentionDays
	if in.AccessKey != "" {
		existing.AccessKey = in.AccessKey
	}
	if in.SecretKey != "" {
		existing.SecretKey = in.SecretKey
	}
	if err := s.repo.UpdateBucket(existing); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, existing)
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
		if eng, ok := s.GetEngine(p.ID); ok {
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
		SyncInterval       *int    `json:"sync_interval"`
		WorkerCount        *int    `json:"worker_count"`
		MaxGetOpsPerMinute *int    `json:"max_get_ops_per_minute"`
		WebhookURL         *string `json:"webhook_url"`
		WebhookEvents      *string `json:"webhook_events"`
		DryRun             *bool   `json:"dry_run"`
		CacheMaxSizeMB     *int    `json:"cache_max_size_mb"`
		CacheTTLDays       *int    `json:"cache_ttl_days"`
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
	if in.MaxGetOpsPerMinute != nil {
		p.MaxGetOpsPerMinute = *in.MaxGetOpsPerMinute
	}
	if in.WebhookURL != nil {
		p.WebhookURL = *in.WebhookURL
	}
	if in.WebhookEvents != nil {
		p.WebhookEvents = *in.WebhookEvents
	}
	if in.DryRun != nil {
		p.DryRun = *in.DryRun
	}
	if in.CacheMaxSizeMB != nil {
		p.CacheMaxSizeMB = *in.CacheMaxSizeMB
	}
	if in.CacheTTLDays != nil {
		p.CacheTTLDays = *in.CacheTTLDays
	}
	if err := s.repo.UpdateSyncPair(p); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, p)
}

func (s *Server) deleteSyncPair(c *gin.Context) {
	id := c.Param("id")
	if eng, ok := s.GetEngine(id); ok {
		eng.Stop()
		s.DeleteEngine(id)
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
		if !s.HasEngine(p.ID) {
			if err := s.StartEngineLoop(s.rootCtx, *p); err != nil {
				slog.Error("disableSyncPair: start engine loop", "pair", p.ID, "error", err)
			}
		}
	} else {
		// Stop the running engine
		if eng, ok := s.GetEngine(p.ID); ok {
			eng.Stop()
			s.DeleteEngine(p.ID)
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
		if err := s.runPairSync(s.rootCtx, pairID); err != nil {
			slog.Warn("trigger sync", "pair", pairID, "error", err)
		}
		if eng, ok := s.GetEngine(pairID); ok {
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

func (s *Server) resetSyncPair(c *gin.Context) {
	id := c.Param("id")

	// Stop engine if running
	if eng, ok := s.GetEngine(id); ok {
		eng.Stop()
		s.DeleteEngine(id)
	}

	// Clear cache for this pair
	if err := s.cache.Clear(id); err != nil {
		slog.Error("reset: clear cache", "pair", id, "error", err)
	}

	// Reset DB fields and ensure enabled
	p, err := s.repo.GetSyncPair(id)
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}
	p.LastSyncAt = nil
	p.LastSyncStatus = ""
	p.ConsecutiveErrors = 0
	p.Enabled = true
	if err := s.repo.UpdateSyncPair(p); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}

	// Start fresh engine loop
	if err := s.StartEngineLoop(s.rootCtx, *p); err != nil {
		slog.Error("reset: start engine loop", "pair", id, "error", err)
	}

	respond(c, http.StatusOK, gin.H{"message": "reset done"})
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
	if eng, ok := s.GetEngine(c.Param("id")); ok {
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

func (s *Server) syncLogs(c *gin.Context) {
	logs, err := s.repo.ListSyncLogs(c.Param("id"), 50)
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	if logs == nil {
		logs = []config.SyncLogEntry{}
	}
	respond(c, http.StatusOK, logs)
}

func (s *Server) clearSyncLogs(c *gin.Context) {
	if err := s.repo.ClearSyncLogs(c.Param("id")); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, gin.H{"cleared": true})
}

func (s *Server) getSyncPairStats(c *gin.Context) {
	stats, err := s.repo.GetPairStats(c.Param("id"))
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, stats)
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

	state := s.GetOrCreateSetupState(sessionID)

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
	state, exists := s.GetSetupState(sessionID)
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

type importBucket struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Endpoint      string `json:"endpoint"`
	Region        string `json:"region"`
	AccessKey     string `json:"access_key"`
	SecretKey     string `json:"secret_key"`
	BucketName    string `json:"bucket_name"`
	ObjectLock    bool   `json:"object_lock"`
	Versioning    bool   `json:"versioning"`
	RetentionMode string `json:"retention_mode,omitempty"`
	RetentionDays int    `json:"retention_days,omitempty"`
}

type importSyncPair struct {
	Name               string `json:"name"`
	SourceBucketID     string `json:"source_bucket_id"`
	TargetBucketID     string `json:"target_bucket_id"`
	SyncInterval       int    `json:"sync_interval"`
	WorkerCount        int    `json:"worker_count"`
	MaxGetOpsPerMinute int    `json:"max_get_ops_per_minute"`
	DeletePropagation  bool   `json:"delete_propagation"`
	TargetStorageClass string `json:"target_storage_class,omitempty"`
	Enabled            bool   `json:"enabled"`
	DryRun             bool   `json:"dry_run"`
	WebhookURL         string `json:"webhook_url,omitempty"`
	WebhookEvents      string `json:"webhook_events,omitempty"`
	CacheMaxSizeMB     int    `json:"cache_max_size_mb"`
	CacheTTLDays       int    `json:"cache_ttl_days"`
}

type importPayload struct {
	Version   int              `json:"version"`
	Buckets   []importBucket   `json:"buckets"`
	SyncPairs []importSyncPair `json:"sync_pairs"`
}

type importResult struct {
	BucketsCreated  int      `json:"buckets_created"`
	BucketsSkipped  int      `json:"buckets_skipped"`
	BucketsReplaced int      `json:"buckets_replaced"`
	PairsCreated    int      `json:"pairs_created"`
	PairsSkipped    int      `json:"pairs_skipped"`
	PairsReplaced   int      `json:"pairs_replaced"`
	Errors          []string `json:"errors"`
}

func (s *Server) importConfig(c *gin.Context) {
	conflict := c.DefaultQuery("on_conflict", "skip")

	var in importPayload
	if err := c.ShouldBindJSON(&in); err != nil {
		respondError(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if len(in.Buckets) == 0 && len(in.SyncPairs) == 0 {
		respondError(c, http.StatusBadRequest, "no buckets or sync pairs to import")
		return
	}
	for _, b := range in.Buckets {
		if b.Name == "" || b.Endpoint == "" || b.Region == "" || b.AccessKey == "" || b.SecretKey == "" {
			respondError(c, http.StatusBadRequest, "bucket "+b.Name+" has missing required fields")
			return
		}
	}
	for _, p := range in.SyncPairs {
		if p.Name == "" {
			respondError(c, http.StatusBadRequest, "sync pair has empty name")
			return
		}
	}

	result := &importResult{}
	bucketIDMap := make(map[string]string)

	for _, b := range in.Buckets {
		existing, err := s.repo.GetBucketByName(b.Name)
		if err == nil && existing != nil {
			switch conflict {
			case "skip":
				result.BucketsSkipped++
				bucketIDMap[b.ID] = existing.ID
				continue
			case "replace":
				existing.Endpoint = b.Endpoint
				existing.Region = b.Region
				existing.AccessKey = b.AccessKey
				existing.SecretKey = b.SecretKey
				existing.BucketName = b.BucketName
				existing.ObjectLock = b.ObjectLock
				existing.Versioning = b.Versioning
				existing.RetentionMode = b.RetentionMode
				existing.RetentionDays = b.RetentionDays
				if err := s.repo.UpdateBucket(existing); err != nil {
					result.Errors = append(result.Errors, "update bucket "+b.Name+": "+err.Error())
					continue
				}
				result.BucketsReplaced++
				bucketIDMap[b.ID] = existing.ID
				continue
			case "new":
			}
		}
		nb := &config.Bucket{
			Name:          b.Name,
			Endpoint:      b.Endpoint,
			Region:        b.Region,
			AccessKey:     b.AccessKey,
			SecretKey:     b.SecretKey,
			BucketName:    b.BucketName,
			ObjectLock:    b.ObjectLock,
			Versioning:    b.Versioning,
			RetentionMode: b.RetentionMode,
			RetentionDays: b.RetentionDays,
		}
		if err := s.repo.CreateBucket(nb); err != nil {
			result.Errors = append(result.Errors, "create bucket "+b.Name+": "+err.Error())
			continue
		}
		result.BucketsCreated++
		bucketIDMap[b.ID] = nb.ID
	}

	for _, p := range in.SyncPairs {
		srcID, ok := bucketIDMap[p.SourceBucketID]
		if !ok {
			result.Errors = append(result.Errors, "sync pair "+p.Name+": source bucket ID "+p.SourceBucketID+" not found")
			continue
		}
		tgtID, ok := bucketIDMap[p.TargetBucketID]
		if !ok {
			result.Errors = append(result.Errors, "sync pair "+p.Name+": target bucket ID "+p.TargetBucketID+" not found")
			continue
		}

		existingPairs, _ := s.repo.ListSyncPairs()
		var existing *config.SyncPair
		for i := range existingPairs {
			if existingPairs[i].Name == p.Name {
				existing = &existingPairs[i]
				break
			}
		}

		if existing != nil {
			switch conflict {
			case "skip":
				result.PairsSkipped++
				continue
			case "replace":
				existing.SourceBucketID = srcID
				existing.TargetBucketID = tgtID
				existing.SyncInterval = p.SyncInterval
				existing.WorkerCount = p.WorkerCount
				existing.MaxGetOpsPerMinute = p.MaxGetOpsPerMinute
				existing.DeletePropagation = p.DeletePropagation
				existing.TargetStorageClass = p.TargetStorageClass
				existing.Enabled = p.Enabled
				existing.DryRun = p.DryRun
				existing.WebhookURL = p.WebhookURL
				existing.WebhookEvents = p.WebhookEvents
				existing.CacheMaxSizeMB = p.CacheMaxSizeMB
				existing.CacheTTLDays = p.CacheTTLDays
				if err := s.repo.UpdateSyncPair(existing); err != nil {
					result.Errors = append(result.Errors, "update sync pair "+p.Name+": "+err.Error())
					continue
				}
				result.PairsReplaced++
				continue
			case "new":
			}
		}

		np := &config.SyncPair{
			Name:               p.Name,
			SourceBucketID:     srcID,
			TargetBucketID:     tgtID,
			SyncInterval:       p.SyncInterval,
			WorkerCount:        p.WorkerCount,
			MaxGetOpsPerMinute: p.MaxGetOpsPerMinute,
			DeletePropagation:  p.DeletePropagation,
			TargetStorageClass: p.TargetStorageClass,
			Enabled:            p.Enabled,
			DryRun:             p.DryRun,
			WebhookURL:         p.WebhookURL,
			WebhookEvents:      p.WebhookEvents,
			CacheMaxSizeMB:     p.CacheMaxSizeMB,
			CacheTTLDays:       p.CacheTTLDays,
		}
		if err := s.repo.CreateSyncPair(np); err != nil {
			result.Errors = append(result.Errors, "create sync pair "+p.Name+": "+err.Error())
			continue
		}
		result.PairsCreated++
	}

	if len(result.Errors) > 0 {
		c.JSON(http.StatusOK, result)
		return
	}
	respond(c, http.StatusOK, result)
}

func (s *Server) exportConfig(c *gin.Context) {
	buckets, err := s.repo.ListBuckets()
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	pairs, err := s.repo.ListSyncPairs()
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}

	type bucketJSON struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		Endpoint      string `json:"endpoint"`
		Region        string `json:"region"`
		BucketName    string `json:"bucket_name"`
		ObjectLock    bool   `json:"object_lock"`
		Versioning    bool   `json:"versioning"`
		RetentionMode string `json:"retention_mode,omitempty"`
		RetentionDays int    `json:"retention_days,omitempty"`
		CreatedAt     string `json:"created_at"`
		UpdatedAt     string `json:"updated_at"`
	}
	bjs := make([]bucketJSON, len(buckets))
	for i, b := range buckets {
		bjs[i] = bucketJSON{
			ID: b.ID, Name: b.Name, Endpoint: b.Endpoint, Region: b.Region,
			BucketName: b.BucketName, ObjectLock: b.ObjectLock, Versioning: b.Versioning,
			RetentionMode: b.RetentionMode, RetentionDays: b.RetentionDays,
			CreatedAt: b.CreatedAt.Format(time.RFC3339), UpdatedAt: b.UpdatedAt.Format(time.RFC3339),
		}
	}

	if len(pairs) == 0 {
		pairs = []config.SyncPair{}
	}

	c.JSON(http.StatusOK, gin.H{
		"version":     1,
		"exported_at": time.Now().UTC().Format(time.RFC3339),
		"buckets":     bjs,
		"sync_pairs":  pairs,
	})
}

func (s *Server) health(c *gin.Context) {
	respond(c, http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) version(c *gin.Context) {
	respond(c, http.StatusOK, gin.H{"version": "0.2.0"})
}
