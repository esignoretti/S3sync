package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/esignoretti/S3sync/internal/config"
	"github.com/esignoretti/S3sync/internal/sync"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type apiResponse struct {
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
	// Code is a stable machine-readable discriminator for errors a client may
	// want to branch on, e.g. "preflight_failed" for a rejected pair.
	Code     string                 `json:"code,omitempty"`
	Warnings []sync.AdvisoryWarning `json:"warnings,omitempty"`
}

func respond(c *gin.Context, status int, data interface{}) {
	c.JSON(status, apiResponse{Data: data})
}

func respondError(c *gin.Context, status int, msg string) {
	c.JSON(status, apiResponse{Error: msg})
}

// respondErrorCode reports an error with a stable code a client can branch on.
func respondErrorCode(c *gin.Context, status int, code, msg string) {
	c.JSON(status, apiResponse{Error: msg, Code: code})
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
	var in struct {
		config.SyncPair
		AcceptCompliance bool `json:"accept_compliance"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	p := in.SyncPair

	tgt, err := s.repo.GetBucket(p.TargetBucketID)
	if err != nil {
		respondErrorCode(c, http.StatusBadRequest, "preflight_failed",
			"get target bucket: "+err.Error())
		return
	}

	warns, err := sync.ValidatePairOptions(&p, tgt.RetentionMode, tgt.RetentionDays,
		in.AcceptCompliance, true, nil, false, time.Now().UTC())
	if err != nil {
		respondErrorCode(c, http.StatusUnprocessableEntity, "preflight_failed", err.Error())
		return
	}

	// Provision before persisting: a pair whose target cannot be prepared would
	// otherwise be stored and then fail every object on every cycle.
	if err := sync.ProvisionPair(c.Request.Context(), s.repo, &p,
		sync.ProvisionOptions{Now: time.Now().UTC()}); err != nil {
		respondErrorCode(c, http.StatusUnprocessableEntity, "preflight_failed", err.Error())
		return
	}

	if err := s.repo.CreateSyncPair(&p); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusCreated, apiResponse{Data: p, Warnings: warns})
}

type syncPairResponse struct {
	config.SyncPair
	// PendingAttrs is how many objects in the last run completed with their
	// characteristics only partially applied.
	PendingAttrs int `json:"attrs_pending"`
	// Expired marks a pair whose target expiry date has passed: the engine
	// refuses to run it, so the UI can say why instead of showing a failure.
	Expired      bool           `json:"expired"`
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

func (s *Server) enrichPair(p config.SyncPair) syncPairResponse {
	r := syncPairResponse{SyncPair: p}
	if p.TargetExpireAt != nil && time.Now().UTC().After(*p.TargetExpireAt) {
		r.Expired = true
	}
	if eng, ok := s.GetEngine(p.ID); ok {
		r.PendingAttrs = eng.PendingAttrs()
	}
	if src, err := s.repo.GetBucket(p.SourceBucketID); err == nil {
		r.SourceBucket = src.BucketName
		r.SourceName = src.Name
		r.SourceURL = src.Endpoint + "/" + src.BucketName
	}
	if tgt, err := s.repo.GetBucket(p.TargetBucketID); err == nil {
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
		r := s.enrichPair(p)
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
	// Enriched like the list endpoint so `expired` and `attrs_pending` are not
	// missing for clients that fetch a single pair.
	r := s.enrichPair(*p)
	if eng, ok := s.GetEngine(p.ID); ok {
		running, lastRun, status, lastError, prog := eng.Status()
		r.Running = running
		r.LastError = lastError
		r.Progress = &prog
		r.LastSyncStatus = status
		if !lastRun.IsZero() {
			r.LastSyncAt = &lastRun
		}
	}
	respond(c, http.StatusOK, r)
}

// syncPairUpdateInput is the partial-update body for PUT /sync-pairs/:id.
// Every field is a pointer so an absent key means "leave unchanged".
type syncPairUpdateInput struct {
	SyncInterval           *int    `json:"sync_interval"`
	WorkerCount            *int    `json:"worker_count"`
	MaxGetOpsPerMinute     *int    `json:"max_get_ops_per_minute"`
	WebhookURL             *string `json:"webhook_url"`
	WebhookEvents          *string `json:"webhook_events"`
	DryRun                 *bool   `json:"dry_run"`
	ReplicateAttributes    *bool   `json:"replicate_attributes"`
	AttrsReconcileInterval *int    `json:"attrs_reconcile_interval"`
	TargetExpireAt         *string `json:"target_expire_at"`
	ConfirmExpireEarlier   *bool   `json:"confirm_expire_earlier"`
	AcceptCompliance       *bool   `json:"accept_compliance"`
	// Noncurrent-version rules are mutable after creation: the lifecycle config
	// is rewritten wholesale. The CLI exposes them on `pair update`, so the API
	// must too or the two surfaces diverge on a field that also drives the
	// noncurrent-recommended advisory.
	NoncurrentExpireDays    *int `json:"noncurrent_expire_days"`
	NewerNoncurrentVersions *int `json:"newer_noncurrent_versions"`
	// Lock settings are rejected when present: they can only be chosen when the
	// bucket is created, so accepting them here would be a silent no-op.
	RequireObjectLock *bool   `json:"require_object_lock"`
	OnLockMissing     *string `json:"on_lock_missing"`
}

// apply copies the fields the caller supplied onto p. It reports whether a lock
// field was present, which the caller must reject rather than apply.
func (in *syncPairUpdateInput) apply(p *config.SyncPair) (lockFieldPresent bool, err error) {
	if in.RequireObjectLock != nil || in.OnLockMissing != nil {
		return true, nil
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
	if in.ReplicateAttributes != nil {
		p.ReplicateAttributes = *in.ReplicateAttributes
	}
	if in.AttrsReconcileInterval != nil {
		p.AttrsReconcileInterval = *in.AttrsReconcileInterval
	}
	if in.NoncurrentExpireDays != nil {
		p.NoncurrentExpireDays = *in.NoncurrentExpireDays
	}
	if in.NewerNoncurrentVersions != nil {
		p.NewerNoncurrentVersions = *in.NewerNoncurrentVersions
	}
	if in.TargetExpireAt != nil {
		t, err := parseExpireAt(*in.TargetExpireAt)
		if err != nil {
			return false, err
		}
		p.TargetExpireAt = t
	}
	return false, nil
}

func (s *Server) updateSyncPair(c *gin.Context) {
	id := c.Param("id")
	var in syncPairUpdateInput
	if err := c.ShouldBindJSON(&in); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	p, err := s.repo.GetSyncPair(id)
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}

	// Captured before apply so the earlier-date confirmation compares against the
	// stored value; apply reassigns the field rather than writing through it.
	previousExpireAt := p.TargetExpireAt
	replicationWasOff := !p.ReplicateAttributes
	lockFieldPresent, err := in.apply(p)
	if err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if lockFieldPresent {
		respondError(c, http.StatusBadRequest,
			"require_object_lock and on_lock_missing cannot be changed after pair creation: "+
				"object lock can only be enabled when a bucket is created")
		return
	}

	tgt, err := s.repo.GetBucket(p.TargetBucketID)
	if err != nil {
		respondErrorCode(c, http.StatusBadRequest, "preflight_failed",
			"get target bucket: "+err.Error())
		return
	}
	acceptCompliance := in.AcceptCompliance != nil && *in.AcceptCompliance
	confirmEarlier := in.ConfirmExpireEarlier != nil && *in.ConfirmExpireEarlier
	warns, err := sync.ValidatePairOptions(p, tgt.RetentionMode, tgt.RetentionDays,
		acceptCompliance, confirmEarlier, previousExpireAt, replicationWasOff, time.Now().UTC())
	if err != nil {
		respondErrorCode(c, http.StatusUnprocessableEntity, "preflight_failed", err.Error())
		return
	}

	// An expiry change only takes effect once the lifecycle rule is rewritten, so
	// the same preflight runs here. ForceRecreate stays false: an update has no
	// legitimate reason to destroy a target bucket, and leaving it false makes
	// ProvisionTarget report ErrLockMissing instead of deleting anything.
	if err := sync.ProvisionPair(c.Request.Context(), s.repo, p,
		sync.ProvisionOptions{ForceRecreate: false, Now: time.Now().UTC()}); err != nil {
		respondErrorCode(c, http.StatusUnprocessableEntity, "preflight_failed", err.Error())
		return
	}

	if err := s.repo.UpdateSyncPair(p); err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}

	// The engine holds a snapshot of the pair taken when it was created and reads
	// replicate_attributes, attrs_reconcile_interval and target_expire_at from it
	// at run time. Without this, every one of those edits is inert until the
	// process restarts — worst of all an expiry moved into the future, which is
	// the documented recovery from an expired pair: the update would report
	// success while the engine kept refusing every run as expired.
	s.restartEngineLoop(p)

	c.JSON(http.StatusOK, apiResponse{Data: p, Warnings: warns})
}

// restartEngineLoop replaces a running engine with one built from the current
// pair row. It is a no-op when no engine is running, so a disabled or not-yet-
// started pair is unaffected.
func (s *Server) restartEngineLoop(p *config.SyncPair) {
	if !s.HasEngine(p.ID) {
		return
	}
	if eng, ok := s.GetEngine(p.ID); ok {
		eng.Stop()
		s.DeleteEngine(p.ID)
	}
	if !p.Enabled {
		return
	}
	if err := s.StartEngineLoop(s.rootCtx, *p); err != nil {
		slog.Error("restartEngineLoop", "pair", p.ID, "error", err)
	}
}

// parseExpireAt accepts an empty string (meaning "no expiry"), an RFC3339
// timestamp, or a bare YYYY-MM-DD date. A date with no time is midnight UTC,
// which is the only form S3 accepts for a lifecycle Expiration.Date.
func parseExpireAt(raw string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return &t, nil
	}
	t, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return nil, fmt.Errorf("target_expire_at %q: want RFC3339 or YYYY-MM-DD: %w", raw, err)
	}
	return &t, nil
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
	// The wizard must run the same preflight as POST /sync-pairs: a pair whose
	// target lacks the required object lock, or whose expiry date is in the
	// past, would otherwise be persisted here and fail on every cycle.
	//
	// Rebuilt on every request rather than once per session: the closure reads
	// accept_compliance, which only arrives on the final step, so a session-scoped
	// closure would permanently capture the first request's false and make the
	// COMPLIANCE checkbox inert.
	acceptCompliance := in.AcceptCompliance
	recreateConfirm := in.RecreateConfirm
	reqCtx := c.Request.Context()
	state.Provision = func(p *config.SyncPair) error {
		tgt, err := s.repo.GetBucket(p.TargetBucketID)
		if err != nil {
			return fmt.Errorf("get target bucket: %w", err)
		}
		if _, err := sync.ValidatePairOptions(p, tgt.RetentionMode, tgt.RetentionDays,
			acceptCompliance, true, nil, false, time.Now().UTC()); err != nil {
			return err
		}
		// Recreating destroys the target bucket, so it needs the operator to
		// have typed its name. Without this the wizard's recreate option could
		// only ever fail: nothing else on the web path sets ForceRecreate.
		forceRecreate := recreateConfirm != "" && recreateConfirm == tgt.BucketName
		if recreateConfirm != "" && !forceRecreate {
			return fmt.Errorf("recreate_confirm %q does not match the target bucket name %q",
				recreateConfirm, tgt.BucketName)
		}
		return sync.ProvisionPair(reqCtx, s.repo, p,
			sync.ProvisionOptions{ForceRecreate: forceRecreate, Now: time.Now().UTC()})
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

func (s *Server) health(c *gin.Context) {
	respond(c, http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) version(c *gin.Context) {
	respond(c, http.StatusOK, gin.H{"version": "0.2.0"})
}

// getSettings reports the global settings. The run timeout is read at the start
// of every run, so a change here applies to the next run of every pair without a
// restart.
func (s *Server) getSettings(c *gin.Context) {
	st, err := s.repo.GetSettings()
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}
	respond(c, http.StatusOK, gin.H{"run_timeout_seconds": st.RunTimeoutSeconds})
}

// updateSettings writes one setting and echoes the result. The range check lives
// in config.SetSetting so the CLI and the API cannot disagree about it; an
// out-of-range value is a client error with a stable code, not a 500.
func (s *Server) updateSettings(c *gin.Context) {
	var in struct {
		RunTimeoutSeconds *int `json:"run_timeout_seconds"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		respondError(c, http.StatusBadRequest, err.Error())
		return
	}
	if in.RunTimeoutSeconds == nil {
		respondError(c, http.StatusBadRequest, "run_timeout_seconds is required")
		return
	}
	if err := s.repo.SetSetting(config.SettingRunTimeoutSeconds,
		strconv.Itoa(*in.RunTimeoutSeconds)); err != nil {
		respondErrorCode(c, http.StatusUnprocessableEntity, "invalid_setting", err.Error())
		return
	}
	s.getSettings(c)
}
