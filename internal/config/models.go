package config

import "time"

type Bucket struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Endpoint      string    `json:"endpoint"`
	Region        string    `json:"region"`
	AccessKey     string    `json:"-"`
	SecretKey     string    `json:"-"`
	BucketName    string    `json:"bucket_name"`
	ObjectLock    bool      `json:"object_lock"`
	Versioning    bool      `json:"versioning"`
	RetentionMode string    `json:"retention_mode,omitempty"`
	RetentionDays int       `json:"retention_days,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// DefaultSyncIntervalSeconds and DefaultWorkerCount are what an omitted value
// becomes. They are applied explicitly (ApplyDefaults) rather than at read time,
// so a stored pair always carries the numbers it actually runs with.
const (
	DefaultSyncIntervalSeconds = 300
	DefaultWorkerCount         = 10
)

// ApplyDefaults fills the settings whose zero value means "not specified".
//
// Only for callers that cannot tell "absent" from "zero": a JSON body that omits
// worker_count and one that sends 0 are the same thing to encoding/json. The CLI
// is different — its flags carry the default, so an explicit --workers 0 is
// visible as 0 and is rejected by ValidatePairOptions instead of being quietly
// replaced.
func (p *SyncPair) ApplyDefaults() {
	if p.SyncInterval <= 0 {
		p.SyncInterval = DefaultSyncIntervalSeconds
	}
	if p.WorkerCount <= 0 {
		p.WorkerCount = DefaultWorkerCount
	}
}

type SyncPair struct {
	ID                      string     `json:"id"`
	Name                    string     `json:"name"`
	SourceBucketID          string     `json:"source_bucket_id"`
	TargetBucketID          string     `json:"target_bucket_id"`
	SyncInterval            int        `json:"sync_interval"`
	WorkerCount             int        `json:"worker_count"`
	MaxGetOpsPerMinute      int        `json:"max_get_ops_per_minute"`
	DeletePropagation       bool       `json:"delete_propagation"`
	TargetStorageClass      string     `json:"target_storage_class,omitempty"`
	Enabled                 bool       `json:"enabled"`
	DryRun                  bool       `json:"dry_run"`
	WebhookURL              string     `json:"webhook_url,omitempty"`
	WebhookEvents           string     `json:"webhook_events,omitempty"`
	LastSyncAt              *time.Time `json:"last_sync_at,omitempty"`
	LastSyncStatus          string     `json:"last_sync_status"`
	ConsecutiveErrors       int        `json:"consecutive_errors"`
	ReplicateAttributes     bool       `json:"replicate_attributes"`
	TargetExpireAt          *time.Time `json:"target_expire_at,omitempty"`
	NoncurrentExpireDays    int        `json:"noncurrent_expire_days,omitempty"`
	NewerNoncurrentVersions int        `json:"newer_noncurrent_versions,omitempty"`
	RequireObjectLock       bool       `json:"require_object_lock"`
	OnLockMissing           string     `json:"on_lock_missing"`
	AttrsReconcileInterval  int        `json:"attrs_reconcile_interval"`
	ProvisionedAt           *time.Time `json:"provisioned_at,omitempty"`
	ProvisionError          string     `json:"provision_error,omitempty"`
	LastReconcileAt         *time.Time `json:"last_reconcile_at,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

type SyncLogEntry struct {
	ID          string    `json:"id"`
	PairID      string    `json:"pair_id"`
	Status      string    `json:"status"`
	ErrorMsg    string    `json:"error_msg,omitempty"`
	Succeeded   int       `json:"succeeded"`
	Failed      int       `json:"failed"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}
