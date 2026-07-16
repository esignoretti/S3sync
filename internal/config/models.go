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

type SyncPair struct {
	ID                 string     `json:"id"`
	Name               string     `json:"name"`
	SourceBucketID     string     `json:"source_bucket_id"`
	TargetBucketID     string     `json:"target_bucket_id"`
	SyncInterval       int        `json:"sync_interval"`
	WorkerCount        int        `json:"worker_count"`
	MaxGetOpsPerMinute int        `json:"max_get_ops_per_minute"`
	DeletePropagation  bool       `json:"delete_propagation"`
	TargetStorageClass string     `json:"target_storage_class,omitempty"`
	Enabled            bool       `json:"enabled"`
	LastSyncAt         *time.Time `json:"last_sync_at,omitempty"`
	LastSyncStatus     string     `json:"last_sync_status"`
	ConsecutiveErrors  int        `json:"consecutive_errors"`
	LastSyncedObjects  int        `json:"last_synced_objects"`
	LastTotalObjects   int        `json:"last_total_objects"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}
