package cache

import "time"

type CachedObject struct {
	PairID       string    `json:"pair_id"`
	Key          string    `json:"key"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
	SyncedAt     time.Time `json:"synced_at"`
	AttrsHash    string    `json:"attrs_hash,omitempty"`
	PendingAttrs bool      `json:"pending_attrs,omitempty"`
}
