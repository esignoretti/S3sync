package config

import (
	"fmt"
	"time"
)

type SetupStep int

const (
	StepSourceBucket SetupStep = iota + 1
	StepTargetBucket
	StepSyncPair
	StepDone
)

func (s SetupStep) String() string {
	switch s {
	case StepSourceBucket:
		return "source-bucket"
	case StepTargetBucket:
		return "target-bucket"
	case StepSyncPair:
		return "sync-pair"
	case StepDone:
		return "done"
	default:
		return "unknown"
	}
}

type SetupState struct {
	SourceBucket *Bucket
	TargetBucket *Bucket
	SyncPair     *SyncPair
	Step         SetupStep
	Error        string

	// Provision, when set, is called with the assembled pair after the user
	// confirms the sync-pair step and before the pair is persisted. Returning
	// an error aborts the step and nothing is written. It is a hook rather
	// than a direct call because validation and provisioning live in
	// internal/sync, which imports this package.
	Provision func(pair *SyncPair) error

	// RecreateConfirm is the text the operator typed to authorise destroying
	// an existing target bucket. It is forwarded to Provision, which compares
	// it against the bucket name; a recreate without an exact match is refused.
	RecreateConfirm string
}

func NewSetupState() *SetupState {
	return &SetupState{Step: StepSourceBucket}
}

type SetupInput struct {
	Step   SetupStep `json:"step"`
	Cancel bool      `json:"cancel,omitempty"`
	Back   bool      `json:"back,omitempty"`

	// Bucket fields
	Name          string `json:"name,omitempty"`
	Endpoint      string `json:"endpoint,omitempty"`
	Region        string `json:"region,omitempty"`
	BucketName    string `json:"bucket_name,omitempty"`
	AccessKey     string `json:"access_key,omitempty"`
	SecretKey     string `json:"secret_key,omitempty"`
	ObjectLock    bool   `json:"object_lock,omitempty"`
	Versioning    bool   `json:"versioning,omitempty"`
	RetentionMode string `json:"retention_mode,omitempty"`
	RetentionDays int    `json:"retention_days,omitempty"`

	// Sync pair fields
	SourceBucketID     string `json:"source_bucket_id,omitempty"`
	TargetBucketID     string `json:"target_bucket_id,omitempty"`
	SyncInterval       int    `json:"sync_interval,omitempty"`
	WorkerCount        int    `json:"worker_count,omitempty"`
	MaxGetOpsPerMinute int    `json:"max_get_ops_per_minute,omitempty"`
	DeletePropagation  *bool  `json:"delete_propagation,omitempty"`
	TargetStorageClass string `json:"target_storage_class,omitempty"`
	PairName           string `json:"pair_name,omitempty"`

	// Attribute replication and lock provisioning fields. These mirror the
	// pair flags so the web wizard cannot silently drop them: a field the
	// form posts but SetupInput does not declare is discarded by
	// ShouldBindJSON, and the pair is then created with the zero value.
	ReplicateAttributes     bool   `json:"replicate_attributes,omitempty"`
	TargetExpireAt          string `json:"target_expire_at,omitempty"`
	NoncurrentExpireDays    int    `json:"noncurrent_expire_days,omitempty"`
	NewerNoncurrentVersions int    `json:"newer_noncurrent_versions,omitempty"`
	RequireObjectLock       bool   `json:"require_object_lock,omitempty"`
	OnLockMissing           string `json:"on_lock_missing,omitempty"`
	AcceptCompliance        bool   `json:"accept_compliance,omitempty"`
	AttrsReconcileInterval  int    `json:"attrs_reconcile_interval,omitempty"`
	// RecreateConfirm must equal the target bucket's name before a
	// lock-missing recreate is allowed to destroy it. It is checked
	// server-side because the wizard resets its form between steps and so
	// cannot reliably compare against the name itself.
	RecreateConfirm string `json:"recreate_confirm,omitempty"`
}

func (in *SetupInput) ToBucket() *Bucket {
	return &Bucket{
		Name:          in.Name,
		Endpoint:      in.Endpoint,
		Region:        in.Region,
		BucketName:    in.BucketName,
		AccessKey:     in.AccessKey,
		SecretKey:     in.SecretKey,
		ObjectLock:    in.ObjectLock,
		Versioning:    in.Versioning,
		RetentionMode: in.RetentionMode,
		RetentionDays: in.RetentionDays,
	}
}

func (in *SetupInput) ToSyncPair(srcName, tgtName string) *SyncPair {
	if in.PairName == "" {
		in.PairName = srcName + "-to-" + tgtName
	}
	p := &SyncPair{
		Name:               in.PairName,
		SourceBucketID:     in.SourceBucketID,
		TargetBucketID:     in.TargetBucketID,
		SyncInterval:       in.SyncInterval,
		WorkerCount:        in.WorkerCount,
		MaxGetOpsPerMinute: in.MaxGetOpsPerMinute,
		TargetStorageClass: in.TargetStorageClass,
		Enabled:            true,

		ReplicateAttributes:     in.ReplicateAttributes,
		NoncurrentExpireDays:    in.NoncurrentExpireDays,
		NewerNoncurrentVersions: in.NewerNoncurrentVersions,
		RequireObjectLock:       in.RequireObjectLock,
		OnLockMissing:           in.OnLockMissing,
		AttrsReconcileInterval:  in.AttrsReconcileInterval,
	}
	if p.OnLockMissing == "" {
		p.OnLockMissing = "fail"
	}
	// A malformed date is left as nil here and rejected by the caller's
	// ValidatePairOptions pass, which quotes the offending value.
	if in.TargetExpireAt != "" {
		if t, err := time.Parse("2006-01-02", in.TargetExpireAt); err == nil {
			p.TargetExpireAt = &t
		}
	}
	if in.DeletePropagation != nil {
		p.DeletePropagation = *in.DeletePropagation
	} else {
		p.DeletePropagation = true
	}
	if p.SyncInterval <= 0 {
		p.SyncInterval = 300
	}
	if p.WorkerCount <= 0 {
		p.WorkerCount = 10
	}
	return p
}

func (s *SetupState) Apply(repo *Repository, in *SetupInput) error {
	if in.Cancel {
		return fmt.Errorf("setup cancelled")
	}

	switch s.Step {
	case StepSourceBucket:
		if err := validateBucket(in); err != nil {
			return fmt.Errorf("source bucket: %w", err)
		}
		b, err := getOrCreateBucket(repo, in)
		if err != nil {
			return fmt.Errorf("source bucket: %w", err)
		}
		s.SourceBucket = b
		s.Step = StepTargetBucket

	case StepTargetBucket:
		if err := validateBucket(in); err != nil {
			return fmt.Errorf("target bucket: %w", err)
		}
		b, err := getOrCreateBucket(repo, in)
		if err != nil {
			return fmt.Errorf("target bucket: %w", err)
		}
		s.TargetBucket = b
		s.Step = StepSyncPair

	case StepSyncPair:
		if in.Back {
			s.Step = StepTargetBucket
			return nil
		}
		// Hydrate IDs from state (web form doesn't send them)
		in.SourceBucketID = s.SourceBucket.ID
		in.TargetBucketID = s.TargetBucket.ID
		if in.PairName == "" {
			in.PairName = s.SourceBucket.Name + "-to-" + s.TargetBucket.Name
		}
		if err := validateSyncPair(in, s); err != nil {
			return fmt.Errorf("sync pair: %w", err)
		}
		p := in.ToSyncPair(s.SourceBucket.Name, s.TargetBucket.Name)
		s.RecreateConfirm = in.RecreateConfirm
		// Provision runs before the pair is persisted: a pair whose target
		// cannot be prepared would otherwise be stored and then fail every
		// object on every cycle. config cannot import internal/sync, so the
		// preflight is injected by the caller.
		if s.Provision != nil {
			if err := s.Provision(p); err != nil {
				return fmt.Errorf("sync pair: %w", err)
			}
		}
		if err := repo.CreateSyncPair(p); err != nil {
			return fmt.Errorf("create sync pair: %w", err)
		}
		s.SyncPair = p
		s.Step = StepDone

	case StepDone:
		return fmt.Errorf("setup already complete")
	}

	return nil
}

func getOrCreateBucket(repo *Repository, in *SetupInput) (*Bucket, error) {
	b, err := repo.GetBucketByName(in.Name)
	if err == nil {
		return b, nil // bucket already exists, use it
	}
	b = in.ToBucket()
	if err := repo.CreateBucket(b); err != nil {
		return nil, err
	}
	return b, nil
}

func validateBucket(in *SetupInput) error {
	if in.Name == "" {
		return fmt.Errorf("name is required")
	}
	if in.BucketName == "" {
		return fmt.Errorf("bucket name is required")
	}
	if in.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if in.Region == "" {
		return fmt.Errorf("region is required")
	}
	if in.AccessKey == "" {
		return fmt.Errorf("access key is required")
	}
	if in.SecretKey == "" {
		return fmt.Errorf("secret key is required")
	}
	if in.ObjectLock && !in.Versioning {
		return fmt.Errorf("object lock requires versioning")
	}
	return nil
}

func validateSyncPair(in *SetupInput, s *SetupState) error {
	if in.PairName == "" {
		return fmt.Errorf("pair name is required")
	}
	srcID := in.SourceBucketID
	if srcID == "" && s.SourceBucket != nil {
		srcID = s.SourceBucket.ID
	}
	tgtID := in.TargetBucketID
	if tgtID == "" && s.TargetBucket != nil {
		tgtID = s.TargetBucket.ID
	}
	if srcID == "" {
		return fmt.Errorf("source bucket ID is required")
	}
	if tgtID == "" {
		return fmt.Errorf("target bucket ID is required")
	}
	return nil
}
