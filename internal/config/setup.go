package config

import (
	"fmt"
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
}

func NewSetupState() *SetupState {
	return &SetupState{Step: StepSourceBucket}
}

type SetupInput struct {
	Step        SetupStep `json:"step"`
	Cancel      bool      `json:"cancel,omitempty"`
	Back        bool      `json:"back,omitempty"`

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

func (in *SetupInput) ToSyncPair() *SyncPair {
	p := &SyncPair{
		Name:               in.PairName,
		SourceBucketID:     in.SourceBucketID,
		TargetBucketID:     in.TargetBucketID,
		SyncInterval:       in.SyncInterval,
		WorkerCount:        in.WorkerCount,
		MaxGetOpsPerMinute: in.MaxGetOpsPerMinute,
		TargetStorageClass: in.TargetStorageClass,
		Enabled:            true,
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
		b := in.ToBucket()
		if err := repo.CreateBucket(b); err != nil {
			return fmt.Errorf("create source bucket: %w", err)
		}
		s.SourceBucket = b
		s.Step = StepTargetBucket

	case StepTargetBucket:
		if err := validateBucket(in); err != nil {
			return fmt.Errorf("target bucket: %w", err)
		}
		b := in.ToBucket()
		if err := repo.CreateBucket(b); err != nil {
			return fmt.Errorf("create target bucket: %w", err)
		}
		s.TargetBucket = b
		s.Step = StepSyncPair

	case StepSyncPair:
		if in.Back {
			s.Step = StepTargetBucket
			return nil
		}
		if err := validateSyncPair(in, s); err != nil {
			return fmt.Errorf("sync pair: %w", err)
		}
		p := in.ToSyncPair()
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
