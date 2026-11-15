package sync

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/esignoretti/S3sync/internal/config"
)

// smithyStatusError is the smithy shape for a bare status code with an empty
// body.
func smithyStatusError(status int) *smithyhttp.ResponseError {
	return &smithyhttp.ResponseError{Response: &smithyhttp.Response{
		Response: &http.Response{StatusCode: status},
	}}
}

// awsStatusError is what the client actually returns: the AWS middleware wraps
// every operation error in awshttp.ResponseError, whose As delegates to the
// smithy error inside.
func awsStatusError(status int) error {
	return &awshttp.ResponseError{ResponseError: smithyStatusError(status)}
}

// The real client must satisfy targetAPI with no adapter type.
var _ targetAPI = (*s3.Client)(nil)

type fakeTarget struct {
	headErr      error
	objects      int32
	lockEnabled  bool
	lockErr      error
	versioning   bool
	lifecycleErr error
	existing     []s3types.LifecycleRule
	lifecycleSet []s3types.LifecycleRule
	calls        []string

	headIn      *s3.HeadBucketInput
	createIn    *s3.CreateBucketInput
	deleteIn    *s3.DeleteBucketInput
	versionIn   *s3.PutBucketVersioningInput
	lockIn      *s3.PutObjectLockConfigurationInput
	listIn      *s3.ListObjectsV2Input
	lifecycleIn *s3.PutBucketLifecycleConfigurationInput
}

func (f *fakeTarget) record(name string) { f.calls = append(f.calls, name) }

func (f *fakeTarget) HeadBucket(_ context.Context, in *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	f.record("HeadBucket")
	f.headIn = in
	if f.headErr != nil {
		return nil, f.headErr
	}
	return &s3.HeadBucketOutput{}, nil
}

func (f *fakeTarget) CreateBucket(_ context.Context, in *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	f.record("CreateBucket")
	f.createIn = in
	if aws.ToBool(in.ObjectLockEnabledForBucket) {
		f.lockEnabled = true
	}
	return &s3.CreateBucketOutput{}, nil
}

func (f *fakeTarget) DeleteBucket(_ context.Context, in *s3.DeleteBucketInput, _ ...func(*s3.Options)) (*s3.DeleteBucketOutput, error) {
	f.record("DeleteBucket")
	f.deleteIn = in
	f.lockEnabled = false
	return &s3.DeleteBucketOutput{}, nil
}

func (f *fakeTarget) PutBucketVersioning(_ context.Context, in *s3.PutBucketVersioningInput, _ ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error) {
	f.record("PutBucketVersioning")
	f.versionIn = in
	f.versioning = true
	return &s3.PutBucketVersioningOutput{}, nil
}

func (f *fakeTarget) GetBucketVersioning(_ context.Context, _ *s3.GetBucketVersioningInput, _ ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	f.record("GetBucketVersioning")
	if !f.versioning {
		return &s3.GetBucketVersioningOutput{}, nil
	}
	return &s3.GetBucketVersioningOutput{Status: s3types.BucketVersioningStatusEnabled}, nil
}

func (f *fakeTarget) PutObjectLockConfiguration(_ context.Context, in *s3.PutObjectLockConfigurationInput, _ ...func(*s3.Options)) (*s3.PutObjectLockConfigurationOutput, error) {
	f.record("PutObjectLockConfiguration")
	f.lockIn = in
	if in.ObjectLockConfiguration != nil && in.ObjectLockConfiguration.ObjectLockEnabled == s3types.ObjectLockEnabledEnabled {
		f.lockEnabled = true
	}
	return &s3.PutObjectLockConfigurationOutput{}, nil
}

func (f *fakeTarget) GetObjectLockConfiguration(_ context.Context, _ *s3.GetObjectLockConfigurationInput, _ ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error) {
	f.record("GetObjectLockConfiguration")
	if f.lockErr != nil {
		return nil, f.lockErr
	}
	if !f.lockEnabled {
		return &s3.GetObjectLockConfigurationOutput{}, nil
	}
	return &s3.GetObjectLockConfigurationOutput{ObjectLockConfiguration: &s3types.ObjectLockConfiguration{
		ObjectLockEnabled: s3types.ObjectLockEnabledEnabled,
	}}, nil
}

func (f *fakeTarget) PutBucketLifecycleConfiguration(_ context.Context, in *s3.PutBucketLifecycleConfigurationInput, _ ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	f.record("PutBucketLifecycleConfiguration")
	f.lifecycleIn = in
	f.lifecycleSet = in.LifecycleConfiguration.Rules
	return &s3.PutBucketLifecycleConfigurationOutput{}, nil
}

func (f *fakeTarget) GetBucketLifecycleConfiguration(_ context.Context, _ *s3.GetBucketLifecycleConfigurationInput, _ ...func(*s3.Options)) (*s3.GetBucketLifecycleConfigurationOutput, error) {
	f.record("GetBucketLifecycleConfiguration")
	if f.lifecycleErr != nil {
		return nil, f.lifecycleErr
	}
	return &s3.GetBucketLifecycleConfigurationOutput{Rules: f.existing}, nil
}

func (f *fakeTarget) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.record("ListObjectsV2")
	f.listIn = in
	contents := make([]s3types.Object, f.objects)
	return &s3.ListObjectsV2Output{KeyCount: aws.Int32(f.objects), Contents: contents}, nil
}

func TestProvisionTargetDecisionTable(t *testing.T) {
	exp := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	notFound := &s3types.NotFound{}

	tests := []struct {
		name     string
		target   *fakeTarget
		pair     *config.SyncPair
		opts     ProvisionOptions
		wantErr  error
		wantCall []string
	}{
		{
			name:   "absent bucket created with lock then retention then lifecycle",
			target: &fakeTarget{headErr: notFound},
			pair:   &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail", TargetExpireAt: &exp},
			wantCall: []string{"HeadBucket", "CreateBucket", "PutBucketVersioning",
				"PutObjectLockConfiguration", "GetBucketLifecycleConfiguration",
				"PutBucketLifecycleConfiguration"},
		},
		{
			name:     "present locked bucket accepted",
			target:   &fakeTarget{lockEnabled: true},
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail"},
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration"},
		},
		{
			name:     "present unlocked bucket with fail policy errors",
			target:   &fakeTarget{},
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail"},
			wantErr:  ErrLockMissing,
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration"},
		},
		{
			name:     "recreate without force flag errors",
			target:   &fakeTarget{},
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "recreate"},
			wantErr:  ErrLockMissing,
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration"},
		},
		{
			name:     "recreate with force but non-empty errors",
			target:   &fakeTarget{objects: 1},
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "recreate"},
			opts:     ProvisionOptions{ForceRecreate: true},
			wantErr:  ErrTargetNotEmpty,
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration", "ListObjectsV2"},
		},
		{
			name:   "recreate with force and empty succeeds",
			target: &fakeTarget{},
			pair:   &config.SyncPair{RequireObjectLock: true, OnLockMissing: "recreate"},
			opts:   ProvisionOptions{ForceRecreate: true},
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration", "ListObjectsV2",
				"DeleteBucket", "CreateBucket", "PutBucketVersioning",
				"PutObjectLockConfiguration"},
		},
		{
			name:   "present locked bucket re-asserts lifecycle",
			target: &fakeTarget{lockEnabled: true},
			pair:   &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail", TargetExpireAt: &exp},
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration",
				"GetBucketLifecycleConfiguration", "PutBucketLifecycleConfiguration"},
		},
		{
			name:     "lock not required leaves an unlocked bucket alone",
			target:   &fakeTarget{},
			pair:     &config.SyncPair{},
			wantCall: []string{"HeadBucket"},
		},
		{
			name:     "modelled lock-not-found code means lock missing",
			target:   &fakeTarget{lockErr: &smithy.GenericAPIError{Code: "ObjectLockConfigurationNotFoundError"}},
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail"},
			wantErr:  ErrLockMissing,
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration"},
		},
		{
			name:     "bare 404 from lock check means lock missing",
			target:   &fakeTarget{lockErr: smithyStatusError(http.StatusNotFound)},
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail"},
			wantErr:  ErrLockMissing,
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration"},
		},
		{
			name:     "client-wrapped 404 from lock check means lock missing",
			target:   &fakeTarget{lockErr: awsStatusError(http.StatusNotFound)},
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail"},
			wantErr:  ErrLockMissing,
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration"},
		},
		{
			name:     "already-locked bucket is never deleted even when recreate is allowed",
			target:   &fakeTarget{lockEnabled: true},
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "recreate"},
			opts:     ProvisionOptions{ForceRecreate: true},
			wantCall: []string{"HeadBucket", "GetObjectLockConfiguration"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.opts
			opts.Now = now
			err := ProvisionTarget(context.Background(), tt.target, "us-east-1",
				&config.Bucket{BucketName: "b"}, tt.pair, opts)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantCall != nil && !reflect.DeepEqual(tt.target.calls, tt.wantCall) {
				t.Fatalf("calls = %v, want %v", tt.target.calls, tt.wantCall)
			}
		})
	}
}

func TestProvisionTargetRejectsInvalidExpiry(t *testing.T) {
	bad := time.Date(2027, 3, 1, 5, 0, 0, 0, time.UTC)
	fake := &fakeTarget{headErr: &s3types.NotFound{}}
	err := ProvisionTarget(context.Background(), fake,
		"us-east-1", &config.Bucket{BucketName: "b"}, &config.SyncPair{TargetExpireAt: &bad},
		ProvisionOptions{Now: time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)})
	if !errors.Is(err, ErrExpireNotMidnight) {
		t.Fatalf("err = %v", err)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("validation must happen before any S3 call, calls = %v", fake.calls)
	}
}

func TestProvisionTargetLifecycleNoPreviousConfig(t *testing.T) {
	exp := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	fake := &fakeTarget{headErr: &s3types.NotFound{}, lifecycleErr: &smithy.GenericAPIError{Code: "NoSuchLifecycleConfiguration"}}
	pair := &config.SyncPair{TargetExpireAt: &exp, NoncurrentExpireDays: 7, NewerNoncurrentVersions: 2}

	if err := ProvisionTarget(context.Background(), fake, "us-east-1",
		&config.Bucket{BucketName: "b"}, pair, ProvisionOptions{Now: now}); err != nil {
		t.Fatalf("NoSuchLifecycleConfiguration must be treated as no previous config, got %v", err)
	}

	want, err := BuildLifecycleRules(pair, now)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fake.lifecycleSet, want) {
		t.Fatalf("lifecycle rules = %+v, want %+v", fake.lifecycleSet, want)
	}
}

func TestProvisionTargetLifecycleBare404IsNoPreviousConfig(t *testing.T) {
	exp := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	fake := &fakeTarget{headErr: &s3types.NotFound{}, lifecycleErr: smithyStatusError(http.StatusNotFound)}
	pair := &config.SyncPair{TargetExpireAt: &exp}

	if err := ProvisionTarget(context.Background(), fake, "us-east-1",
		&config.Bucket{BucketName: "b"}, pair, ProvisionOptions{Now: now}); err != nil {
		t.Fatalf("a bare 404 must be treated as no previous config, got %v", err)
	}
	if !reflect.DeepEqual(fake.calls, []string{"HeadBucket", "CreateBucket",
		"GetBucketLifecycleConfiguration", "PutBucketLifecycleConfiguration"}) {
		t.Fatalf("calls = %v", fake.calls)
	}
}

func TestProvisionTargetLifecycleLogsPreviousConfig(t *testing.T) {
	exp := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	fake := &fakeTarget{
		headErr:  &s3types.NotFound{},
		existing: []s3types.LifecycleRule{{ID: aws.String("old")}},
	}
	pair := &config.SyncPair{TargetExpireAt: &exp}

	if err := ProvisionTarget(context.Background(), fake, "us-east-1",
		&config.Bucket{BucketName: "b"}, pair, ProvisionOptions{Now: now}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fake.calls, []string{"HeadBucket", "CreateBucket",
		"GetBucketLifecycleConfiguration", "PutBucketLifecycleConfiguration"}) {
		t.Fatalf("calls = %v", fake.calls)
	}
}

func TestProvisionTargetZeroNowFallsBackToWallClock(t *testing.T) {
	t.Run("future expiry succeeds", func(t *testing.T) {
		exp := time.Date(2099, 3, 1, 0, 0, 0, 0, time.UTC)
		fake := &fakeTarget{headErr: &s3types.NotFound{}}
		if err := ProvisionTarget(context.Background(), fake, "us-east-1",
			&config.Bucket{BucketName: "b"}, &config.SyncPair{TargetExpireAt: &exp}, ProvisionOptions{}); err != nil {
			t.Fatalf("zero Now must fall back to the wall clock, got %v", err)
		}
		if fake.lifecycleIn == nil {
			t.Fatal("lifecycle must be applied")
		}
	})
	t.Run("past expiry is rejected", func(t *testing.T) {
		// A zero Now must not be read as year 1: that would make every date
		// look like the future and silently skip ErrExpireInPast.
		exp := time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC)
		fake := &fakeTarget{headErr: &s3types.NotFound{}}
		err := ProvisionTarget(context.Background(), fake, "us-east-1",
			&config.Bucket{BucketName: "b"}, &config.SyncPair{TargetExpireAt: &exp}, ProvisionOptions{})
		if !errors.Is(err, ErrExpireInPast) {
			t.Fatalf("err = %v, want ErrExpireInPast", err)
		}
	})
}

func TestProvisionTargetLifecycleGetErrorIsWrapped(t *testing.T) {
	boom := errors.New("lifecycle boom")
	fake := &fakeTarget{headErr: &s3types.NotFound{}, lifecycleErr: boom}
	pair := &config.SyncPair{NoncurrentExpireDays: 7}

	err := ProvisionTarget(context.Background(), fake, "us-east-1",
		&config.Bucket{BucketName: "b"}, pair,
		ProvisionOptions{Now: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapped %v", err, boom)
	}
	for _, c := range fake.calls {
		if c == "PutBucketLifecycleConfiguration" {
			t.Fatalf("must not replace lifecycle config after a read failure, calls = %v", fake.calls)
		}
	}
}

var provisionTestNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func TestProvisionTargetCreateUSEast1(t *testing.T) {
	fake := &fakeTarget{headErr: &s3types.NotFound{}}
	tgt := &config.Bucket{BucketName: "b", ObjectLock: true, Versioning: true,
		RetentionMode: "COMPLIANCE", RetentionDays: 30}
	if err := ProvisionTarget(context.Background(), fake, "us-east-1", tgt,
		&config.SyncPair{}, ProvisionOptions{Now: provisionTestNow}); err != nil {
		t.Fatal(err)
	}
	if fake.createIn.CreateBucketConfiguration != nil {
		t.Fatalf("unexpected location constraint: %+v", fake.createIn.CreateBucketConfiguration)
	}
	if !aws.ToBool(fake.createIn.ObjectLockEnabledForBucket) {
		t.Fatal("bucket config asks for lock, CreateBucket must enable it")
	}
	if fake.versionIn == nil || fake.versionIn.VersioningConfiguration.Status != s3types.BucketVersioningStatusEnabled {
		t.Fatalf("versioning input = %+v", fake.versionIn)
	}
	rule := fake.lockIn.ObjectLockConfiguration.Rule
	if rule == nil || rule.DefaultRetention == nil ||
		rule.DefaultRetention.Mode != s3types.ObjectLockRetentionMode("COMPLIANCE") ||
		aws.ToInt32(rule.DefaultRetention.Days) != 30 {
		t.Fatalf("retention rule = %+v", rule)
	}
}

func TestProvisionTargetCreateRegionalBucket(t *testing.T) {
	fake := &fakeTarget{headErr: &s3types.NotFound{}}
	if err := ProvisionTarget(context.Background(), fake, "eu-central-1",
		&config.Bucket{BucketName: "b"}, &config.SyncPair{}, ProvisionOptions{Now: provisionTestNow}); err != nil {
		t.Fatal(err)
	}
	cfg := fake.createIn.CreateBucketConfiguration
	if cfg == nil || cfg.LocationConstraint != s3types.BucketLocationConstraint("eu-central-1") {
		t.Fatalf("location constraint = %+v", cfg)
	}
}

func TestProvisionTargetCreatePairLockWithoutRetention(t *testing.T) {
	fake := &fakeTarget{headErr: &s3types.NotFound{}}
	if err := ProvisionTarget(context.Background(), fake, "us-east-1",
		&config.Bucket{BucketName: "b"}, &config.SyncPair{RequireObjectLock: true},
		ProvisionOptions{Now: provisionTestNow}); err != nil {
		t.Fatal(err)
	}
	if fake.lockIn.ObjectLockConfiguration.Rule != nil {
		t.Fatalf("rule = %+v, want nil", fake.lockIn.ObjectLockConfiguration.Rule)
	}
	if fake.lockIn.ObjectLockConfiguration.ObjectLockEnabled != s3types.ObjectLockEnabledEnabled {
		t.Fatalf("lock enabled = %v", fake.lockIn.ObjectLockConfiguration.ObjectLockEnabled)
	}
	if fake.versionIn == nil || fake.versionIn.VersioningConfiguration.Status != s3types.BucketVersioningStatusEnabled {
		t.Fatalf("lock forces versioning, versioning input = %+v", fake.versionIn)
	}
}

func TestProvisionTargetRecreateProbesOneKey(t *testing.T) {
	fake := &fakeTarget{objects: 1}
	err := ProvisionTarget(context.Background(), fake, "us-east-1", &config.Bucket{BucketName: "b"},
		&config.SyncPair{RequireObjectLock: true, OnLockMissing: "recreate"},
		ProvisionOptions{ForceRecreate: true, Now: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)})
	if !errors.Is(err, ErrTargetNotEmpty) {
		t.Fatalf("err = %v", err)
	}
	if aws.ToInt32(fake.listIn.MaxKeys) != 1 {
		t.Fatalf("MaxKeys = %v, want 1", fake.listIn.MaxKeys)
	}
	if fake.deleteIn != nil {
		t.Fatal("must not delete a non-empty bucket")
	}
}

func TestProvisionTargetLockMissingMessages(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		pair     *config.SyncPair
		force    bool
		wantText []string
	}{
		{
			name:     "fail policy names both flags",
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "fail"},
			wantText: []string{"--on-lock-missing recreate", "--force-recreate"},
		},
		{
			name:     "recreate without force names the flag",
			pair:     &config.SyncPair{RequireObjectLock: true, OnLockMissing: "recreate"},
			wantText: []string{"--force-recreate"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ProvisionTarget(context.Background(), &fakeTarget{}, "us-east-1",
				&config.Bucket{BucketName: "b"}, tt.pair, ProvisionOptions{ForceRecreate: tt.force, Now: now})
			if !errors.Is(err, ErrLockMissing) {
				t.Fatalf("err = %v, want ErrLockMissing", err)
			}
			for _, want := range tt.wantText {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("message %q does not mention %q", err.Error(), want)
				}
			}
		})
	}
}

// HeadBucket answering a bare 404 means "no such bucket", exactly as it does for
// GetBucketLifecycleConfiguration. Provisioning runs inside setupOnce and fails
// the run, so a 404 that is not recognised here would stop a pair from ever
// creating its bucket — every cycle, not just once.
func TestProvisionTargetBare404OnHeadBucketCreatesIt(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	fake := &fakeTarget{headErr: smithyStatusError(http.StatusNotFound)}

	if err := ProvisionTarget(context.Background(), fake, "us-east-1",
		&config.Bucket{BucketName: "b"}, &config.SyncPair{}, ProvisionOptions{Now: now}); err != nil {
		t.Fatalf("a bare 404 on HeadBucket must mean the bucket is absent, got %v", err)
	}
	if !reflect.DeepEqual(fake.calls, []string{"HeadBucket", "CreateBucket"}) {
		t.Fatalf("calls = %v, want the bucket to be created", fake.calls)
	}
}
