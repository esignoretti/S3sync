// tests/scenario/scenario_versioning_test.go
//go:build integration

package scenario

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestScenarioVersionedSource(t *testing.T) {
	e := newScenarioEnv(t, envOptions{Versioning: true})
	putBody(t, e.Client, e.Source, "doc.txt", "v1")
	putBody(t, e.Client, e.Source, "doc.txt", "v2")
	e.setLiteral("doc.txt", "v2")
	e.sync()
	e.assertTargetCount(1)
	e.assertBytesMatch("doc.txt") // the current version only
}

func TestScenarioVersionedTarget(t *testing.T) {
	e := newScenarioEnv(t, envOptions{Versioning: true})
	putBody(t, e.Client, e.Source, "doc.txt", "v1")
	e.sync()
	putBody(t, e.Client, e.Source, "doc.txt", "v2")
	e.setLiteral("doc.txt", "v2")
	e.sync()
	vs, err := e.Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(e.Target), Prefix: aws.String("doc.txt"),
	})
	if err != nil {
		t.Fatalf("list target versions: %v", err)
	}
	if len(vs.Versions) != 2 {
		t.Fatalf("target holds %d versions of doc.txt, want 2 (an overwrite must not replace)", len(vs.Versions))
	}
}

func TestScenarioDeleteMarkerOnSource(t *testing.T) {
	e := newScenarioEnv(t, envOptions{Versioning: true})
	e.Pair.DeletePropagation = true
	putBody(t, e.Client, e.Source, "doc.txt", "v1")
	e.sync()
	_, err := e.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(e.Source), Key: aws.String("doc.txt"),
	})
	if err != nil {
		t.Fatalf("delete marker: %v", err)
	}
	e.sync()
	e.assertTargetCount(0)
}

func TestScenarioNoncurrentLifecycle(t *testing.T) {
	e := newScenarioEnv(t, envOptions{Versioning: true})
	e.Pair.NoncurrentExpireDays = 7
	e.Pair.NewerNoncurrentVersions = 3
	e.sync()
	cfg, err := e.Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(e.Target),
	})
	if err != nil {
		t.Fatalf("get lifecycle: %v", err)
	}
	if len(cfg.Rules) != 1 || aws.ToString(cfg.Rules[0].ID) != "s3sync-noncurrent" {
		t.Fatalf("rules = %v, want exactly one s3sync-noncurrent rule", ruleIDs(cfg.Rules))
	}
	nc := cfg.Rules[0].NoncurrentVersionExpiration
	if nc == nil || aws.ToInt32(nc.NoncurrentDays) != 7 || aws.ToInt32(nc.NewerNoncurrentVersions) != 3 {
		t.Fatalf("noncurrent expiration = %+v, want NoncurrentDays=7 NewerNoncurrentVersions=3", nc)
	}
}
