// tests/scenario/scenario_cli_test.go
//go:build integration

package scenario

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/esignoretti/S3sync/internal/config"
)

// TestScenarioCLIEndToEnd is the only scenario that exercises the shipped
// binary rather than the in-process engine. It exists because a flag wired into
// the engine but never registered on the command (or registered under a
// different name) is invisible to every other scenario in this suite.
func TestScenarioCLIEndToEnd(t *testing.T) {
	bin := os.Getenv("SCENARIO_CLI_BIN")
	if bin == "" {
		t.Skip("SCENARIO_CLI_BIN not set")
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no CLI binary: %v", err)
	}

	client, _ := newClient(t)
	src := createBucket(t, client, true)
	tgt := createBucket(t, client, false)
	putBody(t, client, src, "cli/doc.txt", "body")
	putTags(t, client, src, "cli/doc.txt", map[string]string{"via": "cli"})

	// The config directory is the global --config-dir flag (cmd/root.go:34),
	// not an environment variable, and every command needs it.
	cfgDir := t.TempDir()
	run := func(args ...string) (string, error) {
		full := append([]string{"--config-dir", cfgDir}, args...)
		out, err := exec.Command(bin, full...).CombinedOutput()
		return string(out), err
	}

	// bucket add takes the pair-facing name POSITIONALLY (cmd/bucket.go:18-20).
	for _, b := range []struct {
		name   string
		bucket string
		lock   bool
	}{{"src", src, true}, {"tgt", tgt, false}} {
		args := []string{"bucket", "add", b.name,
			"--endpoint", os.Getenv("S3_TEST_ENDPOINT"),
			"--access-key", os.Getenv("S3_TEST_ACCESS_KEY"),
			"--secret-key", os.Getenv("S3_TEST_SECRET_KEY"),
			"--bucket-name", b.bucket}
		if b.lock {
			args = append(args, "--object-lock", "--versioning")
		}
		if out, err := run(args...); err != nil {
			t.Fatalf("bucket add %s: %v\n%s", b.name, err, out)
		}
	}

	// pair add needs the full bucket UUID, while `bucket list` prints a
	// TRUNCATED 8-char id (cmd/bucket.go:46-49), so read the config database
	// directly rather than parsing CLI output.
	ids := bucketIDs(t, cfgDir, "src", "tgt")
	expire := midnightUTC(60).Format("2006-01-02")
	out, err := run("pair", "add", "cli",
		"--source-bucket", ids[0], "--target-bucket", ids[1],
		"--replicate-attributes", "--target-expire-at", expire)
	if err != nil {
		t.Fatalf("pair add: %v\n%s", err, out)
	}

	// pair sync takes the pair id, not its name (cmd/pair.go:299-302).
	out, err = run("pair", "sync", pairIDByName(t, cfgDir, "cli"))
	if err != nil {
		t.Fatalf("pair sync: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Sync complete") {
		t.Fatalf("pair sync did not report completion:\n%s", out)
	}

	// Assert on the target's contents, not on log text.
	if keys := targetKeysOf(t, client, tgt); len(keys) != 1 || keys[0] != "cli/doc.txt" {
		t.Fatalf("target contents = %v, want [cli/doc.txt]", keys)
	}
	if f := fetchFacts(t, client, tgt, "cli/doc.txt"); f.Tags["via"] != "cli" {
		t.Fatalf("target tags = %v, want via=cli", f.Tags)
	}

	// The expiry flag must have reached the bucket, not just the pair row.
	if !lifecycleHasRule(t, client, tgt, "s3sync-expire") {
		t.Fatal("the pair's expiry date did not produce an s3sync-expire lifecycle rule")
	}
}

// bucketIDs opens the CLI's own config database and returns the full UUIDs of
// the named buckets, in the order the names were given.
func bucketIDs(t *testing.T, cfgDir string, names ...string) []string {
	t.Helper()
	db, err := config.Open(filepath.Join(cfgDir, "config.db"))
	if err != nil {
		t.Fatalf("open CLI config: %v", err)
	}
	defer db.Close()
	buckets, err := config.NewRepository(db).ListBuckets()
	if err != nil {
		t.Fatalf("list buckets: %v", err)
	}
	ids := make([]string, len(names))
	for i, want := range names {
		for _, b := range buckets {
			if b.Name == want {
				ids[i] = b.ID
				break
			}
		}
		if ids[i] == "" {
			t.Fatalf("no bucket named %q in the CLI config", want)
		}
	}
	return ids
}

// pairIDByName returns the id of the named pair, because the CLI takes ids.
func pairIDByName(t *testing.T, cfgDir, name string) string {
	t.Helper()
	db, err := config.Open(filepath.Join(cfgDir, "config.db"))
	if err != nil {
		t.Fatalf("open CLI config: %v", err)
	}
	defer db.Close()
	pairs, err := config.NewRepository(db).ListSyncPairs()
	if err != nil {
		t.Fatalf("list pairs: %v", err)
	}
	for _, p := range pairs {
		if p.Name == name {
			return p.ID
		}
	}
	t.Fatalf("no pair named %q in the CLI config", name)
	return ""
}
