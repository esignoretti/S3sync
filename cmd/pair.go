package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/esignoretti/S3sync/internal/config"
	"github.com/esignoretti/S3sync/internal/sync"
	"github.com/spf13/cobra"
)

var pairCmd = &cobra.Command{Use: "pair", Short: "Manage sync pairs"}

var pairAddCmd = &cobra.Command{
	Use:   "add [name]",
	Short: "Create a sync pair",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		srcID, _ := cmd.Flags().GetString("source-bucket")
		tgtID, _ := cmd.Flags().GetString("target-bucket")
		interval, _ := cmd.Flags().GetInt("interval")
		workers, _ := cmd.Flags().GetInt("workers")
		maxOps, _ := cmd.Flags().GetInt("max-ops")
		delProp, _ := cmd.Flags().GetBool("delete-propagation")
		sc, _ := cmd.Flags().GetString("storage-class")

		p := &config.SyncPair{
			Name: args[0], SourceBucketID: srcID, TargetBucketID: tgtID,
			SyncInterval: interval, WorkerCount: workers,
			MaxGetOpsPerMinute: maxOps, DeletePropagation: delProp,
			TargetStorageClass: sc, Enabled: true,
			OnLockMissing: "fail",
		}
		if err := applyPairFlags(cmd, p, true); err != nil {
			return err
		}
		tgt, err := repo.GetBucket(tgtID)
		if err != nil {
			return fmt.Errorf("get target bucket: %w", err)
		}

		acceptCompliance, _ := cmd.Flags().GetBool("accept-compliance")
		warns, err := sync.ValidatePairOptions(p, tgt.RetentionMode, tgt.RetentionDays,
			acceptCompliance, true, nil, time.Now().UTC())
		if err != nil {
			return err
		}
		printWarnings(warns)

		forceRecreate, _ := cmd.Flags().GetBool("force-recreate")
		if err := sync.ProvisionPair(cmd.Context(), repo, p,
			sync.ProvisionOptions{ForceRecreate: forceRecreate, Now: time.Now().UTC()}); err != nil {
			return fmt.Errorf("provision target: %w", err)
		}

		if err := repo.CreateSyncPair(p); err != nil {
			return fmt.Errorf("create pair: %w", err)
		}
		fmt.Printf("Pair %q created (id: %s)\n", p.Name, p.ID)
		return nil
	},
}

var pairListCmd = &cobra.Command{
	Use:   "list",
	Short: "List sync pairs",
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		pairs, err := repo.ListSyncPairs()
		if err != nil {
			return err
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tSOURCE\tTARGET\tINTERVAL\tWORKERS\tSTATUS")
		for _, p := range pairs {
			status := p.LastSyncStatus
			if status == "" {
				status = "never"
			}
			shortID := p.ID
			if len(shortID) > 8 {
				shortID = shortID[:8]
			}
			shortSrc := p.SourceBucketID
			if len(shortSrc) > 8 {
				shortSrc = shortSrc[:8]
			}
			shortTgt := p.TargetBucketID
			if len(shortTgt) > 8 {
				shortTgt = shortTgt[:8]
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
				shortID, p.Name, shortSrc, shortTgt,
				p.SyncInterval, p.WorkerCount, status)
		}
		w.Flush()
		return nil
	},
}

var pairGetCmd = &cobra.Command{
	Use:   "get [id]",
	Short: "Get sync pair details",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()
		p, err := repo.GetSyncPair(args[0])
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(p)
	},
}

var pairUpdateCmd = &cobra.Command{
	Use:   "update [id]",
	Short: "Update a sync pair",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		p, err := repo.GetSyncPair(args[0])
		if err != nil {
			return err
		}

		if v, _ := cmd.Flags().GetString("name"); cmd.Flags().Changed("name") {
			p.Name = v
		}
		if v, _ := cmd.Flags().GetString("source-bucket"); cmd.Flags().Changed("source-bucket") {
			p.SourceBucketID = v
		}
		if v, _ := cmd.Flags().GetString("target-bucket"); cmd.Flags().Changed("target-bucket") {
			p.TargetBucketID = v
		}
		if v, _ := cmd.Flags().GetInt("interval"); cmd.Flags().Changed("interval") {
			p.SyncInterval = v
		}
		if v, _ := cmd.Flags().GetInt("workers"); cmd.Flags().Changed("workers") {
			p.WorkerCount = v
		}
		if v, _ := cmd.Flags().GetInt("max-ops"); cmd.Flags().Changed("max-ops") {
			p.MaxGetOpsPerMinute = v
		}
		if v, _ := cmd.Flags().GetBool("delete-propagation"); cmd.Flags().Changed("delete-propagation") {
			p.DeletePropagation = v
		}
		if v, _ := cmd.Flags().GetString("storage-class"); cmd.Flags().Changed("storage-class") {
			p.TargetStorageClass = v
		}
		if v, _ := cmd.Flags().GetBool("enabled"); cmd.Flags().Changed("enabled") {
			p.Enabled = v
		}

		// Lock can only be turned on at bucket creation, so these two are fixed
		// for the life of the pair: changing them here would silently do nothing.
		for _, f := range []string{"require-object-lock", "on-lock-missing"} {
			if cmd.Flags().Changed(f) {
				return fmt.Errorf("--%s cannot be changed after pair creation: "+
					"object lock can only be enabled when a bucket is created; "+
					"delete and recreate the pair against a new target bucket instead", f)
			}
		}

		previousExpireAt := p.TargetExpireAt
		if err := applyPairFlags(cmd, p, false); err != nil {
			return err
		}
		tgt, err := repo.GetBucket(p.TargetBucketID)
		if err != nil {
			return fmt.Errorf("get target bucket: %w", err)
		}

		acceptCompliance, _ := cmd.Flags().GetBool("accept-compliance")
		confirmEarlier, _ := cmd.Flags().GetBool("confirm-expire-earlier")
		warns, err := sync.ValidatePairOptions(p, tgt.RetentionMode, tgt.RetentionDays,
			acceptCompliance, confirmEarlier, previousExpireAt, time.Now().UTC())
		if err != nil {
			return err
		}
		printWarnings(warns)

		if err := sync.ProvisionPair(cmd.Context(), repo, p,
			sync.ProvisionOptions{ForceRecreate: false, Now: time.Now().UTC()}); err != nil {
			return fmt.Errorf("provision target: %w", err)
		}

		return repo.UpdateSyncPair(p)
	},
}

// applyPairFlags copies the pair options that are settable from the command line
// onto p. create selects the add-time subset; the lock options are only accepted
// on create because object lock cannot be enabled after a bucket exists.
func applyPairFlags(cmd *cobra.Command, p *config.SyncPair, create bool) error {
	if cmd.Flags().Changed("replicate-attributes") {
		v, _ := cmd.Flags().GetBool("replicate-attributes")
		p.ReplicateAttributes = v
	}
	if cmd.Flags().Changed("attrs-reconcile-interval") {
		v, _ := cmd.Flags().GetInt("attrs-reconcile-interval")
		p.AttrsReconcileInterval = v
	}
	if cmd.Flags().Changed("target-expire-at") {
		raw, _ := cmd.Flags().GetString("target-expire-at")
		if raw == "" {
			p.TargetExpireAt = nil
		} else {
			t, err := time.Parse("2006-01-02", raw)
			if err != nil {
				return fmt.Errorf("--target-expire-at %q: want YYYY-MM-DD: %w", raw, err)
			}
			// Parse yields UTC for a bare date; ValidatePairOptions does the rest.
			p.TargetExpireAt = &t
		}
	}
	// The noncurrent-version rules are regenerated wholesale by
	// BuildLifecycleRules and applyLifecycle rewrites the lifecycle config, so
	// they are safe to change after creation. They must be: the
	// noncurrent-recommended warning fires on update and advises this flag.
	if cmd.Flags().Changed("noncurrent-expire-days") {
		v, _ := cmd.Flags().GetInt("noncurrent-expire-days")
		p.NoncurrentExpireDays = v
	}
	if cmd.Flags().Changed("newer-noncurrent-versions") {
		v, _ := cmd.Flags().GetInt("newer-noncurrent-versions")
		p.NewerNoncurrentVersions = v
	}
	if create {
		if cmd.Flags().Changed("require-object-lock") {
			v, _ := cmd.Flags().GetBool("require-object-lock")
			p.RequireObjectLock = v
		}
		if cmd.Flags().Changed("on-lock-missing") {
			v, _ := cmd.Flags().GetString("on-lock-missing")
			if v != "fail" && v != "recreate" {
				return fmt.Errorf("--on-lock-missing %q: want fail or recreate", v)
			}
			p.OnLockMissing = v
		}
	}
	return nil
}

// printWarnings reports advisory warnings on stderr. They never block the
// change, so they must not look like failures.
func printWarnings(warns []sync.AdvisoryWarning) {
	for _, w := range warns {
		fmt.Fprintf(os.Stderr, "warning: %s\n", w.Message)
	}
}

var pairDeleteCmd = &cobra.Command{
	Use:   "delete [id]",
	Short: "Delete a sync pair",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()
		return repo.DeleteSyncPair(args[0])
	},
}

var pairSyncCmd = &cobra.Command{
	Use:   "sync [id]",
	Short: "Trigger one-shot sync for a pair",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		cacheDir := filepath.Join(defaultConfigDir(), "cache.db")
		return sync.RunOneShot(context.Background(), repo, args[0], cacheDir)
	},
}

func init() {
	pairAddCmd.Flags().String("source-bucket", "", "Source bucket ID")
	pairAddCmd.Flags().String("target-bucket", "", "Target bucket ID")
	pairAddCmd.Flags().Int("interval", 300, "Sync interval (seconds)")
	pairAddCmd.Flags().Int("workers", 10, "Worker count")
	pairAddCmd.Flags().Int("max-ops", 0, "Max GET ops per minute (0=unlimited)")
	pairAddCmd.Flags().Bool("delete-propagation", true, "Propagate deletes")
	pairAddCmd.Flags().String("storage-class", "", "Target storage class override")
	pairAddCmd.Flags().Bool("replicate-attributes", false, "Replicate object metadata, tags, retention and legal hold")
	pairAddCmd.Flags().String("target-expire-at", "", "Expire the target bucket's objects at this date (YYYY-MM-DD, midnight UTC)")
	pairAddCmd.Flags().Int("noncurrent-expire-days", 0, "Delete noncurrent versions after this many days")
	pairAddCmd.Flags().Int("newer-noncurrent-versions", 0, "Keep this many noncurrent versions regardless of age")
	pairAddCmd.Flags().Bool("require-object-lock", false, "Require object lock on the target bucket")
	pairAddCmd.Flags().String("on-lock-missing", "fail", "When the target bucket lacks object lock: fail or recreate")
	pairAddCmd.Flags().Bool("force-recreate", false, "Allow recreating the target bucket when --on-lock-missing recreate needs it")
	pairAddCmd.Flags().Bool("accept-compliance", false, "Accept COMPLIANCE retention, which cannot be shortened or bypassed")
	pairAddCmd.Flags().Int("attrs-reconcile-interval", 0, "Seconds between attribute drift sweeps (0=off)")

	pairUpdateCmd.Flags().String("name", "", "New name")
	pairUpdateCmd.Flags().String("source-bucket", "", "New source bucket ID")
	pairUpdateCmd.Flags().String("target-bucket", "", "New target bucket ID")
	pairUpdateCmd.Flags().Int("interval", 0, "New sync interval")
	pairUpdateCmd.Flags().Int("workers", 0, "New worker count")
	pairUpdateCmd.Flags().Int("max-ops", 0, "New max GET ops per minute")
	pairUpdateCmd.Flags().Bool("delete-propagation", true, "Enable delete propagation")
	pairUpdateCmd.Flags().String("storage-class", "", "New target storage class")
	pairUpdateCmd.Flags().Bool("enabled", true, "Enable sync pair")
	pairUpdateCmd.Flags().Bool("replicate-attributes", false, "Replicate object metadata, tags, retention and legal hold")
	pairUpdateCmd.Flags().Int("attrs-reconcile-interval", 0, "Seconds between attribute drift sweeps (0=off)")
	pairUpdateCmd.Flags().String("target-expire-at", "", "Expire the target bucket's objects at this date (YYYY-MM-DD); empty clears it")
	pairUpdateCmd.Flags().Int("noncurrent-expire-days", 0, "Delete noncurrent versions after this many days")
	pairUpdateCmd.Flags().Int("newer-noncurrent-versions", 0, "Keep this many noncurrent versions regardless of age")
	pairUpdateCmd.Flags().Bool("confirm-expire-earlier", false, "Confirm moving the target expiry date earlier")
	pairUpdateCmd.Flags().Bool("accept-compliance", false, "Accept COMPLIANCE retention, which cannot be shortened or bypassed")
	pairUpdateCmd.Flags().Bool("require-object-lock", false, "Not settable after creation (rejected)")
	pairUpdateCmd.Flags().String("on-lock-missing", "fail", "Not settable after creation (rejected)")

	pairCmd.AddCommand(pairAddCmd, pairListCmd, pairGetCmd, pairUpdateCmd, pairDeleteCmd, pairSyncCmd)
	rootCmd.AddCommand(pairCmd)
}
