package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/esignoretti/bucketsync/internal/cache"
	"github.com/esignoretti/bucketsync/internal/config"
	"github.com/esignoretti/bucketsync/internal/s3client"
	"github.com/esignoretti/bucketsync/internal/sync"
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

		if v, _ := cmd.Flags().GetString("name"); v != "" {
			p.Name = v
		}
		if v, _ := cmd.Flags().GetInt("interval"); v != 0 {
			p.SyncInterval = v
		}
		if v, _ := cmd.Flags().GetInt("workers"); v != 0 {
			p.WorkerCount = v
		}

		return repo.UpdateSyncPair(p)
	},
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

		pair, err := repo.GetSyncPair(args[0])
		if err != nil {
			return err
		}
		src, err := repo.GetBucket(pair.SourceBucketID)
		if err != nil {
			return err
		}
		tgt, err := repo.GetBucket(pair.TargetBucketID)
		if err != nil {
			return err
		}

		srcS3, err := s3client.NewClient(src)
		if err != nil {
			return err
		}
		tgtS3, err := s3client.NewClient(tgt)
		if err != nil {
			return err
		}

		cacheDir := filepath.Join(defaultConfigDir(), "cache.db")
		cacheStore, err := cache.Open(cacheDir)
		if err != nil {
			return err
		}
		defer cacheStore.Close()

		engine := sync.NewEngine(pair, src, tgt, srcS3, tgtS3, cacheStore)
		ctx := context.Background()

		start := time.Now()
		if err := engine.RunOnce(ctx); err != nil {
			return fmt.Errorf("sync failed: %w", err)
		}

		_, _, status := engine.Status()
		now := time.Now().UTC()
		pair.LastSyncAt = &now
		pair.LastSyncStatus = status
		if status == "error" {
			pair.ConsecutiveErrors++
		} else {
			pair.ConsecutiveErrors = 0
		}
		if err := repo.UpdateSyncPair(pair); err != nil {
			return err
		}

		fmt.Printf("Sync complete for %q. Status: %s (duration: %s)\n", pair.Name, status, time.Since(start).Round(time.Second))
		return nil
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

	pairUpdateCmd.Flags().String("name", "", "New name")
	pairUpdateCmd.Flags().Int("interval", 0, "New sync interval")
	pairUpdateCmd.Flags().Int("workers", 0, "New worker count")

	pairCmd.AddCommand(pairAddCmd, pairListCmd, pairGetCmd, pairUpdateCmd, pairDeleteCmd, pairSyncCmd)
	rootCmd.AddCommand(pairCmd)
}
