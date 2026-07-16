package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

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

	pairUpdateCmd.Flags().String("name", "", "New name")
	pairUpdateCmd.Flags().String("source-bucket", "", "New source bucket ID")
	pairUpdateCmd.Flags().String("target-bucket", "", "New target bucket ID")
	pairUpdateCmd.Flags().Int("interval", 0, "New sync interval")
	pairUpdateCmd.Flags().Int("workers", 0, "New worker count")
	pairUpdateCmd.Flags().Int("max-ops", 0, "New max GET ops per minute")
	pairUpdateCmd.Flags().Bool("delete-propagation", true, "Enable delete propagation")
	pairUpdateCmd.Flags().String("storage-class", "", "New target storage class")
	pairUpdateCmd.Flags().Bool("enabled", true, "Enable sync pair")

	pairCmd.AddCommand(pairAddCmd, pairListCmd, pairGetCmd, pairUpdateCmd, pairDeleteCmd, pairSyncCmd)
	rootCmd.AddCommand(pairCmd)
}
