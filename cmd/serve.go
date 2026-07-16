package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/esignoretti/bucketsync/internal/api"
	"github.com/esignoretti/bucketsync/internal/cache"
	"github.com/esignoretti/bucketsync/internal/s3client"
	"github.com/esignoretti/bucketsync/internal/sync"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start API server + sync engine + web UI",
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		port, _ := cmd.Flags().GetInt("port")
		srv := api.NewServer(repo)

		// Start background sync engines for enabled pairs
		pairs, err := repo.ListSyncPairs()
		if err != nil {
			return fmt.Errorf("list pairs: %w", err)
		}

		for _, p := range pairs {
			if !p.Enabled {
				continue
			}
			p := p
			go func() {
				src, err := repo.GetBucket(p.SourceBucketID)
				if err != nil {
					slog.Error("serve: get source bucket", "pair", p.Name, "error", err)
					return
				}
				tgt, err := repo.GetBucket(p.TargetBucketID)
				if err != nil {
					slog.Error("serve: get target bucket", "pair", p.Name, "error", err)
					return
				}
				srcS3, err := s3client.NewClient(src)
				if err != nil {
					slog.Error("serve: create s3 client", "pair", p.Name, "error", err)
					return
				}
				tgtS3, err := s3client.NewClient(tgt)
				if err != nil {
					slog.Error("serve: create s3 client", "pair", p.Name, "error", err)
					return
				}
				cacheStore, err := cache.Open(filepath.Join(defaultConfigDir(), "cache.db"))
				if err != nil {
					slog.Error("serve: open cache", "pair", p.Name, "error", err)
					return
				}
				defer cacheStore.Close()

				engine := sync.NewEngine(&p, src, tgt, srcS3, tgtS3, cacheStore)
				ticker := time.NewTicker(time.Duration(p.SyncInterval) * time.Second)
				defer ticker.Stop()

				slog.Info("serve: sync engine started", "pair", p.Name, "interval", p.SyncInterval)
				for range ticker.C {
					ctx := context.Background()
					if err := engine.RunOnce(ctx); err != nil {
						slog.Error("serve: sync cycle failed", "pair", p.Name, "error", err)
					} else {
						_, _, status := engine.Status()
						slog.Info("serve: sync cycle complete", "pair", p.Name, "status", status)
					}
				}
			}()
		}

		slog.Info("server starting", "port", port)
		return srv.Router().Run(fmt.Sprintf(":%d", port))
	},
}

func init() {
	serveCmd.Flags().Int("port", 8080, "HTTP port")
	rootCmd.AddCommand(serveCmd)
}
