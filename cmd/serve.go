package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/esignoretti/S3sync/internal/api"
	"github.com/esignoretti/S3sync/internal/cache"
	"github.com/esignoretti/S3sync/internal/s3client"
	"github.com/esignoretti/S3sync/internal/sync"
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
		srv := api.NewServer(repo, filepath.Join(defaultConfigDir(), "cache.db"))

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

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
				for {
					select {
					case <-ticker.C:
						if err := engine.RunOnce(ctx); err != nil {
							slog.Error("serve: sync cycle failed", "pair", p.Name, "error", err)
						} else {
							_, _, status := engine.Status()
							slog.Info("serve: sync cycle complete", "pair", p.Name, "status", status)
						}
					case <-ctx.Done():
						slog.Info("serve: stopping sync engine", "pair", p.Name)
						return
					}
				}
			}()
		}

		slog.Info("server starting", "port", port)
		router := srv.Router()

		go func() {
			<-ctx.Done()
			slog.Info("serve: shutting down")
			// Give in-flight syncs time to finish
			time.Sleep(2 * time.Second)
		}()

		return router.Run(fmt.Sprintf(":%d", port))
	},
}

func init() {
	serveCmd.Flags().Int("port", 8080, "HTTP port")
	rootCmd.AddCommand(serveCmd)
}
