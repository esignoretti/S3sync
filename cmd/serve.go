package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/esignoretti/S3sync/internal/api"
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
		cacheDir := filepath.Join(defaultConfigDir(), "cache.db")
		srv := api.NewServer(repo, cacheDir)

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		srv.SetRootContext(ctx)

		pairs, err := repo.ListSyncPairs()
		if err != nil {
			return fmt.Errorf("list pairs: %w", err)
		}

		for i := range pairs {
			if pairs[i].LastSyncStatus == "running" {
				pairs[i].LastSyncStatus = ""
				repo.UpdateSyncPair(&pairs[i])
			}
			if !pairs[i].Enabled {
				continue
			}
			if err := srv.StartEngineLoop(ctx, pairs[i]); err != nil {
				slog.Error("serve: start engine loop", "pair", pairs[i].Name, "error", err)
			}
		}

		slog.Info("server starting", "port", port)
		return srv.Router().Run(fmt.Sprintf(":%d", port))
	},
}

func init() {
	serveCmd.Flags().Int("port", 8080, "HTTP port")
	rootCmd.AddCommand(serveCmd)
}
