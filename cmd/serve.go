package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

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
		srv, err := api.NewServer(repo, cacheDir)
		if err != nil {
			return fmt.Errorf("create server: %w", err)
		}
		defer srv.Close()

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
		httpSrv := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: srv.Router()}

		go func() {
			<-ctx.Done()
			slog.Info("shutting down")
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			httpSrv.Shutdown(shutdownCtx)
		}()

		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	},
}

func init() {
	serveCmd.Flags().Int("port", 8080, "HTTP port")
	rootCmd.AddCommand(serveCmd)
}
