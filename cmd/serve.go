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
		srv.SetVersion(buildVersion)

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
		httpSrv := &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: srv.Router(),
			// ReadHeaderTimeout bounds a slow-loris client that dribbles
			// headers; ReadTimeout bounds a slow request body. WriteTimeout
			// is deliberately generous because the sync and setup handlers
			// do real S3 work before responding, and a short write deadline
			// would cut off a response that is merely slow, not stuck.
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       60 * time.Second,
			WriteTimeout:      120 * time.Second,
			IdleTimeout:       60 * time.Second,
		}

		go func() {
			<-ctx.Done()
			slog.Info("shutting down")
			httpSrv.SetKeepAlivesEnabled(false)
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer shutdownCancel()
			httpSrv.Shutdown(shutdownCtx)
			httpSrv.Close()
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
