package cmd

import (
	"os"

	"github.com/esignoretti/S3sync/internal/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "s3sync",
	Short: "Keep S3 buckets in sync, one-way",
	Long:  `S3sync — one-way S3 bucket sync with CLI, API, and web UI.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		level, _ := cmd.Flags().GetString("log-level")
		format, _ := cmd.Flags().GetString("log-format")
		file, _ := cmd.Flags().GetString("log-file")
		log.Init(log.Config{
			Level:  level,
			Format: format,
			File:   file,
		})
		return nil
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("config-dir", "", "config directory (default: ~/.s3sync/)")
	rootCmd.PersistentFlags().String("log-level", "info", "log level: debug|info|warn|error")
	rootCmd.PersistentFlags().String("log-format", "text", "log format: text|json")
	rootCmd.PersistentFlags().String("log-file", "", "log file path (optional)")
}
