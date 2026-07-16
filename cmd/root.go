package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "bucketsync",
	Short: "Keep S3 buckets in sync, one-way",
	Long:  `BucketSync — one-way S3 bucket sync with CLI, API, and web UI.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("config-dir", "", "config directory (default: ~/.bucketsync/)")
	rootCmd.PersistentFlags().String("log-level", "info", "log level: debug|info|warn|error")
	rootCmd.PersistentFlags().String("log-format", "text", "log format: text|json")
	rootCmd.PersistentFlags().String("log-file", "", "log file path (optional)")
}
