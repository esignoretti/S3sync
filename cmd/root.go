package cmd

import (
	"fmt"
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

// groupCommand builds a command that exists only to hold subcommands.
//
// A bare `&cobra.Command{Use: "pair"}` with no Run is not an error to cobra: it
// treats an unrecognised word as an argument, prints the help text and returns
// flag.ErrHelp, which Execute turns into exit 0. So `s3sync pair synk <id>` — a
// typo in a cron entry — printed help and reported success forever, and nothing
// upstream could tell it had done nothing.
//
// Setting RunE makes the group runnable, so an unknown subcommand reaches
// cobra's own "unknown command" path and exits non-zero.
func groupCommand(use, short string) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			return fmt.Errorf("unknown command %q for %q", args[0], cmd.CommandPath())
		},
	}
}

func init() {
	rootCmd.PersistentFlags().String("config-dir", "", "config directory (default: ~/.s3sync/)")
	rootCmd.PersistentFlags().String("log-level", "info", "log level: debug|info|warn|error")
	rootCmd.PersistentFlags().String("log-format", "text", "log format: text|json")
	rootCmd.PersistentFlags().String("log-file", "", "log file path (optional)")
}
