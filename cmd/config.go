package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/esignoretti/S3sync/internal/config"
	"github.com/spf13/cobra"
)

func defaultConfigDir() string {
	if d := rootCmd.Flag("config-dir").Value.String(); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".bucketsync")
}

func openConfig() (*config.Repository, func(), error) {
	dir := defaultConfigDir()
	dbPath := filepath.Join(dir, "config.db")
	db, err := config.Open(dbPath)
	if err != nil {
		return nil, nil, err
	}
	return config.NewRepository(db), func() { db.Close() }, nil
}

var configCmd = &cobra.Command{Use: "config", Short: "Manage configuration"}

var configInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize config database",
	RunE: func(cmd *cobra.Command, args []string) error {
		_, close, err := openConfig()
		if err != nil {
			return fmt.Errorf("init config: %w", err)
		}
		close()
		fmt.Println("Config initialized at", filepath.Join(defaultConfigDir(), "config.db"))
		return nil
	},
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show config path",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Config dir:", defaultConfigDir())
		return nil
	},
}

func init() {
	configCmd.AddCommand(configInitCmd, configShowCmd)
	rootCmd.AddCommand(configCmd)
}
