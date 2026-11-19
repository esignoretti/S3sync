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
	return filepath.Join(home, ".s3sync")
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

var configCmd = groupCommand("config", "Manage configuration")

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

// configGetCmd prints one global setting. An unknown key is an error rather than
// a silent empty line: a typo must not look like "the value is blank".
var configGetCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Read a global setting",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, closeFn, err := openConfig()
		if err != nil {
			return err
		}
		defer closeFn()
		st, err := repo.GetSettings()
		if err != nil {
			return err
		}
		switch args[0] {
		case config.SettingRunTimeoutSeconds:
			fmt.Println(st.RunTimeoutSeconds)
		default:
			return fmt.Errorf("unknown setting %q (known: %s)", args[0], config.SettingRunTimeoutSeconds)
		}
		return nil
	},
}

// configSetCmd writes one global setting. The range check lives in
// config.SetSetting so this and the API cannot disagree about it.
var configSetCmd = &cobra.Command{
	Use:   "set <key> <value>",
	Short: "Write a global setting",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, closeFn, err := openConfig()
		if err != nil {
			return err
		}
		defer closeFn()
		if err := repo.SetSetting(args[0], args[1]); err != nil {
			return err
		}
		fmt.Printf("%s = %s\n", args[0], args[1])
		return nil
	},
}

func init() {
	configCmd.AddCommand(configInitCmd, configShowCmd, configGetCmd, configSetCmd)
	rootCmd.AddCommand(configCmd)
}
