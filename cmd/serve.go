package cmd

import (
	"fmt"

	"github.com/esignoretti/bucketsync/internal/api"
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
		router := srv.Router()

		fmt.Printf("BucketSync server starting on :%d\n", port)
		return router.Run(fmt.Sprintf(":%d", port))
	},
}

func init() {
	serveCmd.Flags().Int("port", 8080, "HTTP port")
	rootCmd.AddCommand(serveCmd)
}
