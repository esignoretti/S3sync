package cmd

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check server health via HTTP",
	Run: func(cmd *cobra.Command, args []string) {
		port, _ := cmd.Flags().GetString("port")
		if port == "" {
			port = "8080"
		}
		// Bounded timeout: the default client has none, so a hung server would
		// leave the healthcheck running until Docker's own --timeout fires.
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://localhost:%s/api/health", port))
		if err != nil {
			os.Exit(1)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(healthCmd)
	healthCmd.Flags().String("port", "8080", "server port")
}
