package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show sync status",
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		pairs, err := repo.ListSyncPairs()
		if err != nil {
			return err
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "PAIR\tSTATUS\tLAST SYNC\tERRORS")
		for _, p := range pairs {
			lastSync := "never"
			if p.LastSyncAt != nil {
				lastSync = p.LastSyncAt.Format("2006-01-02 15:04:05")
			}
			status := p.LastSyncStatus
			if status == "" {
				status = "never"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", p.Name, status, lastSync, p.ConsecutiveErrors)
		}
		w.Flush()
		return nil
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}
