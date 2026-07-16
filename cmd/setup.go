package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/esignoretti/S3sync/internal/config"
	"github.com/spf13/cobra"
)

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Interactive configuration wizard",
	Long: `Walk through setting up source bucket, target bucket, and sync pair
with interactive prompts. Run this on a fresh install to get started.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		s := config.NewSetupState()
		scanner := bufio.NewScanner(os.Stdin)

		for s.Step < config.StepDone {
			switch s.Step {
			case config.StepSourceBucket:
				fmt.Printf("\n--- source bucket ---\n")
				b := bucketFromPrompts(scanner)
				if err := repo.CreateBucket(b); err != nil {
					return fmt.Errorf("create source bucket: %w", err)
				}
				s.SourceBucket = b
				s.Step = config.StepTargetBucket
				fmt.Println("✓ Source bucket created:", b.Name)

			case config.StepTargetBucket:
				fmt.Printf("\n--- target bucket ---\n")
				b := bucketFromPrompts(scanner)
				if err := repo.CreateBucket(b); err != nil {
					return fmt.Errorf("create target bucket: %w", err)
				}
				s.TargetBucket = b
				s.Step = config.StepSyncPair
				fmt.Println("✓ Target bucket created:", b.Name)

			case config.StepSyncPair:
				fmt.Printf("\n--- sync pair ---\n")
				p, err := syncPairFromPrompts(scanner, s)
				if err != nil {
					return err
				}
				p.SourceBucketID = s.SourceBucket.ID
				p.TargetBucketID = s.TargetBucket.ID
				if err := repo.CreateSyncPair(p); err != nil {
					return fmt.Errorf("create sync pair: %w", err)
				}
				s.SyncPair = p
				s.Step = config.StepDone
			}
		}

		fmt.Println("\n✓ Configuration complete!")
		fmt.Printf("  Source: %s → Target: %s\n", s.SourceBucket.BucketName, s.TargetBucket.BucketName)
		fmt.Printf("  Pair: %s (sync every %ds with %d workers)\n", s.SyncPair.Name, s.SyncPair.SyncInterval, s.SyncPair.WorkerCount)
		fmt.Println("\nRun 's3sync serve' to start the sync server, or 's3sync pair sync <id>' for a one-shot sync.")
		return nil
	},
}

func prompt(scanner *bufio.Scanner, label, defaultVal string) string {
	fmt.Printf("  %s", label)
	if defaultVal != "" {
		fmt.Printf(" [%s]", defaultVal)
	}
	fmt.Print(": ")
	if !scanner.Scan() {
		return ""
	}
	v := strings.TrimSpace(scanner.Text())
	if v != "" {
		return v
	}
	return defaultVal
}

func promptBool(scanner *bufio.Scanner, label string, defaultVal bool) bool {
	dv := "n"
	if defaultVal {
		dv = "y"
	}
	v := strings.ToLower(prompt(scanner, label, dv))
	return v == "y" || v == "yes"
}

func promptInt(scanner *bufio.Scanner, label string, defaultVal int) int {
	v := prompt(scanner, label, strconv.Itoa(defaultVal))
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}

func bucketFromPrompts(scanner *bufio.Scanner) *config.Bucket {
	b := &config.Bucket{}
	b.Name = prompt(scanner, "Config name", "")
	b.Endpoint = prompt(scanner, "S3 endpoint URL", "https://s3.amazonaws.com")
	b.Region = prompt(scanner, "Region", "us-east-1")
	b.BucketName = prompt(scanner, "Bucket name", "")
	b.AccessKey = prompt(scanner, "Access key", "")
	b.SecretKey = prompt(scanner, "Secret key", "")
	b.Versioning = promptBool(scanner, "Enable versioning", false)
	if b.Versioning {
		b.ObjectLock = promptBool(scanner, "Enable object lock", false)
	}
	if b.ObjectLock {
		b.RetentionMode = prompt(scanner, "Retention mode (GOVERNANCE/COMPLIANCE)", "GOVERNANCE")
		b.RetentionDays = promptInt(scanner, "Retention days", 365)
	}
	return b
}

func syncPairFromPrompts(scanner *bufio.Scanner, s *config.SetupState) (*config.SyncPair, error) {
	p := &config.SyncPair{}
	dp := true
	name := s.SourceBucket.Name + "-to-" + s.TargetBucket.Name
	p.Name = prompt(scanner, "Pair name", name)
	p.SyncInterval = promptInt(scanner, "Sync interval (seconds)", 300)
	p.WorkerCount = promptInt(scanner, "Worker count", 10)
	p.MaxGetOpsPerMinute = promptInt(scanner, "Max GET ops/min (0=unlimited)", 600)
	dpVal := promptBool(scanner, "Enable delete propagation", true)
	if !dpVal {
		dp = false
	}
	p.DeletePropagation = dp
	p.TargetStorageClass = prompt(scanner, "Target storage class (optional)", "")
	p.Enabled = true
	return p, nil
}

func init() {
	rootCmd.AddCommand(setupCmd)
}
