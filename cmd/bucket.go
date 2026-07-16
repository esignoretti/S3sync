package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/esignoretti/S3sync/internal/config"
	"github.com/spf13/cobra"
)

var bucketCmd = &cobra.Command{Use: "bucket", Short: "Manage bucket configurations"}

var bucketAddCmd = &cobra.Command{
	Use:   "add [name]",
	Short: "Add a bucket",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		endpoint, _ := cmd.Flags().GetString("endpoint")
		region, _ := cmd.Flags().GetString("region")
		bucketName, _ := cmd.Flags().GetString("bucket-name")
		accessKey, _ := cmd.Flags().GetString("access-key")
		secretKey, _ := cmd.Flags().GetString("secret-key")
		objectLock, _ := cmd.Flags().GetBool("object-lock")
		versioning, _ := cmd.Flags().GetBool("versioning")
		retentionMode, _ := cmd.Flags().GetString("retention-mode")
		retentionDays, _ := cmd.Flags().GetInt("retention-days")

		b := &config.Bucket{
			Name: args[0], Endpoint: endpoint, Region: region,
			BucketName: bucketName, AccessKey: accessKey, SecretKey: secretKey,
			ObjectLock: objectLock, Versioning: versioning,
			RetentionMode: retentionMode, RetentionDays: retentionDays,
		}
		if err := repo.CreateBucket(b); err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}
		fmt.Printf("Bucket %q created (id: %s)\n", b.Name, b.ID)
		return nil
	},
}

var bucketListCmd = &cobra.Command{
	Use:   "list",
	Short: "List buckets",
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		buckets, err := repo.ListBuckets()
		if err != nil {
			return err
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tENDPOINT\tBUCKET")
		for _, b := range buckets {
			shortID := b.ID
			if len(shortID) > 8 {
				shortID = shortID[:8]
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", shortID, b.Name, b.Endpoint, b.BucketName)
		}
		w.Flush()
		return nil
	},
}

var bucketGetCmd = &cobra.Command{
	Use:   "get [id]",
	Short: "Get bucket details",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		b, err := repo.GetBucket(args[0])
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(b)
	},
}

var bucketUpdateCmd = &cobra.Command{
	Use:   "update [id]",
	Short: "Update a bucket",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()

		b, err := repo.GetBucket(args[0])
		if err != nil {
			return err
		}

		if v, _ := cmd.Flags().GetString("name"); cmd.Flags().Changed("name") {
			b.Name = v
		}
		if v, _ := cmd.Flags().GetString("endpoint"); cmd.Flags().Changed("endpoint") {
			b.Endpoint = v
		}
		if v, _ := cmd.Flags().GetString("region"); cmd.Flags().Changed("region") {
			b.Region = v
		}
		if v, _ := cmd.Flags().GetString("bucket-name"); cmd.Flags().Changed("bucket-name") {
			b.BucketName = v
		}
		if v, _ := cmd.Flags().GetString("access-key"); cmd.Flags().Changed("access-key") {
			b.AccessKey = v
		}
		if v, _ := cmd.Flags().GetString("secret-key"); cmd.Flags().Changed("secret-key") {
			b.SecretKey = v
		}
		if v, _ := cmd.Flags().GetBool("object-lock"); cmd.Flags().Changed("object-lock") {
			b.ObjectLock = v
		}
		if v, _ := cmd.Flags().GetBool("versioning"); cmd.Flags().Changed("versioning") {
			b.Versioning = v
		}
		if v, _ := cmd.Flags().GetString("retention-mode"); cmd.Flags().Changed("retention-mode") {
			b.RetentionMode = v
		}
		if v, _ := cmd.Flags().GetInt("retention-days"); cmd.Flags().Changed("retention-days") {
			b.RetentionDays = v
		}

		return repo.UpdateBucket(b)
	},
}

var bucketDeleteCmd = &cobra.Command{
	Use:   "delete [id]",
	Short: "Delete a bucket",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		repo, close, err := openConfig()
		if err != nil {
			return err
		}
		defer close()
		return repo.DeleteBucket(args[0])
	},
}

func init() {
	bucketAddCmd.Flags().String("endpoint", "", "S3 endpoint URL")
	bucketAddCmd.Flags().String("region", "us-east-1", "AWS region")
	bucketAddCmd.Flags().String("bucket-name", "", "Bucket name on S3")
	bucketAddCmd.Flags().String("access-key", "", "Access key")
	bucketAddCmd.Flags().String("secret-key", "", "Secret key")
	bucketAddCmd.Flags().Bool("object-lock", false, "Enable object lock")
	bucketAddCmd.Flags().Bool("versioning", false, "Enable versioning")
	bucketAddCmd.Flags().String("retention-mode", "", "GOVERNANCE or COMPLIANCE")
	bucketAddCmd.Flags().Int("retention-days", 0, "Retention period in days")

	bucketUpdateCmd.Flags().String("name", "", "New name")
	bucketUpdateCmd.Flags().String("endpoint", "", "New endpoint")
	bucketUpdateCmd.Flags().String("region", "", "New region")
	bucketUpdateCmd.Flags().String("bucket-name", "", "New bucket name on S3")
	bucketUpdateCmd.Flags().String("access-key", "", "New access key")
	bucketUpdateCmd.Flags().String("secret-key", "", "New secret key")
	bucketUpdateCmd.Flags().Bool("object-lock", false, "Enable object lock")
	bucketUpdateCmd.Flags().Bool("versioning", false, "Enable versioning")
	bucketUpdateCmd.Flags().String("retention-mode", "", "GOVERNANCE or COMPLIANCE")
	bucketUpdateCmd.Flags().Int("retention-days", 0, "Retention period in days")

	bucketCmd.AddCommand(bucketAddCmd, bucketListCmd, bucketGetCmd, bucketUpdateCmd, bucketDeleteCmd)
	rootCmd.AddCommand(bucketCmd)
}
