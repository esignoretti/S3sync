package config

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
)

type Repository struct {
	db  *sql.DB
	key []byte // AES-256 key for credential encryption (nil = no encryption)
}

func NewRepository(db *sql.DB) *Repository {
	r := &Repository{db: db}
	if mk := os.Getenv("BUCKETSYNC_MASTER_KEY"); mk != "" {
		r.key = DeriveKey([]byte(mk))
	}
	return r
}

// --- Buckets ---

func (r *Repository) CreateBucket(b *Bucket) error {
	b.ID = uuid.New().String()
	now := time.Now().UTC()
	b.CreatedAt = now
	b.UpdatedAt = now

	ak, sk := b.AccessKey, b.SecretKey
	if r.key != nil {
		encAK, err := Encrypt([]byte(b.AccessKey), r.key)
		if err != nil {
			return fmt.Errorf("encrypt access key: %w", err)
		}
		encSK, err := Encrypt([]byte(b.SecretKey), r.key)
		if err != nil {
			return fmt.Errorf("encrypt secret key: %w", err)
		}
		ak, sk = encAK, encSK
	}

	_, err := r.db.Exec(
		`INSERT INTO buckets (id,name,endpoint,region,access_key,secret_key,bucket_name,
		 object_lock,versioning,retention_mode,retention_days,created_at,updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		b.ID, b.Name, b.Endpoint, b.Region, ak, sk, b.BucketName,
		boolInt(b.ObjectLock), boolInt(b.Versioning),
		nullStr(b.RetentionMode), nullInt(b.RetentionDays),
		rfc(b.CreatedAt), rfc(b.UpdatedAt),
	)
	return err
}

func (r *Repository) ListBuckets() ([]Bucket, error) {
	rows, err := r.db.Query(`SELECT id,name,endpoint,region,access_key,secret_key,bucket_name,object_lock,versioning,retention_mode,retention_days,created_at,updated_at FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out, err := scanBuckets(rows)
	if err != nil {
		return nil, err
	}
	for i := range out {
		decryptCreds(r.key, &out[i])
	}
	return out, nil
}

func (r *Repository) GetBucket(id string) (*Bucket, error) {
	row := r.db.QueryRow(`SELECT id,name,endpoint,region,access_key,secret_key,bucket_name,object_lock,versioning,retention_mode,retention_days,created_at,updated_at FROM buckets WHERE id = ?`, id)
	b, err := scanBucket(row)
	if err != nil {
		return nil, err
	}
	decryptCreds(r.key, b)
	return b, nil
}

func (r *Repository) GetBucketByName(name string) (*Bucket, error) {
	row := r.db.QueryRow(`SELECT id,name,endpoint,region,access_key,secret_key,bucket_name,object_lock,versioning,retention_mode,retention_days,created_at,updated_at FROM buckets WHERE name = ?`, name)
	b, err := scanBucket(row)
	if err != nil {
		return nil, err
	}
	decryptCreds(r.key, b)
	return b, nil
}

func (r *Repository) UpdateBucket(b *Bucket) error {
	b.UpdatedAt = time.Now().UTC()

	ak, sk := b.AccessKey, b.SecretKey
	if r.key != nil {
		encAK, err := Encrypt([]byte(b.AccessKey), r.key)
		if err != nil {
			return fmt.Errorf("encrypt access key: %w", err)
		}
		encSK, err := Encrypt([]byte(b.SecretKey), r.key)
		if err != nil {
			return fmt.Errorf("encrypt secret key: %w", err)
		}
		ak, sk = encAK, encSK
	}

	res, err := r.db.Exec(
		`UPDATE buckets SET name=?,endpoint=?,region=?,access_key=?,secret_key=?,
		 bucket_name=?,object_lock=?,versioning=?,retention_mode=?,retention_days=?,
		 updated_at=? WHERE id=?`,
		b.Name, b.Endpoint, b.Region, ak, sk, b.BucketName,
		boolInt(b.ObjectLock), boolInt(b.Versioning),
		nullStr(b.RetentionMode), nullInt(b.RetentionDays),
		rfc(b.UpdatedAt), b.ID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("bucket %q not found", b.ID)
	}
	return nil
}

func (r *Repository) DeleteBucket(id string) error {
	res, err := r.db.Exec(`DELETE FROM buckets WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("bucket %q not found", id)
	}
	return nil
}

// --- Sync Pairs ---

func (r *Repository) CreateSyncPair(p *SyncPair) error {
	p.ID = uuid.New().String()
	now := time.Now().UTC()
	p.CreatedAt = now
	p.UpdatedAt = now
	_, err := r.db.Exec(
		`INSERT INTO sync_pairs (id,name,source_bucket_id,target_bucket_id,
		 sync_interval,worker_count,max_get_ops_per_minute,delete_propagation,
		 target_storage_class,enabled,dry_run,webhook_url,webhook_events,
		 last_sync_at,last_sync_status,consecutive_errors,created_at,updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		p.ID, p.Name, p.SourceBucketID, p.TargetBucketID,
		p.SyncInterval, p.WorkerCount, p.MaxGetOpsPerMinute, boolInt(p.DeletePropagation),
		nullStr(p.TargetStorageClass), boolInt(p.Enabled), boolInt(p.DryRun),
		p.WebhookURL, p.WebhookEvents,
		nullTimeRFC(p.LastSyncAt), p.LastSyncStatus,
		p.ConsecutiveErrors, rfc(p.CreatedAt), rfc(p.UpdatedAt),
	)
	return err
}

func (r *Repository) ListSyncPairs() ([]SyncPair, error) {
	rows, err := r.db.Query(`SELECT id,name,source_bucket_id,target_bucket_id,sync_interval,worker_count,max_get_ops_per_minute,delete_propagation,target_storage_class,enabled,dry_run,webhook_url,webhook_events,last_sync_at,last_sync_status,consecutive_errors,created_at,updated_at FROM sync_pairs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSyncPairs(rows)
}

func (r *Repository) GetSyncPair(id string) (*SyncPair, error) {
	row := r.db.QueryRow(`SELECT id,name,source_bucket_id,target_bucket_id,sync_interval,worker_count,max_get_ops_per_minute,delete_propagation,target_storage_class,enabled,dry_run,webhook_url,webhook_events,last_sync_at,last_sync_status,consecutive_errors,created_at,updated_at FROM sync_pairs WHERE id = ?`, id)
	return scanSyncPair(row)
}

func (r *Repository) UpdateSyncPair(p *SyncPair) error {
	p.UpdatedAt = time.Now().UTC()
	res, err := r.db.Exec(
		`UPDATE sync_pairs SET name=?,source_bucket_id=?,target_bucket_id=?,
		 sync_interval=?,worker_count=?,max_get_ops_per_minute=?,delete_propagation=?,
		 target_storage_class=?,enabled=?,dry_run=?,webhook_url=?,webhook_events=?,
		 last_sync_at=?,last_sync_status=?,consecutive_errors=?,updated_at=? WHERE id=?`,
		p.Name, p.SourceBucketID, p.TargetBucketID,
		p.SyncInterval, p.WorkerCount, p.MaxGetOpsPerMinute, boolInt(p.DeletePropagation),
		nullStr(p.TargetStorageClass), boolInt(p.Enabled), boolInt(p.DryRun),
		p.WebhookURL, p.WebhookEvents,
		nullTimeRFC(p.LastSyncAt), p.LastSyncStatus,
		p.ConsecutiveErrors, rfc(p.UpdatedAt), p.ID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("sync pair %q not found", p.ID)
	}
	return nil
}

func (r *Repository) DeleteSyncPair(id string) error {
	r.db.Exec(`DELETE FROM sync_logs WHERE pair_id = ?`, id)
	res, err := r.db.Exec(`DELETE FROM sync_pairs WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("sync pair %q not found", id)
	}
	return nil
}

// --- Sync Logs ---

func (r *Repository) CreateSyncLog(entry *SyncLogEntry) error {
	entry.ID = uuid.New().String()
	_, err := r.db.Exec(
		`INSERT INTO sync_logs (id,pair_id,status,error_msg,succeeded,failed,started_at,completed_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		entry.ID, entry.PairID, entry.Status, entry.ErrorMsg,
		entry.Succeeded, entry.Failed,
		rfc(entry.StartedAt), rfc(entry.CompletedAt),
	)
	return err
}

func (r *Repository) ListSyncLogs(pairID string, limit int) ([]SyncLogEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.db.Query(`SELECT id,pair_id,status,error_msg,succeeded,failed,started_at,completed_at FROM sync_logs WHERE pair_id = ? ORDER BY started_at DESC LIMIT ?`, pairID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SyncLogEntry
	for rows.Next() {
		var e SyncLogEntry
		var s, c string
		if err := rows.Scan(&e.ID, &e.PairID, &e.Status, &e.ErrorMsg, &e.Succeeded, &e.Failed, &s, &c); err != nil {
			return nil, err
		}
		e.StartedAt, _ = time.Parse(time.RFC3339, s)
		e.CompletedAt, _ = time.Parse(time.RFC3339, c)
		out = append(out, e)
	}
	return out, rows.Err()
}

// --- scanning helpers ---

func scanBuckets(rows *sql.Rows) ([]Bucket, error) {
	var out []Bucket
	for rows.Next() {
		b, err := scanBucketFromRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}

func scanBucket(row *sql.Row) (*Bucket, error) {
	return scanBucketFromRow(row)
}

type rowScanner interface {
	Scan(...interface{}) error
}

func scanBucketFromRow(s rowScanner) (*Bucket, error) {
	var (
		b        Bucket
		ol, ver  int
		c, u     string
		rm, rd   sql.NullString
	)
	err := s.Scan(&b.ID, &b.Name, &b.Endpoint, &b.Region,
		&b.AccessKey, &b.SecretKey, &b.BucketName,
		&ol, &ver, &rm, &rd, &c, &u)
	if err != nil {
		return nil, err
	}
	b.ObjectLock = ol == 1
	b.Versioning = ver == 1
	b.RetentionMode = rm.String
	if rd.Valid {
		fmt.Sscanf(rd.String, "%d", &b.RetentionDays)
	}
	b.CreatedAt, _ = time.Parse(time.RFC3339, c)
	b.UpdatedAt, _ = time.Parse(time.RFC3339, u)
	return &b, nil
}

func scanSyncPairs(rows *sql.Rows) ([]SyncPair, error) {
	var out []SyncPair
	for rows.Next() {
		p, err := scanSyncPairFromRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func scanSyncPair(row *sql.Row) (*SyncPair, error) {
	return scanSyncPairFromRow(row)
}

func scanSyncPairFromRow(s rowScanner) (*SyncPair, error) {
	var (
		p           SyncPair
		en, dp, dr  int
		c, u        string
		lsa         sql.NullString
		ce          sql.NullInt64
		tsc         sql.NullString
	)
	err := s.Scan(&p.ID, &p.Name, &p.SourceBucketID, &p.TargetBucketID,
		&p.SyncInterval, &p.WorkerCount, &p.MaxGetOpsPerMinute, &dp,
		&tsc, &en, &dr, &p.WebhookURL, &p.WebhookEvents,
		&lsa, &p.LastSyncStatus,
		&ce, &c, &u)
	if err != nil {
		return nil, err
	}
	p.Enabled = en == 1
	p.DeletePropagation = dp == 1
	p.DryRun = dr == 1
	p.TargetStorageClass = tsc.String
	if lsa.Valid {
		t, _ := time.Parse(time.RFC3339, lsa.String)
		p.LastSyncAt = &t
	}
	p.ConsecutiveErrors = int(ce.Int64)
	p.CreatedAt, _ = time.Parse(time.RFC3339, c)
	p.UpdatedAt, _ = time.Parse(time.RFC3339, u)
	return &p, nil
}

// --- value helpers ---

func boolInt(b bool) int {
	if b { return 1 }
	return 0
}

func nullStr(s string) *string {
	if s == "" { return nil }
	return &s
}

func nullInt(n int) *int {
	if n == 0 { return nil }
	return &n
}

func nullTimeRFC(t *time.Time) *string {
	if t == nil { return nil }
	s := t.Format(time.RFC3339)
	return &s
}

func rfc(t time.Time) string {
	return t.Format(time.RFC3339)
}

func decryptCreds(key []byte, b *Bucket) {
	if key == nil {
		return
	}
	if b.AccessKey != "" {
		if d, err := Decrypt(b.AccessKey, key); err == nil {
			b.AccessKey = string(d)
		}
	}
	if b.SecretKey != "" {
		if d, err := Decrypt(b.SecretKey, key); err == nil {
			b.SecretKey = string(d)
		}
	}
}
