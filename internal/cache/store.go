package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.etcd.io/bbolt"
)

type Store struct {
	db *bbolt.DB
}

func Open(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bolt: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) bucket(pairID string) []byte {
	return []byte("cache_" + pairID)
}

func (s *Store) ensureBucket(pairID string, tx *bbolt.Tx) (*bbolt.Bucket, error) {
	return tx.CreateBucketIfNotExists(s.bucket(pairID))
}

// Put stores one object. Production writes go through PutMany so a run's
// results land in a single transaction; this single-object form is kept for
// tests (store_test.go, differ_test.go).
func (s *Store) Put(obj *CachedObject) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := s.ensureBucket(obj.PairID, tx)
		if err != nil {
			return err
		}
		data, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		return b.Put([]byte(obj.Key), data)
	})
}

func (s *Store) PutMany(objs []*CachedObject) error {
	if len(objs) == 0 {
		return nil
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		buckets := make(map[string]*bbolt.Bucket)
		for _, obj := range objs {
			b, ok := buckets[obj.PairID]
			if !ok {
				var err error
				b, err = s.ensureBucket(obj.PairID, tx)
				if err != nil {
					return err
				}
				buckets[obj.PairID] = b
			}
			data, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(obj.Key), data); err != nil {
				return err
			}
		}
		return nil
	})
}

// Delete removes one key. Production deletions go through DeleteMany for the
// same single-transaction reason; this form is kept for tests.
func (s *Store) Delete(pairID, key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket(pairID))
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
}

// DeleteMany removes keys in a single transaction. Deletes must not be written
// back as cached objects: a key deleted from the target that stays in the cache
// is re-diffed as a deletion on every subsequent run, so the pair never
// converges and issues one DELETE per run forever.
func (s *Store) DeleteMany(pairID string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket(pairID))
		if b == nil {
			return nil
		}
		for _, k := range keys {
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Clear resets a pair's cache to a valid, empty bucket. A missing bucket is the
// ordinary first-run case and must still end with a bucket present; anything
// else is a real failure and is reported rather than discarded.
//
// Note on reachability: bbolt's public Tx API only ever returns
// ErrBucketNotFound from a writable, open transaction's DeleteBucket (the root
// bucket is unexported, so no plain top-level value can be created and
// ErrIncompatibleValue cannot arise). The error branch below is therefore
// defensive, but it keeps a future bbolt change or a corrupt file from being
// reported as a successful reset.
func (s *Store) Clear(pairID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(s.bucket(pairID)); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
			return fmt.Errorf("clear cache bucket for pair %q: %w", pairID, err)
		}
		_, err := tx.CreateBucket(s.bucket(pairID))
		return err
	})
}

func (s *Store) PruneIfNeeded(pairID string, maxSizeMB, ttlDays int) (int64, error) {
	if maxSizeMB <= 0 || ttlDays <= 0 {
		return 0, nil
	}
	var removed int64
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket(pairID))
		if b == nil {
			return nil
		}
		stats := b.Stats()
		totalSize := stats.BranchAlloc + stats.LeafAlloc
		maxBytes := maxSizeMB * 1024 * 1024
		if int(totalSize) < maxBytes {
			return nil
		}
		cutoff := time.Now().AddDate(0, 0, -ttlDays)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var obj CachedObject
			if err := json.Unmarshal(v, &obj); err != nil {
				continue
			}
			if obj.SyncedAt.Before(cutoff) {
				if err := b.Delete(k); err != nil {
					return err
				}
				removed++
			}
		}
		return nil
	})
	if removed > 0 {
		slog.Info("cache pruned", "pair", pairID, "removed", removed)
	}
	return removed, err
}

type CacheCursor struct {
	tx  *bbolt.Tx
	c   *bbolt.Cursor
	obj CachedObject
	key string
	err error
}

func (s *Store) NewCursor(pairID string) (*CacheCursor, func(), error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, nil, err
	}
	close := func() { tx.Rollback() }

	b := tx.Bucket(s.bucket(pairID))
	if b == nil {
		return &CacheCursor{tx: tx}, close, nil
	}

	return &CacheCursor{tx: tx, c: b.Cursor()}, close, nil
}

// Next advances the cursor and loads the next cached object.
// Returns false when exhausted. After Next returns true,
// Key() and Object() return the current item's data.
func (c *CacheCursor) Next() bool {
	var k, v []byte
	if c.c == nil {
		return false
	}
	if c.key == "" {
		k, v = c.c.First()
	} else {
		k, v = c.c.Next()
	}
	if k == nil {
		return false
	}
	c.key = string(k)
	c.err = json.Unmarshal(v, &c.obj)
	return c.err == nil
}

func (c *CacheCursor) Key() string {
	return c.key
}

func (c *CacheCursor) Object() CachedObject {
	return c.obj
}
