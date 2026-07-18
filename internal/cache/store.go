package cache

import (
	"encoding/json"
	"fmt"
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

func (s *Store) Delete(pairID, key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket(pairID))
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
}

func (s *Store) List(pairID string) ([]CachedObject, error) {
	var out []CachedObject
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket(pairID))
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			var o CachedObject
			if err := json.Unmarshal(v, &o); err != nil {
				return err
			}
			out = append(out, o)
			return nil
		})
	})
	return out, err
}

func (s *Store) Clear(pairID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(s.bucket(pairID)); err != nil {
			return nil
		}
		_, err := tx.CreateBucket(s.bucket(pairID))
		return err
	})
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


