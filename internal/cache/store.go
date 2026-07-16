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
			return nil // bucket didn't exist
		}
		_, err := tx.CreateBucket(s.bucket(pairID))
		return err
	})
}
