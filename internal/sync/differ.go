package sync

import "time"

type ActionType int

const (
	ActionCopy ActionType = iota
	ActionDelete
)

type SyncAction struct {
	Type         ActionType
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
}

type cachedEntry struct {
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
	ErrorCount   int
	LastError    string
}

type DiffResult struct {
	NewOrChanged []SyncAction
	ToDelete     []SyncAction
	Skipped      int
}

func Diff(listing []ListedObject, cached []cachedEntry, deletePropagation bool) DiffResult {
	cm := make(map[string]cachedEntry, len(cached))
	for _, c := range cached {
		cm[c.Key] = c
	}

	var actions, deletes []SyncAction
	skipped := 0

	for _, obj := range listing {
		cc, found := cm[obj.Key]
		if !found {
			actions = append(actions, SyncAction{
				Type: ActionCopy, Key: obj.Key,
				ETag: obj.ETag, Size: obj.Size, LastModified: obj.LastModified,
			})
			continue
		}
		if cc.ETag != obj.ETag || !cc.LastModified.Equal(obj.LastModified) {
			actions = append(actions, SyncAction{
				Type: ActionCopy, Key: obj.Key,
				ETag: obj.ETag, Size: obj.Size, LastModified: obj.LastModified,
			})
			continue
		}
		skipped++
	}

	if deletePropagation {
		lk := make(map[string]struct{}, len(listing))
		for _, o := range listing {
			lk[o.Key] = struct{}{}
		}
		for _, c := range cached {
			if _, exists := lk[c.Key]; !exists {
				deletes = append(deletes, SyncAction{Type: ActionDelete, Key: c.Key})
			}
		}
	}

	return DiffResult{NewOrChanged: actions, ToDelete: deletes, Skipped: skipped}
}
