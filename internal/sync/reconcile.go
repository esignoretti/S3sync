package sync

import (
	"context"
	"log/slog"
	"time"

	"github.com/esignoretti/S3sync/internal/config"
)

// ShouldReconcile reports whether a drift sweep is due.
//
// attrs_reconcile_interval is a duration in seconds; zero or negative means the
// sweep is off, which keeps existing pairs' cost unchanged. now is a parameter so
// the caller and the tests agree on the clock.
func ShouldReconcile(pair *config.SyncPair, now time.Time) bool {
	if pair.AttrsReconcileInterval <= 0 {
		return false
	}
	if pair.LastReconcileAt == nil {
		return true
	}
	elapsed := now.Sub(*pair.LastReconcileAt)
	return elapsed >= time.Duration(pair.AttrsReconcileInterval)*time.Second
}

// reconcileSweep finds objects whose characteristics drifted after their bytes
// were copied, and emits ActionCopyAttributes for each.
//
// This is the only detector for the drift that matters most: a tag, metadata or
// retention change bumps neither the ETag nor LastModified, so StreamingDiff
// cannot see it. The cost is why the sweep is opt-in (attrs_reconcile_interval):
// one throttled HeadObject plus one throttled GetObjectTagging per cached object
// per sweep, both charged to the source get-op budget because both are source
// GETs.
//
// Entries with an empty AttrsHash are skipped without a read: empty means
// "unknown" (written before this feature existed), never "matches".
func (e *Engine) reconcileSweep(ctx context.Context, actions chan<- SyncAction) error {
	entries, err := e.store.List(e.pair.ID)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	emitted := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.AttrsHash == "" {
			continue
		}

		headOut, err := e.pool.headSource(ctx, entry.Key)
		if err != nil {
			// One unreadable object must not abort the sweep: the rest of the
			// bucket still needs checking. The next sweep re-reads this one.
			slog.Warn("reconcile: head source", "key", entry.Key, "error", err)
			continue
		}
		if headOut == nil {
			// Vanished since the copy. Not drift, and the differ reports the
			// deletion; a tag read would only repeat the same 404.
			slog.Debug("reconcile: source object vanished", "key", entry.Key)
			continue
		}

		attrs := e.pool.effectiveAttrs(entry.Key, headOut)
		tags, err := e.pool.sourceTags(ctx, entry.Key)
		if err != nil {
			// The object is probably fine; the read was not. Emitting an action
			// here would fabricate drift out of a transient failure.
			slog.Warn("reconcile: read source tags", "key", entry.Key, "error", err)
			continue
		}
		attrs.Tags = tags

		if attrs.Hash() == entry.AttrsHash {
			continue
		}

		slog.Info("reconcile: attributes drifted", "key", entry.Key)
		select {
		case actions <- SyncAction{
			Type: ActionCopyAttributes, Key: entry.Key,
			ETag: entry.ETag, Size: entry.Size,
			LastModified: entry.LastModified,
		}:
			emitted++
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if emitted > 0 {
		slog.Info("reconcile sweep", "pair", e.pair.Name, "objects", len(entries), "drifted", emitted)
	}
	return nil
}
