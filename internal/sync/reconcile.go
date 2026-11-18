package sync

import (
	"context"
	"fmt"
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
// were copied, and emits ActionCopy for each.
//
// This is the only detector for the drift that matters most: a tag, metadata or
// retention change bumps neither the ETag nor LastModified, so StreamingDiff
// cannot see it. The cost is why the sweep is opt-in (attrs_reconcile_interval):
// one throttled HeadObject plus one throttled GetObjectTagging per cached object
// per sweep, both charged to the source get-op budget because both are source
// GETs.
//
// A drift is repaired with ActionCopy, not ActionCopyAttributes: the server-side
// copy carries every replicated characteristic in one request, so it can repair
// metadata, content type, retention and storage-class drift alike. The
// attribute-only action rewrites tags alone, so using it here would report a
// repaired object whose other characteristics are still stale.
//
// Entries with an empty AttrsHash are skipped without a read: empty means
// "unknown" (written before this feature existed), never "matches".
func (e *Engine) reconcileSweep(ctx context.Context, actions chan<- SyncAction) error {
	entries, err := e.store.List(e.pair.ID)
	if err != nil {
		return fmt.Errorf("list cache: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	// Resume where the last budget-limited pass stopped. A sweep that cannot
	// finish inside its budget must still make progress: restarting from the
	// front every run means the tail of a large cache is never examined.
	e.mu.Lock()
	cursor := e.sweepCursor
	e.mu.Unlock()

	emitted := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if cursor != "" && entry.Key <= cursor {
			continue
		}
		if entry.AttrsHash == "" {
			e.advanceCursor(entry.Key)
			continue
		}

		headOut, err := e.pool.headSource(ctx, entry.Key)
		if err != nil {
			// One unreadable object must not abort the sweep: the rest of the
			// bucket still needs checking. A transient error leaves the cursor
			// where it is so the next sweep retries this key; a budget expiry
			// must not, or every run would spend its budget on the same object
			// and the sweep would never move.
			slog.Warn("reconcile: head source", "key", entry.Key, "error", err)
			if ctx.Err() != nil {
				e.advanceCursor(entry.Key)
			}
			continue
		}
		if headOut == nil {
			// Vanished since the copy. Not drift, and the differ reports the
			// deletion; a tag read would only repeat the same 404.
			slog.Debug("reconcile: source object vanished", "key", entry.Key)
			e.advanceCursor(entry.Key)
			continue
		}

		tags, err := e.pool.readTags(ctx, entry.Key)
		if err != nil {
			// The object is probably fine; the read was not. Emitting an action
			// here would fabricate drift out of a transient failure. As above:
			// a transient error retries this key next sweep, a budget expiry
			// advances past it so the sweep makes progress.
			slog.Warn("reconcile: read source tags", "key", entry.Key, "error", err)
			if ctx.Err() != nil {
				e.advanceCursor(entry.Key)
			}
			continue
		}
		attrs := e.pool.effectiveAttrs(entry.Key, headOut).withTags(tags)

		if attrs.Hash() == entry.AttrsHash {
			e.advanceCursor(entry.Key)
			continue
		}

		slog.Info("reconcile: attributes drifted", "key", entry.Key)
		select {
		case actions <- SyncAction{
			Type: ActionCopy, Key: entry.Key,
			ETag: entry.ETag, Size: entry.Size,
			LastModified: entry.LastModified,
		}:
			emitted++
		case <-ctx.Done():
			// The key WAS examined, so the cursor advances even though the
			// action could not be delivered: leaving it here would spend every
			// future budget on this same object and the sweep would never move
			// past it. The repair is picked up on the next full pass.
			e.advanceCursor(entry.Key)
			return ctx.Err()
		}
		e.advanceCursor(entry.Key)
	}

	// A budget expiry is not a completed sweep. Returning nil here would make
	// the caller stamp LastReconcileAt as if the whole cache had been examined,
	// deferring the next sweep by a full interval and hiding the drift this
	// sweep never reached.
	if err := ctx.Err(); err != nil {
		return err
	}

	// Reaching the end of the list means the whole cache has been examined, so
	// the next sweep starts over from the front.
	e.mu.Lock()
	e.sweepCursor = ""
	e.mu.Unlock()

	if emitted > 0 {
		slog.Info("reconcile sweep", "pair", e.pair.Name, "objects", len(entries), "drifted", emitted)
	}
	return nil
}
