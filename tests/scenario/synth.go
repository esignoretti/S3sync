// tests/scenario/synth.go — NO build tag; the determinism properties are unit
// tests that need no endpoint.

package scenario

import (
	"crypto/sha256"
	"fmt"
	"hash/fnv"
	"io"
)

// synthSeed maps a key to a stable 64-bit seed. FNV-1a is used rather than a
// map lookup so that two processes (a test and a probe binary) derive the same
// seed for the same key with no shared state.
func synthSeed(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

// splitmix64 is the standard SplitMix64 step: a bijective mix with no state.
// Unlike math/rand, whose sequence is only stable within one Go version, this
// produces identical bytes on every platform and every toolchain, which is what
// lets a test re-derive an object's content years later without storing it.
func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

// synthBytesAt returns n deterministic bytes starting at absolute offset off.
// Because each 8-byte word is derived from (seed, word index) alone, any window
// of any object can be regenerated without downloading it — the property that
// makes a 6 GiB object verifiable on a 5 GB disk.
func synthBytesAt(key string, off, n int64) []byte {
	seed := synthSeed(key)
	out := make([]byte, n)
	for i := int64(0); i < n; {
		abs := off + i
		wordIndex := uint64(abs / 8)
		word := splitmix64(seed ^ wordIndex)
		// Copy the tail of this word, then whole words, so a multi-megabyte
		// read costs one mix per 8 bytes rather than one per byte.
		start := abs % 8
		remaining := 8 - start
		if remaining > n-i {
			remaining = n - i
		}
		for j := int64(0); j < remaining; j++ {
			out[i+j] = byte(word >> (8 * uint64(start+j)))
		}
		i += remaining
	}
	return out
}

// synthReader streams deterministic bytes for one key. It keeps no buffer of
// the whole object: a 6 GiB fixture must be readable in bounded memory.
type synthReader struct {
	key  string
	size int64
	pos  int64
}

// newSynthReader returns an io.Reader yielding exactly size deterministic bytes.
// Reads of any chunk size are equivalent, because the SDK chooses the chunking
// and each multipart part reads the stream independently.
func newSynthReader(key string, size int64) io.Reader {
	return &synthReader{key: key, size: size}
}

func (r *synthReader) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if r.pos+n > r.size {
		n = r.size - r.pos
	}
	copy(p[:n], synthBytesAt(r.key, r.pos, n))
	r.pos += n
	return int(n), nil
}

// synthChecksum is the SHA-256 of the whole object, streamed so memory stays
// bounded. Assertions compare this rather than re-reading bodies.
func synthChecksum(key string, size int64) [32]byte {
	h := sha256.New()
	buf := make([]byte, 1<<20)
	r := newSynthReader(key, size)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			_, _ = h.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// synthKey names an object after its index and shape, so a failure message
// names both what kind of fixture it was and which one.
func synthKey(i int, shape string) string {
	return fmt.Sprintf("obj/%04d-%s.bin", i, shape)
}

// objectShape is one row of the size table.
type objectShape struct {
	Name string
	Min  int64
	Max  int64
}

// shapes is the fixture size table. "huge" exists to cross the 5 GiB
// CopyObject limit so the multipart path is exercised; "large" crosses the
// multipart part size so a single part is not the whole object.
// The tiny shape is DETERMINISTIC (0 or 1 byte) rather than a range, because the
// 0-byte case is the one a differ and a copy both get wrong; a range would make
// half the runs exercise 1 byte and leave the empty case untested. Use
// synthKey(0,"tiny") for the empty object and synthKey(1,"tiny") for the 1-byte.
var shapes = []objectShape{
	{"tiny", 0, 0},
	{"small", 1 << 10, 64 << 10},
	{"medium", 1 << 20, 8 << 20},
	{"large", 100 << 20, 100 << 20},
	{"huge", 6 << 30, 6 << 30},
}

// synthSize derives an object's size from its key so that a rerun of the same
// scenario produces the same bytes and the same size.
func synthSize(key string, shape objectShape) int64 {
	if shape.Max <= shape.Min {
		return shape.Min
	}
	span := uint64(shape.Max - shape.Min + 1)
	return shape.Min + int64(synthSeed(key)%span)
}
