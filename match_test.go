package onpair

import (
	"bytes"
	"math"
	"testing"
)

func TestMatcherInsertRejectsOversizedPattern(t *testing.T) {
	// suffixLens stores uint16: a pattern whose suffix exceeds 65535 bytes
	// must be rejected, not silently truncated (regression: truncation made
	// greedy matching advance by the wrapped length, corrupting round-trips).
	m := newMatcher(0)
	limit := make([]byte, minMatch+math.MaxUint16)
	if !m.insert(limit, 256) {
		t.Fatal("pattern at the representable limit should insert")
	}
	over := make([]byte, minMatch+math.MaxUint16+1)
	if m.insert(over, 257) {
		t.Fatal("pattern past the representable limit must be rejected")
	}
}

func TestRoundTripHighlyRepetitiveDefaultConfig(t *testing.T) {
	// Threshold 1 doubles token lengths aggressively; before the insert guard
	// this crossed the 65,543-byte representable limit and round-trips came
	// back with the wrong length.
	input := string(bytes.Repeat([]byte{'a'}, 512*1024))
	archive := mustEncode(NewEncoder(WithThreshold(1)), []string{input})
	out, err := archive.AppendRow(nil, 0)
	if err != nil {
		t.Fatalf("AppendRow: %v", err)
	}
	if string(out) != input {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(out), len(input))
	}
}

func TestOnPair16MatcherBucketBound(t *testing.T) {
	m := newMatcher(16)
	prefix := []byte("abcdefgh")
	prefixKey := bytesToU64LE(prefix, minMatch)

	inserted := 0
	for i := 0; i < maxOnPair16BucketSize+32; i++ {
		entry := append([]byte(nil), prefix...)
		entry = append(entry, byte(i), byte(i>>8))
		if m.insert(entry, uint16(inserted)) {
			inserted++
		}
	}

	if inserted != maxOnPair16BucketSize {
		t.Fatalf("inserted long-token count mismatch: got %d want %d", inserted, maxOnPair16BucketSize)
	}
	if got := m.longMatchBuckets.get(prefixKey).len(); got != maxOnPair16BucketSize {
		t.Fatalf("bucket size mismatch: got %d want %d", got, maxOnPair16BucketSize)
	}
}

func TestOnPair16MatcherFindLongToken(t *testing.T) {
	m := newMatcher(16)
	token := []byte("abcdefghXYZ")

	if !m.insert(token, 0) {
		t.Fatalf("insert failed")
	}
	id, n, ok := m.find([]byte("abcdefghXYZ_tail"))
	if !ok {
		t.Fatalf("expected long match")
	}
	if id != 0 {
		t.Fatalf("token id mismatch: got %d want 0", id)
	}
	if n != len(token) {
		t.Fatalf("token length mismatch: got %d want %d", n, len(token))
	}
}
