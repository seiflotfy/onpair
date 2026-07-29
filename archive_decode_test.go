package onpair

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestDecodeFastMatchesGeneric differential-tests decodeFast (the assembly
// kernel on arm64/amd64) against decodeFastGeneric across randomized token
// boundaries — including corrupt, wrapped, and dictionary-end cases — code
// streams, and buffer sizes. Both must stop at the same token with the same
// write offset and byte-identical output, over-stores included.
func TestDecodeFastMatchesGeneric(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for round := 0; round < 2000; round++ {
		dictLen := rng.Intn(64)
		dict := make([]byte, dictLen)
		rng.Read(dict)

		numTokens := 1 + rng.Intn(20)
		bounds := make([]uint32, numTokens+1)
		for i := 1; i <= numTokens; i++ {
			switch rng.Intn(8) {
			case 0: // corrupt: random 32-bit value, possibly wrapped or huge
				bounds[i] = rng.Uint32()
			case 1: // long token
				bounds[i] = bounds[i-1] + uint32(rng.Intn(40))
			default: // plausible short token
				bounds[i] = bounds[i-1] + uint32(rng.Intn(17))
			}
		}

		codes := make([]uint16, rng.Intn(30))
		for i := range codes {
			codes[i] = uint16(rng.Intn(numTokens + 2)) // occasionally out of range
		}

		bufLen := rng.Intn(200)
		bufA := make([]byte, bufLen)
		bufG := make([]byte, bufLen)
		w := 0
		if bufLen > 0 {
			w = rng.Intn(bufLen)
		}
		start := 0
		if len(codes) > 0 {
			start = rng.Intn(len(codes))
		}

		iA, wA := decodeFast(bufA, w, codes, start, bounds, dict)
		iG, wG := decodeFastGeneric(bufG, w, codes, start, bounds, dict)
		if iA != iG || wA != wG {
			t.Fatalf("round %d: kernel (i=%d w=%d) != generic (i=%d w=%d)\nbounds=%v codes=%v dictLen=%d bufLen=%d w0=%d start=%d",
				round, iA, wA, iG, wG, bounds, codes, dictLen, bufLen, w, start)
		}
		if !bytes.Equal(bufA, bufG) {
			t.Fatalf("round %d: kernel output differs from generic\nbounds=%v codes=%v dictLen=%d bufLen=%d w0=%d start=%d",
				round, bounds, codes, dictLen, bufLen, w, start)
		}
	}
}
