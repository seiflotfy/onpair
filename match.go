package onpair

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/bits"
	"unsafe"
)

// Bit masks for extracting prefixes of different lengths (little-endian)
var masks = [9]uint64{
	0x0000000000000000, // 0 bytes
	0x00000000000000FF, // 1 byte
	0x000000000000FFFF, // 2 bytes
	0x0000000000FFFFFF, // 3 bytes
	0x00000000FFFFFFFF, // 4 bytes
	0x000000FFFFFFFFFF, // 5 bytes
	0x0000FFFFFFFFFFFF, // 6 bytes
	0x00FFFFFFFFFFFFFF, // 7 bytes
	0xFFFFFFFFFFFFFFFF, // 8 bytes
}

const (
	minMatch              = 8
	maxOnPair16BucketSize = 128
)

// matcher is a hybrid longest prefix matcher supporting patterns up to
// 8+65535 bytes (insert rejects longer ones).
//
// Combines direct table lookup for short patterns with bucketed search for
// long patterns. Both paths are gated by 2-byte-prefix filters so find()
// skips the table probe entirely when no stored pattern starts with those
// bytes.
type matcher struct {
	longMatchBuckets longBucketTable          // 8-byte prefix → candidate bucket (sorted desc by suffix length)
	shortPacked      [7]u64U16PackedTable     // lengths 2-6: key fits 48 bits, value packed in the slot
	shortMatchLookup [9]u64U16Table           // lengths 7-8: key needs more than 48 bits
	lengthByPrefix2  *[65536]uint8            // bit L-1 set ⇒ some short token of length L starts with these 2 LE bytes
	longBloom        *[longBloomWords]uint64  // bit at hash(8-byte prefix) set ⇒ that exact bucket key may exist
	shortBloom       *[shortBloomWords]uint64 // bit at hash(masked prefix) set ⇒ that exact short key may exist
	dictionary       []byte                   // Suffix storage for long patterns
	endPositions     []uint32                 // Boundary positions in dictionary
	byteTokens       [256]uint16              // single-byte fallback ids; identity until applyRemap
	onPair16         bool
	bucketSizeLimit  int
}

// longBloomWords sizes the long-prefix filter at 2^18 bits (32 KiB). Keyed by
// a hash of the full 8-byte prefix it answers "could this exact bucket
// exist?", unlike the 2-byte gate it replaced which passed for any input
// sharing two leading bytes with any long token (~80% useless bucket probes
// on text). ~30k distinct prefixes → ~11% false positives, and it stays small
// enough to live in L1/L2 next to the parse.
const longBloomWords = 1 << 12

// shortBloomWords sizes the short-key filter at 2^19 bits (64 KiB). One
// shared filter over the masked keys of every short table: ~70% of short
// probes are misses on this workload, and a miss resolved in this L2-resident
// bitset skips the load into the much larger table it would otherwise touch.
// The bit index reuses the table's own key hash, so a passing check costs the
// probe nothing extra.
const shortBloomWords = 1 << 13

// longBucket is a struct-of-arrays layout. heads + suffixLens are the hot
// arrays touched on every probe (reject path); ids + dictStarts are cold,
// read only when the packed head matches and the suffix exceeds 8 bytes.
// Keeping them in parallel slices cuts the reject-path working set from
// 16 B/entry (the equivalent AoS struct) to 10 B/entry.
type longBucket struct {
	heads      []uint64 // first min(suffixLen, 8) bytes of suffix, LE, zero-extended
	suffixLens []uint16 // bytes past the 8-byte prefix; token length = suffixLen + 8
	ids        []uint16
	dictStarts []uint32 // offset in m.dictionary where this suffix starts
}

func (b *longBucket) len() int { return len(b.heads) }

func (b *longBucket) appendEntry(head uint64, suffixLen uint16, id uint16, dictStart uint32) {
	b.heads = append(b.heads, head)
	b.suffixLens = append(b.suffixLens, suffixLen)
	b.ids = append(b.ids, id)
	b.dictStarts = append(b.dictStarts, dictStart)
}

func (b *longBucket) swap(i, j int) {
	b.heads[i], b.heads[j] = b.heads[j], b.heads[i]
	b.suffixLens[i], b.suffixLens[j] = b.suffixLens[j], b.suffixLens[i]
	b.ids[i], b.ids[j] = b.ids[j], b.ids[i]
	b.dictStarts[i], b.dictStarts[j] = b.dictStarts[j], b.dictStarts[i]
}

// newMatcher creates a new empty longest prefix matcher.
func newMatcher(maxTokenLen int) *matcher {
	onPair16 := maxTokenLen == 16
	bucketSizeLimit := 0
	if onPair16 {
		bucketSizeLimit = maxOnPair16BucketSize
	}

	m := &matcher{
		endPositions:    []uint32{0},
		onPair16:        onPair16,
		bucketSizeLimit: bucketSizeLimit,
	}
	for i := range m.byteTokens {
		m.byteTokens[i] = uint16(i)
	}
	return m
}

// applyRemap rewrites every stored token id through remap, so find returns
// final (sorted-dictionary) ids directly and compress needs no per-token
// indirection. Values stay non-zero — sorted id 0 always belongs to the
// single-byte token 0x00, which no merged token can byte-equal — so the
// tables' zero-value empty sentinels remain valid.
func (m *matcher) applyRemap(remap []uint16) {
	for i := range m.byteTokens {
		m.byteTokens[i] = remap[i]
	}
	for l := 2; l <= 6; l++ {
		t := &m.shortPacked[l]
		for i, s := range t.slots {
			if s != 0 {
				t.slots[i] = s&packedKeyMask | uint64(remap[uint16(s>>48)])<<48
			}
		}
	}
	for l := 7; l <= 8; l++ {
		t := &m.shortMatchLookup[l]
		for i := range t.entries {
			if t.entries[i].value != 0 {
				t.entries[i].value = remap[t.entries[i].value]
			}
		}
	}
	for i := range m.longMatchBuckets.entries {
		bucket := m.longMatchBuckets.entries[i].bucket
		if bucket == nil {
			continue
		}
		for j := range bucket.ids {
			bucket.ids[j] = remap[bucket.ids[j]]
		}
	}
}

// insert inserts a new pattern with associated token ID.
//
// Automatically chooses storage strategy based on pattern length:
// - Short patterns (≤8 bytes): Direct hash table insertion
// - Long patterns (>8 bytes): Bucketed by 8-byte prefix with suffix storage
//
// Long pattern buckets are kept sorted by pattern length (descending) for
// efficient longest-match-first lookup during matching.
//
// IMPORTANT: Token IDs must be inserted sequentially starting from 0!
func (m *matcher) insert(entry []byte, id uint16) bool {
	if len(entry) > minMatch {
		if len(entry)-minMatch > math.MaxUint16 {
			// suffixLens stores uint16; a longer pattern would silently
			// truncate and corrupt greedy matching, so reject it instead.
			return false
		}
		// Long pattern: store 8-byte prefix in bucket, suffix in dictionary
		prefix := bytesToU64LE(entry, minMatch)
		bucket := m.longMatchBuckets.get(prefix)
		if bucket != nil && m.bucketSizeLimit > 0 && bucket.len() >= m.bucketSizeLimit {
			return false
		}
		if bucket == nil {
			// Pre-size the four parallel arrays: one allocation each instead
			// of the nil→1→2→4 append doublings most buckets would do.
			bucket = &longBucket{
				heads:      make([]uint64, 0, 4),
				suffixLens: make([]uint16, 0, 4),
				ids:        make([]uint16, 0, 4),
				dictStarts: make([]uint32, 0, 4),
			}
			m.longMatchBuckets.set(prefix, bucket)
		}

		suffix := entry[minMatch:]
		suffixLen := len(suffix)
		headLen := suffixLen
		if headLen > minMatch {
			headLen = minMatch
		}
		head := bytesToU64LE(suffix, headLen)
		dictStart := uint32(len(m.dictionary))

		m.dictionary = append(m.dictionary, suffix...)
		m.endPositions = append(m.endPositions, uint32(len(m.dictionary)))
		bucket.appendEntry(head, uint16(suffixLen), id, dictStart)

		if m.longBloom == nil {
			m.longBloom = new([longBloomWords]uint64)
		}
		h := hashU64(prefix)
		m.longBloom[(h>>6)&(longBloomWords-1)] |= 1 << (h & 63)

		// Sort by suffix length (longest first) for greedy matching.
		// Insertion sort as we add one at a time.
		for i := bucket.len() - 1; i > 0; i-- {
			if bucket.suffixLens[i] > bucket.suffixLens[i-1] {
				bucket.swap(i, i-1)
			} else {
				break
			}
		}
	} else {
		// Single-byte tokens are always byte-value identity tokens.
		if len(entry) == 1 {
			m.endPositions = append(m.endPositions, uint32(len(m.dictionary)))
			return true
		}

		// Short pattern: direct hash table lookup
		prefix := bytesToU64LE(entry, len(entry))
		if len(entry) <= 6 {
			m.shortPacked[len(entry)].set(prefix, id)
		} else {
			m.shortMatchLookup[len(entry)].set(prefix, id)
		}
		if m.shortBloom == nil {
			m.shortBloom = new([shortBloomWords]uint64)
		}
		sh := hashU64(prefix)
		m.shortBloom[(sh>>6)&(shortBloomWords-1)] |= 1 << (sh & 63)
		if m.lengthByPrefix2 == nil {
			m.lengthByPrefix2 = new([65536]uint8)
		}
		// Bit L-1 for length L: lengths 2..8 fit in uint8, where bit 8 of a
		// plain 1<<L would overflow to zero and make 8-byte tokens unmatchable.
		m.lengthByPrefix2[uint16(prefix)] |= 1 << uint(len(entry)-1)
		m.endPositions = append(m.endPositions, uint32(len(m.dictionary)))
	}
	return true
}

// find finds the longest matching pattern for the given input data.
//
// Returns the token ID and match length for the longest pattern that matches
// the beginning of the input data. Uses two-phase search:
//
// 1. Long pattern search: Check bucketed patterns (>8 bytes) first for longest matches
// 2. Short pattern search: Check direct lookup patterns (≤8 bytes) in decreasing length order
func (m *matcher) find(data []byte) (uint16, int, bool) {
	// The first up-to-8 bytes serve as both the long-bucket prefix key and the
	// short-lookup probe window, so load them once.
	maxLen := minMatch
	if len(data) < maxLen {
		maxLen = len(data)
	}
	low8 := bytesToU64LE(data, maxLen)

	// Phase 1: Long pattern search (>8 bytes) - check longest matches first.
	// Gate the table probe behind the full-prefix bloom so inputs without a
	// bucket for these exact 8 bytes skip it entirely.
	if len(data) > minMatch && m.longBloom != nil {
		bh := hashU64(low8)
		if m.longBloom[(bh>>6)&(longBloomWords-1)]&(1<<(bh&63)) != 0 {
			inputSuffix := data[minMatch:]
			inputHeadLen := len(inputSuffix)
			if inputHeadLen > minMatch {
				inputHeadLen = minMatch
			}
			inputHead := bytesToU64LE(inputSuffix, inputHeadLen)

			if bucket := m.longMatchBuckets.get(low8); bucket != nil {
				heads := bucket.heads
				lens := bucket.suffixLens
				for i := 0; i < len(heads); i++ {
					sLen := int(lens[i])
					if sLen > len(inputSuffix) {
						continue
					}
					// Packed head prefilter: XOR the stored head with the input's
					// head masked to the relevant length. For suffixes ≤ 8 bytes
					// this is authoritative; otherwise it's a cheap reject.
					mLen := sLen
					if mLen > minMatch {
						mLen = minMatch
					}
					if (heads[i]^inputHead)&masks[mLen] != 0 {
						continue
					}
					if sLen <= minMatch {
						return bucket.ids[i], minMatch + sLen, true
					}
					// Suffix longer than 8 bytes: verify the tail past the head.
					start := int(bucket.dictStarts[i])
					if bytes.Equal(m.dictionary[start+minMatch:start+sLen], inputSuffix[minMatch:sLen]) {
						return bucket.ids[i], minMatch + sLen, true
					}
				}
			}
		}
	}

	// Phase 2: Short pattern search (≤8 bytes) - longest to shortest.
	// Use 2-byte-prefix bitmask to skip lengths with no candidates. Lengths 8
	// and 7 live in the wide tables and are unrolled; 6 and below probe the
	// packed tables.
	if maxLen >= 2 && m.lengthByPrefix2 != nil {
		lenMask := m.lengthByPrefix2[uint16(low8)]
		// Drop bits for lengths > maxLen (bit L-1 encodes length L).
		lenMask &= (1 << uint(maxLen)) - 1
		bloom := m.shortBloom
		if lenMask >= 0x40 {
			if lenMask&0x80 != 0 {
				h := hashU64(low8)
				if bloom[(h>>6)&(shortBloomWords-1)]&(1<<(h&63)) != 0 {
					if id, ok := m.shortMatchLookup[8].getHashed(low8, h); ok {
						return id, 8, true
					}
				}
			}
			if lenMask&0x40 != 0 {
				key := low8 & masks[7]
				h := hashU64(key)
				if bloom[(h>>6)&(shortBloomWords-1)]&(1<<(h&63)) != 0 {
					if id, ok := m.shortMatchLookup[7].getHashed(key, h); ok {
						return id, 7, true
					}
				}
			}
			lenMask &= 0x3F
		}
		for lenMask != 0 {
			length := bits.Len8(lenMask)
			key := low8 & masks[length]
			h := hashU64(key)
			if bloom[(h>>6)&(shortBloomWords-1)]&(1<<(h&63)) != 0 {
				if id, ok := m.shortPacked[length].getHashed(key, h); ok {
					return id, length, true
				}
			}
			lenMask &^= 1 << (length - 1)
		}
	}
	if len(data) > 0 {
		return m.byteTokens[data[0]], 1, true
	}

	return 0, 0, false
}

// bytesToU64LE converts byte sequence to little-endian u64 with length masking.
func bytesToU64LE(bytes []byte, length int) uint64 {
	// Clamp length to valid range
	if length > 8 {
		length = 8
	}
	if length < 0 {
		length = 0
	}

	if len(bytes) < 8 {
		// Safe path for short slices
		var buf [8]byte
		copy(buf[:], bytes)
		value := binary.LittleEndian.Uint64(buf[:])
		return value & masks[length]
	}

	// Fast path using unsafe pointer
	// Safe because we verified len(bytes) >= 8 above
	ptr := unsafe.Pointer(&bytes[0])
	value := *(*uint64)(ptr)
	return value & masks[length]
}
