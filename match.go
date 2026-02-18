package onpair

import (
	"bytes"
	"encoding/binary"
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

// Matcher is a hybrid longest prefix matcher supporting arbitrary-length patterns.
//
// Combines direct hash lookup for short patterns with bucketed search for long patterns.
// Optimized for OnPair's token discovery phase where most patterns are short but
// long patterns provide significant compression benefits.
type Matcher struct {
	longMatchBuckets map[uint64][]uint16  // 8-byte prefix → candidate token IDs
	shortMatchLookup [9]map[uint64]uint16 // length → (prefix, token ID)
	dictionary       []byte               // Suffix storage for long patterns
	endPositions     []uint32             // Boundary positions in dictionary
	onPair16         bool
	bucketSizeLimit  int
}

// newMatcher creates a new empty longest prefix matcher.
func newMatcher(maxTokenLen int) *Matcher {
	onPair16 := maxTokenLen == 16
	bucketSizeLimit := 0
	if onPair16 {
		bucketSizeLimit = maxOnPair16BucketSize
	}

	return &Matcher{
		endPositions:    []uint32{0},
		onPair16:        onPair16,
		bucketSizeLimit: bucketSizeLimit,
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
func (m *Matcher) insert(entry []byte, id uint16) bool {
	if len(entry) > minMatch {
		// Long pattern: store 8-byte prefix in bucket, suffix in dictionary
		prefix := bytesToU64LE(entry, minMatch)
		if m.longMatchBuckets == nil {
			m.longMatchBuckets = make(map[uint64][]uint16)
		}
		bucket := m.longMatchBuckets[prefix]
		if m.bucketSizeLimit > 0 && len(bucket) >= m.bucketSizeLimit {
			return false
		}

		m.dictionary = append(m.dictionary, entry[minMatch:]...)
		m.endPositions = append(m.endPositions, uint32(len(m.dictionary)))
		bucket = append(bucket, id)

		// Sort by pattern length (longest first) for greedy matching
		// Use insertion sort as we add one at a time
		for i := len(bucket) - 1; i > 0; i-- {
			id1 := bucket[i]
			id2 := bucket[i-1]
			len1 := int(m.endPositions[id1+1]) - int(m.endPositions[id1])
			len2 := int(m.endPositions[id2+1]) - int(m.endPositions[id2])
			if len1 > len2 {
				bucket[i], bucket[i-1] = bucket[i-1], bucket[i]
			} else {
				break
			}
		}
		m.longMatchBuckets[prefix] = bucket
	} else {
		// Single-byte tokens are always byte-value identity tokens.
		if len(entry) == 1 {
			m.endPositions = append(m.endPositions, uint32(len(m.dictionary)))
			return true
		}

		// Short pattern: direct hash table lookup
		prefix := bytesToU64LE(entry, len(entry))
		lookup := m.shortMatchLookup[len(entry)]
		if lookup == nil {
			lookup = make(map[uint64]uint16)
			m.shortMatchLookup[len(entry)] = lookup
		}
		lookup[prefix] = id
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
func (m *Matcher) find(data []byte) (uint16, int, bool) {
	// Phase 1: Long pattern search (>8 bytes) - check longest matches first
	if len(data) > minMatch {
		prefix := bytesToU64LE(data, minMatch)
		inputSuffix := data[minMatch:]
		inputPacked := uint64(0)
		inputPackedLen := len(inputSuffix)
		if inputPackedLen > minMatch {
			inputPackedLen = minMatch
		}
		if m.onPair16 {
			inputPacked = bytesToU64LE(inputSuffix, inputPackedLen)
		}

		if bucket, ok := m.longMatchBuckets[prefix]; ok {
			for _, id := range bucket {
				// Bounds check: ensure we don't access beyond endPositions array
				// This can happen when we've filled all 65536 token slots
				if int(id)+1 >= len(m.endPositions) {
					continue
				}

				dictStart := int(m.endPositions[id])
				dictEnd := int(m.endPositions[id+1])

				// Additional safety check for dictionary bounds
				if dictStart < 0 || dictEnd > len(m.dictionary) || dictStart > dictEnd {
					continue
				}

				length := dictEnd - dictStart

				// Verify suffix matches beyond the 8-byte prefix
				if len(inputSuffix) >= length {
					suffix := m.dictionary[dictStart:dictEnd]
					if m.onPair16 && length <= minMatch {
						if hasPackedPrefix(inputPacked, bytesToU64LE(suffix, length), length) {
							return id, minMatch + length, true
						}
						continue
					}
					if bytes.HasPrefix(inputSuffix, suffix) {
						return id, minMatch + length, true
					}
				}
			}
		}
	}

	// Phase 2: Short pattern search (≤8 bytes) - longest to shortest
	maxLen := minMatch
	if len(data) < maxLen {
		maxLen = len(data)
	}

	// Compute prefix once at max length, then mask down per iteration
	prefix := bytesToU64LE(data, maxLen)
	for length := maxLen; length >= 2; length-- {
		maskedPrefix := prefix & masks[length]
		if id, ok := m.shortMatchLookup[length][maskedPrefix]; ok {
			return id, length, true
		}
	}
	if len(data) > 0 {
		return uint16(data[0]), 1, true
	}

	return 0, 0, false
}

func hasPackedPrefix(inputPacked, tokenPacked uint64, tokenLen int) bool {
	if tokenLen <= 0 {
		return true
	}
	if tokenLen > minMatch {
		return false
	}

	diff := inputPacked ^ tokenPacked
	if diff == 0 {
		return true
	}
	return bits.TrailingZeros64(diff)/8 >= tokenLen
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
