// Package lpm provides longest prefix matching implementations for OnPair compression.
package lpm

import (
	"bytes"
	"encoding/binary"
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

const minMatch = 8

// prefixKey is a composite key for short pattern lookups, matching Rust's (u64, u8) tuple
type prefixKey struct {
	prefix uint64
	length uint8
}

// LongestPrefixMatcher is a hybrid longest prefix matcher supporting arbitrary-length patterns.
//
// Combines direct hash lookup for short patterns with bucketed search for long patterns.
// Optimized for OnPair's token discovery phase where most patterns are short but
// long patterns provide significant compression benefits.
type LongestPrefixMatcher struct {
	longMatchBuckets map[uint64][]uint16  // 8-byte prefix → candidate token IDs
	shortMatchLookup map[prefixKey]uint16 // (prefix, length) → token ID
	dictionary       []byte               // Suffix storage for long patterns
	endPositions     []uint32             // Boundary positions in dictionary
}

// NewLongestPrefixMatcher creates a new empty longest prefix matcher.
func NewLongestPrefixMatcher() *LongestPrefixMatcher {
	return &LongestPrefixMatcher{
		longMatchBuckets: make(map[uint64][]uint16),
		shortMatchLookup: make(map[prefixKey]uint16),
		dictionary:       make([]byte, 0, 1024*1024),
		endPositions:     []uint32{0},
	}
}

// Insert inserts a new pattern with associated token ID.
//
// Automatically chooses storage strategy based on pattern length:
// - Short patterns (≤8 bytes): Direct hash table insertion
// - Long patterns (>8 bytes): Bucketed by 8-byte prefix with suffix storage
//
// Long pattern buckets are kept sorted by pattern length (descending) for
// efficient longest-match-first lookup during matching.
//
// IMPORTANT: Token IDs must be inserted sequentially starting from 0!
func (lpm *LongestPrefixMatcher) Insert(entry []byte, id uint16) {
	if len(entry) > minMatch {
		// Long pattern: store 8-byte prefix in bucket, suffix in dictionary
		prefix := bytesToU64LE(entry, minMatch)
		lpm.dictionary = append(lpm.dictionary, entry[minMatch:]...)
		lpm.endPositions = append(lpm.endPositions, uint32(len(lpm.dictionary)))

		bucket := lpm.longMatchBuckets[prefix]
		bucket = append(bucket, id)

		// Sort by pattern length (longest first) for greedy matching
		// Use insertion sort as we add one at a time
		for i := len(bucket) - 1; i > 0; i-- {
			id1 := bucket[i]
			id2 := bucket[i-1]
			len1 := int(lpm.endPositions[id1+1]) - int(lpm.endPositions[id1])
			len2 := int(lpm.endPositions[id2+1]) - int(lpm.endPositions[id2])
			if len1 > len2 {
				bucket[i], bucket[i-1] = bucket[i-1], bucket[i]
			} else {
				break
			}
		}
		lpm.longMatchBuckets[prefix] = bucket
	} else {
		// Short pattern: direct hash table lookup
		prefix := bytesToU64LE(entry, len(entry))
		key := prefixKey{prefix: prefix, length: uint8(len(entry))}
		lpm.shortMatchLookup[key] = id
		lpm.endPositions = append(lpm.endPositions, uint32(len(lpm.dictionary)))
	}
}

// FindLongestMatch finds the longest matching pattern for the given input data.
//
// Returns the token ID and match length for the longest pattern that matches
// the beginning of the input data. Uses two-phase search:
//
// 1. Long pattern search: Check bucketed patterns (>8 bytes) first for longest matches
// 2. Short pattern search: Check direct lookup patterns (≤8 bytes) in decreasing length order
func (lpm *LongestPrefixMatcher) FindLongestMatch(data []byte) (uint16, int, bool) {
	// Phase 1: Long pattern search (>8 bytes) - check longest matches first
	if len(data) > minMatch {
		prefix := bytesToU64LE(data, minMatch)

		if bucket, ok := lpm.longMatchBuckets[prefix]; ok {
			for _, id := range bucket {
				// Bounds check: ensure we don't access beyond endPositions array
				// This can happen when we've filled all 65536 token slots
				if int(id)+1 >= len(lpm.endPositions) {
					continue
				}

				dictStart := int(lpm.endPositions[id])
				dictEnd := int(lpm.endPositions[id+1])

				// Additional safety check for dictionary bounds
				if dictStart < 0 || dictEnd > len(lpm.dictionary) || dictStart > dictEnd {
					continue
				}

				length := dictEnd - dictStart

				// Verify suffix matches beyond the 8-byte prefix
				if len(data[minMatch:]) >= length {
					suffix := lpm.dictionary[dictStart:dictEnd]
					if bytes.HasPrefix(data[minMatch:], suffix) {
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

	for length := maxLen; length >= 1; length-- {
		prefix := bytesToU64LE(data, length)
		key := prefixKey{prefix: prefix, length: uint8(length)}

		if id, ok := lpm.shortMatchLookup[key]; ok {
			return id, length, true
		}
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

	// Fast path using unsafe pointer (like Rust implementation)
	// Safe because we verified len(bytes) >= 8 above
	ptr := unsafe.Pointer(&bytes[0])
	value := *(*uint64)(ptr)
	return value & masks[length]
}
