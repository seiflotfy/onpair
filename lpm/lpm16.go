package lpm

import (
	"encoding/binary"
	"math/bits"

	"github.com/cespare/xxhash/v2"
)

const (
	nInlineSuffixes = 4
	maxBucketSize   = 128
)

// prefixKey16 is a composite key for short pattern lookups in LPM16
type prefixKey16 struct {
	prefix uint64
	length uint8
}

// LongestPrefixMatcher16 is a dynamic longest prefix matcher for 16-byte constrained patterns.
//
// Used during OnPair16's training phase. Optimized for incremental pattern insertion
// with length constraint enabling simplified data structures and faster operations.
type LongestPrefixMatcher16 struct {
	dictionary map[prefixKey16]uint16  // (prefix, length) → token ID
	buckets    map[uint64][]bucketEntry // 8-byte prefix → (suffix, len, ID) entries
}

type bucketEntry struct {
	suffix uint64
	length uint8
	id     uint16
}

// NewLongestPrefixMatcher16 creates a new empty matcher for training phase.
func NewLongestPrefixMatcher16() *LongestPrefixMatcher16 {
	return &LongestPrefixMatcher16{
		dictionary: make(map[prefixKey16]uint16),
		buckets:    make(map[uint64][]bucketEntry),
	}
}

// Insert inserts a pattern with length constraint checking.
//
// Returns false if the pattern would cause bucket overflow.
//
// Length-based storage strategy:
// - ≤8 bytes: Direct dictionary storage
// - >8 bytes: Bucketed with 8-byte prefix + up to 8-byte suffix
func (lpm *LongestPrefixMatcher16) Insert(data []byte, id uint16) bool {
	length := len(data)

	if length <= 8 {
		// Short pattern: direct hash table storage
		value := bytesToU64LE(data, length)
		key := prefixKey16{prefix: value, length: uint8(length)}
		lpm.dictionary[key] = id
		return true
	}

	// Long pattern: bucketed storage with overflow protection
	prefix := bytesToU64LE(data, 8)
	bucket := lpm.buckets[prefix]

	// Reject patterns that would cause bucket overflow
	if len(bucket) > maxBucketSize {
		return false
	}

	suffixLen := length - 8
	suffix := bytesToU64LE(data[8:], suffixLen)

	bucket = append(bucket, bucketEntry{
		suffix: suffix,
		length: uint8(suffixLen),
		id:     id,
	})

	// Sort by suffix length (longest first) for greedy matching
	for i := len(bucket) - 1; i > 0; i-- {
		if bucket[i].length > bucket[i-1].length {
			bucket[i], bucket[i-1] = bucket[i-1], bucket[i]
		} else {
			break
		}
	}

	lpm.buckets[prefix] = bucket
	return true
}

// FindLongestMatch finds longest matching pattern with 16-byte constraint optimization.
func (lpm *LongestPrefixMatcher16) FindLongestMatch(data []byte) (uint16, int, bool) {
	// Phase 1: Long pattern search (>8 bytes)
	if len(data) > 8 {
		suffixLen := len(data)
		if suffixLen > 16 {
			suffixLen = 16
		}
		suffixLen -= 8

		prefix := bytesToU64LE(data, 8)
		suffix := bytesToU64LE(data[8:], suffixLen)

		if bucket, ok := lpm.buckets[prefix]; ok {
			for _, entry := range bucket {
				// Fast bitwise prefix comparison
				if isPrefix(suffix, entry.suffix, suffixLen, int(entry.length)) {
					return entry.id, 8 + int(entry.length), true
				}
			}
		}
	}

	// Phase 2: Short pattern search (≤8 bytes)
	prefix := bytesToU64LE(data, min(8, len(data)))
	for length := min(8, len(data)); length >= 1; length-- {
		prefix = prefix & masks[length]
		key := prefixKey16{prefix: prefix, length: uint8(length)}
		if id, ok := lpm.dictionary[key]; ok {
			return id, length, true
		}
	}

	return 0, 0, false
}

// Finalize converts dynamic matcher to optimized static representation.
//
// Transitions from training-optimized data structures to parsing-optimized
// structures. The static version uses more efficient layouts optimized for
// read-only access during the parsing phase.
func (lpm *LongestPrefixMatcher16) Finalize() *StaticLongestPrefixMatcher16 {
	longDictionary := make(map[uint64]*LongMatchInfo)
	var longBuckets []bucketEntry

	for prefix, bucket := range lpm.buckets {
		answerID, answerLength, _ := lpm.FindLongestMatch(uint64ToBytes(prefix))

		offset := uint16(len(longBuckets))
		nSuffixes := uint16(0)

		var inlineSuffixes [nInlineSuffixes]uint64
		var inlineLengths [nInlineSuffixes]uint8
		var inlineIDs [nInlineSuffixes]uint16

		for i := 0; i < nInlineSuffixes && i < len(bucket); i++ {
			inlineSuffixes[i] = bucket[i].suffix
			inlineLengths[i] = bucket[i].length
			inlineIDs[i] = bucket[i].id
			nSuffixes++
		}

		for i := nInlineSuffixes; i < len(bucket); i++ {
			longBuckets = append(longBuckets, bucket[i])
			nSuffixes++
		}

		info := &LongMatchInfo{
			prefix:         prefix,
			answerID:       answerID,
			answerLength:   uint8(answerLength),
			nSuffixes:      nSuffixes,
			inlineSuffixes: inlineSuffixes,
			inlineLengths:  inlineLengths,
			inlineIDs:      inlineIDs,
			offset:         offset,
		}

		longDictionary[prefix] = info
	}

	shortDictionary := make(map[prefixKey16]uint16)

	for key, id := range lpm.dictionary {
		if key.length == 8 {
			if _, exists := longDictionary[key.prefix]; exists {
				continue
			}

			info := &LongMatchInfo{
				prefix:       key.prefix,
				answerID:     id,
				answerLength: key.length,
				nSuffixes:    0,
				offset:       0,
			}
			longDictionary[key.prefix] = info
			continue
		}

		shortDictionary[key] = id
	}

	// Build minimal perfect hash table for long patterns
	prefixes := make([]uint64, 0, len(longDictionary))
	for prefix := range longDictionary {
		prefixes = append(prefixes, prefix)
	}

	// Use minimal perfect hash function for collision-free lookups
	mph := newMinimalPerfectHash(prefixes)
	longInfo := make([]*LongMatchInfo, mph.tableSize)

	for prefix, info := range longDictionary {
		index := mph.hash(prefix)
		longInfo[index] = info
	}

	return &StaticLongestPrefixMatcher16{
		shortDictionary: shortDictionary,
		longMPH:         mph,
		longInfo:        longInfo,
		longBuckets:     longBuckets,
	}
}

// LongMatchInfo is cache-aligned metadata for efficient long pattern matching.
//
// Includes inline storage for up to nInlineSuffixes patterns to minimize
// memory indirection during the parsing phase.
type LongMatchInfo struct {
	prefix         uint64
	inlineSuffixes [nInlineSuffixes]uint64
	inlineLengths  [nInlineSuffixes]uint8
	inlineIDs      [nInlineSuffixes]uint16
	nSuffixes      uint16
	offset         uint16
	answerID       uint16
	answerLength   uint8
}

// StaticLongestPrefixMatcher16 is a static (read-only) longest prefix matcher optimized for parsing phase.
//
// Immutable data structure optimized for maximum query performance during
// string parsing. Uses minimal perfect hash functions and inline storage to minimize
// memory indirection and cache misses.
type StaticLongestPrefixMatcher16 struct {
	shortDictionary map[prefixKey16]uint16
	longMPH         *minimalPerfectHash
	longInfo        []*LongMatchInfo
	longBuckets     []bucketEntry
}

// FindLongestMatch performs optimized longest match search for parsing phase.
//
// High-performance implementation leveraging static optimizations:
// - Hash function eliminates most collisions
// - Inline suffix storage reduces memory indirection
func (slpm *StaticLongestPrefixMatcher16) FindLongestMatch(data []byte) (uint16, int, bool) {
	// Phase 1: Long pattern search using hash function
	if len(data) >= 8 {
		suffixLen := len(data)
		if suffixLen > 16 {
			suffixLen = 16
		}
		suffixLen -= 8

		prefix := bytesToU64LE(data, 8)
		suffix := bytesToU64LE(data[8:], suffixLen)

		if id, length, ok := slpm.computeLongAnswer(prefix, suffix, suffixLen); ok {
			return id, length, true
		}
	}

	// Phase 2: Short pattern search
	prefix := bytesToU64LE(data, min(8, len(data)))
	for length := min(7, len(data)); length >= 1; length-- {
		prefix = prefix & masks[length]
		key := prefixKey16{prefix: prefix, length: uint8(length)}
		if id, ok := slpm.shortDictionary[key]; ok {
			return id, length, true
		}
	}

	return 0, 0, false
}

// computeLongAnswer performs optimized long pattern resolution with inline storage.
func (slpm *StaticLongestPrefixMatcher16) computeLongAnswer(prefix, suffix uint64, suffixLen int) (uint16, int, bool) {
	index := slpm.longMPH.hash(prefix)

	// Minimal perfect hash validation - ensure we found the right prefix
	if index >= len(slpm.longInfo) || slpm.longInfo[index] == nil || prefix != slpm.longInfo[index].prefix {
		return 0, 0, false
	}

	longInfo := slpm.longInfo[index]

	// Phase 1: Check inline suffixes
	inlineCount := nInlineSuffixes
	if int(longInfo.nSuffixes) < inlineCount {
		inlineCount = int(longInfo.nSuffixes)
	}

	for i := 0; i < inlineCount; i++ {
		inlineSuffix := longInfo.inlineSuffixes[i]
		inlineID := longInfo.inlineIDs[i]
		inlineLen := int(longInfo.inlineLengths[i])

		if isPrefix(suffix, inlineSuffix, suffixLen, inlineLen) {
			return inlineID, 8 + inlineLen, true
		}
	}

	// Phase 2: Check overflow bucket if inline storage insufficient
	if int(longInfo.nSuffixes) > nInlineSuffixes {
		start := int(longInfo.offset)
		end := start + int(longInfo.nSuffixes) - nInlineSuffixes

		for i := start; i < end; i++ {
			item := slpm.longBuckets[i]
			if isPrefix(suffix, item.suffix, suffixLen, int(item.length)) {
				return item.id, 8 + int(item.length), true
			}
		}
	}

	// Phase 3: Fallback to default prefix match
	return longInfo.answerID, int(longInfo.answerLength), true
}

// minimalPerfectHash implements a displacement-based minimal perfect hash function.
// This ensures no collisions and optimal space usage (table size ≈ number of keys).
type minimalPerfectHash struct {
	displacements []uint32
	tableSize     int
	seed1         uint64
	seed2         uint64
}

func newMinimalPerfectHash(keys []uint64) *minimalPerfectHash {
	if len(keys) == 0 {
		return &minimalPerfectHash{
			displacements: []uint32{},
			tableSize:     0,
			seed1:         0,
			seed2:         1,
		}
	}

	// Use 1.05x keys for minimal space overhead
	tableSize := (len(keys) * 105) / 100
	if tableSize < len(keys)+1 {
		tableSize = len(keys) + 1
	}

	seed1 := uint64(0x517cc1b727220a95)
	seed2 := uint64(0x8b51f5e3e9f0d2af)

	// Try to build perfect hash with different seeds if needed
	for attempts := 0; attempts < 100; attempts++ {
		displacements := make([]uint32, tableSize)
		occupied := make([]bool, tableSize)
		buckets := make(map[int][]uint64)

		// Group keys by their primary hash
		for _, key := range keys {
			h := hash1(key, seed1, tableSize)
			buckets[h] = append(buckets[h], key)
		}

		// Sort buckets by size (largest first) for better displacement finding
		type bucketInfo struct {
			index int
			keys  []uint64
		}
		sortedBuckets := make([]bucketInfo, 0, len(buckets))
		for idx, keys := range buckets {
			sortedBuckets = append(sortedBuckets, bucketInfo{idx, keys})
		}
		// Simple bubble sort (sufficient for small bucket counts)
		for i := 0; i < len(sortedBuckets); i++ {
			for j := i + 1; j < len(sortedBuckets); j++ {
				if len(sortedBuckets[j].keys) > len(sortedBuckets[i].keys) {
					sortedBuckets[i], sortedBuckets[j] = sortedBuckets[j], sortedBuckets[i]
				}
			}
		}

		success := true
		for _, bucket := range sortedBuckets {
			if len(bucket.keys) == 1 {
				// Single key bucket - try displacement 0 first
				key := bucket.keys[0]
				h := hash2(key, 0, seed2, tableSize)
				if !occupied[h] {
					displacements[bucket.index] = 0
					occupied[h] = true
					continue
				}
			}

			// Find a displacement that doesn't cause collisions
			found := false
			for d := uint32(0); d < uint32(tableSize*2); d++ {
				valid := true
				positions := make([]int, len(bucket.keys))

				for i, key := range bucket.keys {
					pos := hash2(key, d, seed2, tableSize)
					positions[i] = pos
					if occupied[pos] {
						valid = false
						break
					}
				}

				if valid {
					// Check for internal collisions within this bucket
					seen := make(map[int]bool)
					for _, pos := range positions {
						if seen[pos] {
							valid = false
							break
						}
						seen[pos] = true
					}

					if valid {
						displacements[bucket.index] = d
						for _, pos := range positions {
							occupied[pos] = true
						}
						found = true
						break
					}
				}
			}

			if !found {
				success = false
				break
			}
		}

		if success {
			return &minimalPerfectHash{
				displacements: displacements,
				tableSize:     tableSize,
				seed1:         seed1,
				seed2:         seed2,
			}
		}

		// Try different seeds
		seed1 = xxhash.Sum64(uint64ToBytes(seed1))
		seed2 = xxhash.Sum64(uint64ToBytes(seed2))
	}

	// Fallback to larger table if we can't find perfect hash
	tableSize = len(keys) * 2
	displacements := make([]uint32, tableSize)
	return &minimalPerfectHash{
		displacements: displacements,
		tableSize:     tableSize,
		seed1:         seed1,
		seed2:         seed2,
	}
}

func (mph *minimalPerfectHash) hash(key uint64) int {
	if mph.tableSize == 0 {
		return 0
	}
	h1 := hash1(key, mph.seed1, mph.tableSize)
	d := uint32(0)
	if h1 < len(mph.displacements) {
		d = mph.displacements[h1]
	}
	return hash2(key, d, mph.seed2, mph.tableSize)
}

// hash1 computes the primary hash (bucket assignment)
func hash1(key uint64, seed uint64, tableSize int) int {
	h := key ^ seed
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return int(h % uint64(tableSize))
}

// hash2 computes the secondary hash with displacement
func hash2(key uint64, displacement uint32, seed uint64, tableSize int) int {
	h := key ^ seed ^ uint64(displacement)
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return int(h % uint64(tableSize))
}

// isPrefix performs fast prefix checking using bitwise operations.
func isPrefix(text, prefix uint64, textSize, prefixSize int) bool {
	return prefixSize <= textSize && sharedPrefixSize(text, prefix) >= prefixSize
}

// sharedPrefixSize calculates bitwise shared prefix length.
func sharedPrefixSize(a, b uint64) int {
	return bits.TrailingZeros64(a^b) >> 3
}

// uint64ToBytes converts uint64 to byte slice (little-endian).
func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
