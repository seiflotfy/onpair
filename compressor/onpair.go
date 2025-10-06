// Package compressor provides OnPair compression implementations.
package compressor

import (
	"math"
	"unsafe"

	"github.com/seif/onpair/lpm"
)

const fastCopySize = 16

// OnPair is the main OnPair compressor with unlimited token length support.
type OnPair struct {
	// Compressed data storage
	compressedData   []uint16 // Sequence of token IDs
	stringBoundaries []int    // End positions for each string

	// Dictionary storage
	dictionary      []byte   // Raw token data
	tokenBoundaries []uint32 // Token end positions in dictionary
}

// New creates a new OnPair compressor instance.
func New() *OnPair {
	return &OnPair{
		compressedData:   make([]uint16, 0),
		stringBoundaries: make([]int, 0),
		dictionary:       make([]byte, 0),
		tokenBoundaries:  make([]uint32, 0),
	}
}

// WithCapacity creates a new OnPair compressor with capacity hints for better memory allocation.
func WithCapacity(nStrings, nBytes int) *OnPair {
	return &OnPair{
		compressedData:   make([]uint16, 0, nBytes),
		stringBoundaries: make([]int, 0, nStrings),
		dictionary:       make([]byte, 0, 1024*1024),
		tokenBoundaries:  make([]uint32, 0, 1<<16),
	}
}

// CompressStrings compresses a collection of strings.
//
// This is a convenience method that handles the flattening for you.
func (op *OnPair) CompressStrings(strings []string) {
	data, endPositions := flattenStrings(strings)
	op.CompressBytes(data, endPositions)
}

// CompressBytes compresses pre-flattened byte data with end positions.
//
// The endPositions should be a prefix sum array starting with 0.
// For example, if you have strings of lengths [3, 2, 4],
// then endPositions should be [0, 3, 5, 9].
func (op *OnPair) CompressBytes(data []byte, endPositions []int) {
	lpmatcher := op.trainDictionary(data, endPositions)
	op.parseData(data, endPositions, lpmatcher)
}

// trainDictionary is Phase 1: Dictionary population.
//
// Uses longest prefix matching to parse training data and identify frequent
// adjacent token pairs.
//
// Algorithm:
// 1. Initialize 256 single-byte tokens
// 2. Parse shuffled training data with longest prefix matching
// 3. Track adjacent token pair frequencies
// 4. Merge frequent pairs into new tokens until dictionary full (65,536 tokens)
func (op *OnPair) trainDictionary(data []byte, endPositions []int) *lpm.LongestPrefixMatcher {
	op.tokenBoundaries = append(op.tokenBoundaries, 0)

	frequency := make(map[[2]uint16]uint16)
	lpmatcher := lpm.NewLongestPrefixMatcher()
	nextTokenID := uint16(256)

	// Initialize the dictionary with single-byte tokens
	for i := 0; i < 256; i++ {
		token := []byte{byte(i)}
		lpmatcher.Insert(token, uint16(i))
		op.dictionary = append(op.dictionary, token...)
		op.tokenBoundaries = append(op.tokenBoundaries, uint32(len(op.dictionary)))
	}

	// Shuffle entries with cross-platform deterministic PRNG
	shuffledIndices := make([]int, len(endPositions)-1)
	for i := range shuffledIndices {
		shuffledIndices[i] = i
	}
	rng := NewSimplePRNG(42)
	rng.Shuffle(shuffledIndices)

	// Set the threshold for merging tokens
	dataSizeMiB := float64(len(data)) / (1024.0 * 1024.0)
	threshold := uint16(math.Max(2.0, math.Log2(dataSizeMiB)))

	// Iterate over entries
outer:
	for _, index := range shuffledIndices {
		start := endPositions[index]
		end := endPositions[index+1]

		if start == end {
			continue
		}

		matchTokenID, matchLength, ok := lpmatcher.FindLongestMatch(data[start:end])
		if !ok {
			continue
		}

		previousTokenID := matchTokenID
		previousLength := matchLength
		pos := start + previousLength

		for pos < end {
			// Find the longest match
			matchTokenID, matchLength, ok := lpmatcher.FindLongestMatch(data[pos:end])
			if !ok {
				break
			}

			// Update token frequency and possibly merge tokens
			pair := [2]uint16{previousTokenID, matchTokenID}
			frequency[pair]++

			if frequency[pair] >= threshold {
				mergedToken := data[pos-previousLength : pos+matchLength]
				lpmatcher.Insert(mergedToken, nextTokenID)
				op.dictionary = append(op.dictionary, mergedToken...)
				op.tokenBoundaries = append(op.tokenBoundaries, uint32(len(op.dictionary)))

				delete(frequency, pair)
				previousTokenID = nextTokenID
				previousLength = len(mergedToken)

				if nextTokenID == 65535 {
					break outer
				}

				nextTokenID++
			} else {
				previousTokenID = matchTokenID
				previousLength = matchLength
			}

			pos += matchLength
		}
	}

	return lpmatcher
}

// parseData is Phase 2: String compression using learned dictionary.
//
// Compresses each string independently by greedily applying longest prefix matching
// with the constructed dictionary. Each string becomes a sequence of token IDs.
func (op *OnPair) parseData(data []byte, endPositions []int, lpmatcher *lpm.LongestPrefixMatcher) {
	op.stringBoundaries = append(op.stringBoundaries, 0)

	for i := 0; i < len(endPositions)-1; i++ {
		start := endPositions[i]
		end := endPositions[i+1]

		if start == end {
			op.stringBoundaries = append(op.stringBoundaries, len(op.compressedData))
			continue
		}

		pos := start
		for pos < end {
			// Find the longest match
			tokenID, length, ok := lpmatcher.FindLongestMatch(data[pos:end])
			if !ok {
				break
			}
			op.compressedData = append(op.compressedData, tokenID)
			pos += length
		}

		op.stringBoundaries = append(op.stringBoundaries, len(op.compressedData))
	}
}

// DecompressString decompresses a specific string by index.
//
// Safety Warning:
// This method uses unsafe memory operations for performance. For each token, it initially
// copies 16 bytes regardless of the actual token length (for optimization), then copies
// any remaining bytes if the token is longer than 16 bytes.
//
// The buffer must have sufficient space beyond the actual decompressed data to accommodate
// the initial 16-byte copy for the last token, or undefined behavior will occur.
func (op *OnPair) DecompressString(index int, buffer []byte) int {
	stringStart := op.stringBoundaries[index]
	stringEnd := op.stringBoundaries[index+1]
	dictPtr := unsafe.Pointer(&op.dictionary[0])
	endPositionsPtr := unsafe.Pointer(&op.tokenBoundaries[0])
	size := 0

	for _, tokenID := range op.compressedData[stringStart:stringEnd] {
		// Bounds check: ensure tokenID+1 is within array bounds
		if int(tokenID)+1 >= len(op.tokenBoundaries) {
			// Skip invalid token IDs (shouldn't happen but defensive)
			continue
		}

		dictStart := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + uintptr(tokenID)*4))
		dictEnd := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + uintptr(tokenID+1)*4))
		length := int(dictEnd - dictStart)

		// Sanity check on length
		if length < 0 || dictEnd > uint32(len(op.dictionary)) {
			continue
		}

		src := unsafe.Pointer(uintptr(dictPtr) + uintptr(dictStart))
		dst := unsafe.Pointer(&buffer[size])

		// Fast copy 16 bytes regardless of actual length
		*(*[fastCopySize]byte)(dst) = *(*[fastCopySize]byte)(src)

		if length > fastCopySize {
			// Copy remaining bytes
			src = unsafe.Pointer(uintptr(src) + fastCopySize)
			dst = unsafe.Pointer(uintptr(dst) + fastCopySize)
			remaining := length - fastCopySize
			copy((*[1 << 30]byte)(dst)[:remaining:remaining], (*[1 << 30]byte)(src)[:remaining:remaining])
		}

		size += length
	}

	return size
}

// DecompressAll decompresses all strings into a single buffer.
//
// Safety Warning:
// This method uses unsafe memory operations for performance. For each token, it initially
// copies 16 bytes regardless of the actual token length (for optimization), then copies
// any remaining bytes if the token is longer than 16 bytes.
//
// The buffer must have sufficient space beyond the actual decompressed data to accommodate
// the initial 16-byte copy for the last token, or undefined behavior will occur.
func (op *OnPair) DecompressAll(buffer []byte) int {
	if len(op.dictionary) == 0 {
		return 0
	}

	dictPtr := unsafe.Pointer(&op.dictionary[0])
	endPositionsPtr := unsafe.Pointer(&op.tokenBoundaries[0])
	size := 0

	for _, tokenID := range op.compressedData {
		// Bounds check: ensure tokenID+1 is within array bounds
		if int(tokenID)+1 >= len(op.tokenBoundaries) {
			continue
		}

		dictStart := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + uintptr(tokenID)*4))
		dictEnd := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + uintptr(tokenID+1)*4))
		length := int(dictEnd - dictStart)

		// Sanity check on length
		if length < 0 || dictEnd > uint32(len(op.dictionary)) {
			continue
		}

		src := unsafe.Pointer(uintptr(dictPtr) + uintptr(dictStart))
		dst := unsafe.Pointer(&buffer[size])

		// Fast copy 16 bytes regardless of actual length
		*(*[fastCopySize]byte)(dst) = *(*[fastCopySize]byte)(src)

		if length > fastCopySize {
			// Copy remaining bytes
			src = unsafe.Pointer(uintptr(src) + fastCopySize)
			dst = unsafe.Pointer(uintptr(dst) + fastCopySize)
			remaining := length - fastCopySize
			copy((*[1 << 30]byte)(dst)[:remaining:remaining], (*[1 << 30]byte)(src)[:remaining:remaining])
		}

		size += length
	}

	return size
}

// SpaceUsed returns the total space (in bytes) used by the compressed data.
func (op *OnPair) SpaceUsed() int {
	return len(op.compressedData)*2 +
		len(op.dictionary) +
		len(op.tokenBoundaries)*4
}

// ShrinkToFit shrinks all internal buffers to fit their current contents.
func (op *OnPair) ShrinkToFit() {
	// Go doesn't have direct shrink_to_fit, but we can reallocate
	newCompressed := make([]uint16, len(op.compressedData))
	copy(newCompressed, op.compressedData)
	op.compressedData = newCompressed

	newBoundaries := make([]int, len(op.stringBoundaries))
	copy(newBoundaries, op.stringBoundaries)
	op.stringBoundaries = newBoundaries

	newDict := make([]byte, len(op.dictionary))
	copy(newDict, op.dictionary)
	op.dictionary = newDict

	newTokenBoundaries := make([]uint32, len(op.tokenBoundaries))
	copy(newTokenBoundaries, op.tokenBoundaries)
	op.tokenBoundaries = newTokenBoundaries
}

// GetDictionary returns the dictionary for inspection
func (op *OnPair) GetDictionary() []byte {
	return op.dictionary
}

// GetTokenBoundaries returns the token boundaries for inspection
func (op *OnPair) GetTokenBoundaries() []uint32 {
	return op.tokenBoundaries
}

// GetCompressedData returns the compressed data for inspection
func (op *OnPair) GetCompressedData() []uint16 {
	return op.compressedData
}

// GetStringBoundaries returns the string boundaries for inspection
func (op *OnPair) GetStringBoundaries() []int {
	return op.stringBoundaries
}

// flattenStrings flattens a collection of strings into a single byte array with boundary positions.
//
// Returns a tuple of (flattened_data, end_positions) where end_positions is a
// prefix sum array starting with 0.
func flattenStrings(strings []string) ([]byte, []int) {
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}

	data := make([]byte, 0, totalLen)
	endPositions := make([]int, 0, len(strings)+1)
	endPositions = append(endPositions, 0)

	for _, s := range strings {
		data = append(data, []byte(s)...)
		endPositions = append(endPositions, len(data))
	}

	return data, endPositions
}
