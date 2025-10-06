// Package compressor provides OnPair dictionary-based compression.
package compressor

import (
	"encoding/binary"
	"io"
	"math"
	"unsafe"

	"github.com/axiomhq/axiom/pkg/kirby/x/onpair/lpm"
)

// Dictionary holds a trained OnPair dictionary for compression and decompression.
// A Dictionary is created via Train and can compress or decompress byte slices.
// After training, a Dictionary can be serialized with WriteTo and restored with ReadFrom.
type Dictionary struct {
	// Dictionary storage
	dictionary      []byte   // Raw token data
	tokenBoundaries []uint32 // Token end positions in dictionary
	lpmatcher       *lpm.LongestPrefixMatcher
}

// Train builds a compression dictionary from the provided training data.
//
// The endPositions should be a prefix sum array starting with 0.
// For example, if you have strings of lengths [3, 2, 4],
// then endPositions should be [0, 3, 5, 9].
//
// Example:
//
//	data := []byte("helloworld")
//	endPositions := []int{0, 5, 10} // "hello" and "world"
//	dict := Train(data, endPositions)
func Train(data []byte, endPositions []int) *Dictionary {
	dict := &Dictionary{
		dictionary:      make([]byte, 0),
		tokenBoundaries: make([]uint32, 0, 1<<16),
	}

	dict.trainDictionary(data, endPositions)
	return dict
}

// TrainStrings is a convenience function that trains a dictionary from a slice of strings.
func TrainStrings(strings []string) *Dictionary {
	data, endPositions := flattenStrings(strings)
	return Train(data, endPositions)
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
func (dict *Dictionary) trainDictionary(data []byte, endPositions []int) {
	dict.tokenBoundaries = append(dict.tokenBoundaries, 0)

	frequency := make(map[[2]uint16]uint16)
	lpmatcher := lpm.NewLongestPrefixMatcher()
	nextTokenID := uint16(256)

	// Initialize the dictionary with single-byte tokens
	for i := 0; i < 256; i++ {
		token := []byte{byte(i)}
		lpmatcher.Insert(token, uint16(i))
		dict.dictionary = append(dict.dictionary, token...)
		dict.tokenBoundaries = append(dict.tokenBoundaries, uint32(len(dict.dictionary)))
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
				// Check if we've reached the maximum token ID BEFORE inserting
				if nextTokenID == 65535 {
					break outer
				}

				mergedToken := data[pos-previousLength : pos+matchLength]
				lpmatcher.Insert(mergedToken, nextTokenID)
				dict.dictionary = append(dict.dictionary, mergedToken...)
				dict.tokenBoundaries = append(dict.tokenBoundaries, uint32(len(dict.dictionary)))

				delete(frequency, pair)
				previousTokenID = nextTokenID
				previousLength = len(mergedToken)

				nextTokenID++
			} else {
				previousTokenID = matchTokenID
				previousLength = matchLength
			}

			pos += matchLength
		}
	}

	dict.lpmatcher = lpmatcher
}

// Compress compresses a single byte slice.
//
// Example:
//
//	dict := TrainStrings([]string{"hello", "world"})
//	compressed := dict.Compress([]byte("hello"))
func (dict *Dictionary) Compress(data []byte) []byte {
	if len(data) == 0 {
		return []byte{}
	}

	tokens := make([]uint16, 0, len(data))
	pos := 0
	end := len(data)

	for pos < end {
		tokenID, length, ok := dict.lpmatcher.FindLongestMatch(data[pos:end])
		if !ok {
			break
		}
		tokens = append(tokens, tokenID)
		pos += length
	}

	// Encode tokens as bytes (little-endian uint16s)
	compressed := make([]byte, len(tokens)*2)
	for i, token := range tokens {
		compressed[i*2] = byte(token)
		compressed[i*2+1] = byte(token >> 8)
	}

	return compressed
}

// Decompress decompresses compressed data, optionally reusing scratch buffer.
//
// The scratch buffer can be nil or undersized; it will be grown as needed.
// Returns the decompressed data (may have different backing array than scratch).
//
// Safety Warning:
// This method uses unsafe memory operations for performance. For each token, it initially
// copies 16 bytes regardless of the actual token length (for optimization), then copies
// any remaining bytes if the token is longer than 16 bytes.
//
// The buffer is automatically sized with extra padding to accommodate the 16-byte copies.
//
// Example:
//
//	dict := TrainStrings([]string{"hello", "world"})
//	compressed := dict.Compress([]byte("hello"))
//	result := dict.Decompress(nil, compressed) // "hello"
//
//	// Reuse scratch buffer for multiple decompressions
//	scratch := make([]byte, 0, 1024)
//	result1 := dict.Decompress(scratch, compressed1)
//	result2 := dict.Decompress(scratch, compressed2)
func (dict *Dictionary) Decompress(scratch []byte, compressed []byte) []byte {
	if len(dict.dictionary) == 0 || len(compressed) == 0 {
		return []byte{}
	}

	// Decode bytes to tokens (little-endian uint16s)
	numTokens := len(compressed) / 2
	if len(compressed)%2 != 0 {
		return []byte{} // Invalid compressed data
	}

	// Calculate required size and allocate buffer with padding
	requiredSize := 0
	for i := 0; i < numTokens; i++ {
		tokenID := uint16(compressed[i*2]) | (uint16(compressed[i*2+1]) << 8)
		if int(tokenID)+1 >= len(dict.tokenBoundaries) {
			continue
		}
		dictStart := dict.tokenBoundaries[tokenID]
		dictEnd := dict.tokenBoundaries[tokenID+1]
		requiredSize += int(dictEnd - dictStart)
	}

	// Add padding for safe 16-byte copies
	bufferSize := requiredSize + fastCopySize

	var buffer []byte
	if cap(scratch) >= bufferSize {
		buffer = scratch[:bufferSize]
	} else {
		buffer = make([]byte, bufferSize)
	}

	dictPtr := unsafe.Pointer(&dict.dictionary[0])
	endPositionsPtr := unsafe.Pointer(&dict.tokenBoundaries[0])
	size := 0

	for i := 0; i < numTokens; i++ {
		tokenID := uint16(compressed[i*2]) | (uint16(compressed[i*2+1]) << 8)

		// Bounds check: ensure tokenID+1 is within array bounds
		if int(tokenID)+1 >= len(dict.tokenBoundaries) {
			continue
		}

		dictStart := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + uintptr(tokenID)*4))
		dictEnd := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + uintptr(tokenID+1)*4))

		// Check for boundary corruption
		if dictEnd < dictStart {
			continue
		}
		length := int(dictEnd - dictStart)

		// Sanity check on length
		if length < 0 || dictEnd > uint32(len(dict.dictionary)) {
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

	return buffer[:size]
}

// WriteTo serializes the Dictionary to w.
// Layout:
// - 8 bytes: version
// - 4 bytes: number of tokens (uint32)
// - 4 bytes: dictionary size (uint32)
// - token boundaries (4 bytes each)
// - dictionary bytes
func (dict *Dictionary) WriteTo(w io.Writer) (int64, error) {
	var n int64

	// Version
	version := uint64(1)
	if err := binary.Write(w, binary.LittleEndian, version); err != nil {
		return n, err
	}
	n += 8

	// Number of tokens
	numTokens := uint32(len(dict.tokenBoundaries))
	if err := binary.Write(w, binary.LittleEndian, numTokens); err != nil {
		return n, err
	}
	n += 4

	// Dictionary size
	dictSize := uint32(len(dict.dictionary))
	if err := binary.Write(w, binary.LittleEndian, dictSize); err != nil {
		return n, err
	}
	n += 4

	// Token boundaries
	for _, boundary := range dict.tokenBoundaries {
		if err := binary.Write(w, binary.LittleEndian, boundary); err != nil {
			return n, err
		}
		n += 4
	}

	// Dictionary bytes
	written, err := w.Write(dict.dictionary)
	n += int64(written)
	if err != nil {
		return n, err
	}

	return n, nil
}

// ReadFrom deserializes a Dictionary from r.
func (dict *Dictionary) ReadFrom(r io.Reader) (int64, error) {
	var n int64

	// Version
	var version uint64
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return n, err
	}
	n += 8

	if version != 1 {
		return n, io.ErrUnexpectedEOF
	}

	// Number of tokens
	var numTokens uint32
	if err := binary.Read(r, binary.LittleEndian, &numTokens); err != nil {
		return n, err
	}
	n += 4

	// Dictionary size
	var dictSize uint32
	if err := binary.Read(r, binary.LittleEndian, &dictSize); err != nil {
		return n, err
	}
	n += 4

	// Token boundaries
	dict.tokenBoundaries = make([]uint32, numTokens)
	for i := range dict.tokenBoundaries {
		if err := binary.Read(r, binary.LittleEndian, &dict.tokenBoundaries[i]); err != nil {
			return n, err
		}
		n += 4
	}

	// Dictionary bytes
	dict.dictionary = make([]byte, dictSize)
	read, err := io.ReadFull(r, dict.dictionary)
	n += int64(read)
	if err != nil {
		return n, err
	}

	// Rebuild LPM matcher
	dict.rebuildLPM()

	return n, nil
}

// rebuildLPM reconstructs the LPM matcher from the dictionary data.
func (dict *Dictionary) rebuildLPM() {
	dict.lpmatcher = lpm.NewLongestPrefixMatcher()

	for tokenID := 0; tokenID < len(dict.tokenBoundaries)-1; tokenID++ {
		start := dict.tokenBoundaries[tokenID]
		end := dict.tokenBoundaries[tokenID+1]
		token := dict.dictionary[start:end]
		dict.lpmatcher.Insert(token, uint16(tokenID))
	}
}

// SpaceUsed returns the total space (in bytes) used by the dictionary.
func (dict *Dictionary) SpaceUsed() int {
	return len(dict.dictionary) + len(dict.tokenBoundaries)*4
}
