package compressor

import (
	"encoding/binary"
	"io"
	"math"
	"math/rand"
	"unsafe"

	"github.com/axiomhq/axiom/pkg/kirby/x/onpair/lpm"
)

// Dictionary16 holds a trained OnPair dictionary with 16-byte token length constraint.
// A Dictionary16 is created via Train16 and can compress or decompress byte slices.
// After training, a Dictionary16 can be serialized with WriteTo and restored with ReadFrom.
type Dictionary16 struct {
	// Dictionary storage
	dictionary      []byte   // Raw token data
	tokenBoundaries []uint32 // Token end positions in dictionary
	lpmatcher       *lpm.StaticLongestPrefixMatcher16
}

// Train16 builds a compression dictionary from the provided training data with 16-byte token constraint.
//
// The endPositions should be a prefix sum array starting with 0.
// For example, if you have strings of lengths [3, 2, 4],
// then endPositions should be [0, 3, 5, 9].
//
// Example:
//
//	data := []byte("helloworld")
//	endPositions := []int{0, 5, 10} // "hello" and "world"
//	dict := Train16(data, endPositions)
func Train16(data []byte, endPositions []int) *Dictionary16 {
	dict := &Dictionary16{
		dictionary:      make([]byte, 0),
		tokenBoundaries: make([]uint32, 0, 1<<16),
	}

	dict.trainDictionary(data, endPositions)
	return dict
}

// Train16Strings is a convenience function that trains a dictionary from a slice of strings.
func Train16Strings(strings []string) *Dictionary16 {
	data, endPositions := flattenStrings(strings)
	return Train16(data, endPositions)
}

// trainDictionary is Phase 1: Dictionary population with 16-byte constraint.
func (dict *Dictionary16) trainDictionary(data []byte, endPositions []int) {
	dict.tokenBoundaries = append(dict.tokenBoundaries, 0)

	frequency := make(map[[2]uint16]uint16)
	lpmatcher := lpm.NewLongestPrefixMatcher16()
	nextTokenID := uint16(256)

	// Initialize the dictionary with single-byte tokens
	for i := 0; i < 256; i++ {
		token := []byte{byte(i)}
		lpmatcher.Insert(token, uint16(i))
		dict.dictionary = append(dict.dictionary, token...)
		dict.tokenBoundaries = append(dict.tokenBoundaries, uint32(len(dict.dictionary)))
	}

	// Shuffle entries
	shuffledIndices := make([]int, len(endPositions)-1)
	for i := range shuffledIndices {
		shuffledIndices[i] = i
	}
	rand.Shuffle(len(shuffledIndices), func(i, j int) {
		shuffledIndices[i], shuffledIndices[j] = shuffledIndices[j], shuffledIndices[i]
	})

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

			addedToken := false
			if matchLength+previousLength <= maxLength {
				// Update token frequency and possibly merge tokens
				pair := [2]uint16{previousTokenID, matchTokenID}
				frequency[pair]++

				if frequency[pair] >= threshold {
					// Check if we've reached the maximum token ID BEFORE inserting
					if nextTokenID == 65535 {
						break outer
					}

					mergedToken := data[pos-previousLength : pos+matchLength]
					addedToken = lpmatcher.Insert(mergedToken, nextTokenID)
					if addedToken {
						dict.dictionary = append(dict.dictionary, mergedToken...)
						dict.tokenBoundaries = append(dict.tokenBoundaries, uint32(len(dict.dictionary)))

						delete(frequency, pair)
						previousTokenID = nextTokenID
						previousLength = len(mergedToken)

						nextTokenID++
					}
				}
			}

			if !addedToken {
				previousTokenID = matchTokenID
				previousLength = matchLength
			}

			pos += matchLength
		}
	}

	dict.lpmatcher = lpmatcher.Finalize()
}

// Compress compresses a single byte slice.
//
// Example:
//
//	dict := Train16Strings([]string{"hello", "world"})
//	compressed := dict.Compress([]byte("hello"))
func (dict *Dictionary16) Compress(data []byte) []byte {
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
// This method uses unsafe memory operations for performance. All tokens are constrained
// to be at most 16 bytes long, and this method always copies exactly 16 bytes for each
// token regardless of the actual token length (for optimization).
//
// The buffer is automatically sized with extra padding to accommodate the 16-byte copies.
//
// Example:
//
//	dict := Train16Strings([]string{"hello", "world"})
//	compressed := dict.Compress([]byte("hello"))
//	result := dict.Decompress(nil, compressed) // "hello"
//
//	// Reuse scratch buffer for multiple decompressions
//	scratch := make([]byte, 0, 1024)
//	result1 := dict.Decompress(scratch, compressed1)
//	result2 := dict.Decompress(scratch, compressed2)
func (dict *Dictionary16) Decompress(scratch []byte, compressed []byte) []byte {
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
	bufferSize := requiredSize + maxLength

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

		// Cast to uintptr BEFORE adding to avoid uint16 overflow (65535+1=0)
		dictStart := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + uintptr(tokenID)*4))
		dictEnd := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + (uintptr(tokenID)+1)*4))

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

		// Always copy exactly 16 bytes
		*(*[maxLength]byte)(dst) = *(*[maxLength]byte)(src)

		size += length
	}

	return buffer[:size]
}

// WriteTo serializes the Dictionary16 to w.
// Layout:
// - 8 bytes: version
// - 4 bytes: number of tokens (uint32)
// - 4 bytes: dictionary size (uint32)
// - token boundaries (4 bytes each)
// - dictionary bytes
func (dict *Dictionary16) WriteTo(w io.Writer) (int64, error) {
	var n int64

	// Version (use version 2 to distinguish from unlimited version)
	version := uint64(2)
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

// ReadFrom deserializes a Dictionary16 from r.
func (dict *Dictionary16) ReadFrom(r io.Reader) (int64, error) {
	var n int64

	// Version
	var version uint64
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return n, err
	}
	n += 8

	if version != 2 {
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
func (dict *Dictionary16) rebuildLPM() {
	lpmatcher := lpm.NewLongestPrefixMatcher16()

	for tokenID := 0; tokenID < len(dict.tokenBoundaries)-1; tokenID++ {
		start := dict.tokenBoundaries[tokenID]
		end := dict.tokenBoundaries[tokenID+1]
		token := dict.dictionary[start:end]
		lpmatcher.Insert(token, uint16(tokenID))
	}

	dict.lpmatcher = lpmatcher.Finalize()
}

// SpaceUsed returns the total space (in bytes) used by the dictionary.
func (dict *Dictionary16) SpaceUsed() int {
	return len(dict.dictionary) + len(dict.tokenBoundaries)*4
}
