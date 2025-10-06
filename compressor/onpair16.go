package compressor

import (
	"math"
	"math/rand"
	"unsafe"

	"github.com/seif/onpair/lpm"
)

// Maximum token length constraint for optimization
const maxLength = 16

// OnPair16 is the OnPair compressor with 16-byte token length constraint.
type OnPair16 struct {
	// Compressed data storage
	compressedData   []uint16 // Sequence of token IDs
	stringBoundaries []int    // End positions for each string

	// Dictionary storage
	dictionary      []byte   // Raw token data
	tokenBoundaries []uint32 // Token end positions in dictionary
}

// New16 creates a new OnPair16 compressor instance.
func New16() *OnPair16 {
	return &OnPair16{
		compressedData:   make([]uint16, 0),
		stringBoundaries: make([]int, 0),
		dictionary:       make([]byte, 0),
		tokenBoundaries:  make([]uint32, 0),
	}
}

// WithCapacity16 creates a new OnPair16 compressor with capacity hints for better memory allocation.
func WithCapacity16(nStrings, nBytes int) *OnPair16 {
	return &OnPair16{
		compressedData:   make([]uint16, 0, nBytes),
		stringBoundaries: make([]int, 0, nStrings),
		dictionary:       make([]byte, 0, 1024*1024),
		tokenBoundaries:  make([]uint32, 0, 1<<16),
	}
}

// CompressStrings compresses a collection of strings.
//
// This is a convenience method that handles the flattening for you.
func (op *OnPair16) CompressStrings(strings []string) {
	data, endPositions := flattenStrings(strings)
	op.CompressBytes(data, endPositions)
}

// CompressBytes compresses pre-flattened byte data with end positions.
//
// The endPositions should be a prefix sum array starting with 0.
// For example, if you have strings of lengths [3, 2, 4],
// then endPositions should be [0, 3, 5, 9].
func (op *OnPair16) CompressBytes(data []byte, endPositions []int) {
	lpmatcher := op.trainDictionary(data, endPositions)
	staticLPM := lpmatcher.Finalize()
	op.parseData(data, endPositions, staticLPM)
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
func (op *OnPair16) trainDictionary(data []byte, endPositions []int) *lpm.LongestPrefixMatcher16 {
	op.tokenBoundaries = append(op.tokenBoundaries, 0)

	frequency := make(map[[2]uint16]uint16)
	lpmatcher := lpm.NewLongestPrefixMatcher16()
	nextTokenID := uint16(256)

	// Initialize the dictionary with single-byte tokens
	for i := 0; i < 256; i++ {
		token := []byte{byte(i)}
		lpmatcher.Insert(token, uint16(i))
		op.dictionary = append(op.dictionary, token...)
		op.tokenBoundaries = append(op.tokenBoundaries, uint32(len(op.dictionary)))
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
					mergedToken := data[pos-previousLength : pos+matchLength]
					addedToken = lpmatcher.Insert(mergedToken, nextTokenID)
					if addedToken {
						op.dictionary = append(op.dictionary, mergedToken...)
						op.tokenBoundaries = append(op.tokenBoundaries, uint32(len(op.dictionary)))

						delete(frequency, pair)
						previousTokenID = nextTokenID
						previousLength = len(mergedToken)

						if nextTokenID == 65535 {
							break outer
						}

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

	return lpmatcher
}

// parseData is Phase 2: String compression using learned dictionary.
//
// Compresses each string independently by greedily applying longest prefix matching
// with the constructed dictionary. Each string becomes a sequence of token IDs.
func (op *OnPair16) parseData(data []byte, endPositions []int, lpmatcher *lpm.StaticLongestPrefixMatcher16) {
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
// This method uses unsafe memory operations for performance. All tokens are constrained
// to be at most 16 bytes long, and this method always copies exactly 16 bytes for each
// token regardless of the actual token length (for optimization).
//
// The buffer must have sufficient space beyond the actual decompressed data to accommodate
// the full 16-byte copy for the last token, or undefined behavior will occur.
func (op *OnPair16) DecompressString(index int, buffer []byte) int {
	itemStart := op.stringBoundaries[index]
	itemEnd := op.stringBoundaries[index+1]
	dictPtr := unsafe.Pointer(&op.dictionary[0])
	endPositionsPtr := unsafe.Pointer(&op.tokenBoundaries[0])
	size := 0

	for _, tokenID := range op.compressedData[itemStart:itemEnd] {
		// Bounds check: ensure tokenID+1 is within array bounds
		if int(tokenID)+1 >= len(op.tokenBoundaries) {
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
		if length < 0 || dictEnd > uint32(len(op.dictionary)) {
			continue
		}

		src := unsafe.Pointer(uintptr(dictPtr) + uintptr(dictStart))
		dst := unsafe.Pointer(&buffer[size])

		// Always copy exactly 16 bytes
		*(*[maxLength]byte)(dst) = *(*[maxLength]byte)(src)

		size += length
	}

	return size
}

// DecompressAll decompresses all strings into a single buffer.
//
// Safety Warning:
// This method uses unsafe memory operations for performance. All tokens are constrained
// to be at most 16 bytes long, and this method always copies exactly 16 bytes for each
// token regardless of the actual token length (for optimization).
//
// The buffer must have sufficient space beyond the actual decompressed data to accommodate
// the full 16-byte copy for the last token, or undefined behavior will occur.
func (op *OnPair16) DecompressAll(buffer []byte) int {
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

		// Cast to uintptr BEFORE adding to avoid uint16 overflow (65535+1=0)
		dictStart := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + uintptr(tokenID)*4))
		dictEnd := *(*uint32)(unsafe.Pointer(uintptr(endPositionsPtr) + (uintptr(tokenID)+1)*4))

		// Check for boundary corruption
		if dictEnd < dictStart {
			continue
		}
		length := int(dictEnd - dictStart)

		// Sanity check on length
		if length < 0 || dictEnd > uint32(len(op.dictionary)) {
			continue
		}

		src := unsafe.Pointer(uintptr(dictPtr) + uintptr(dictStart))
		dst := unsafe.Pointer(&buffer[size])

		// Always copy exactly 16 bytes
		*(*[maxLength]byte)(dst) = *(*[maxLength]byte)(src)

		size += length
	}

	return size
}

// SpaceUsed returns the total space (in bytes) used by the compressed data.
func (op *OnPair16) SpaceUsed() int {
	return len(op.compressedData)*2 +
		len(op.dictionary) +
		len(op.tokenBoundaries)*4
}

// ShrinkToFit shrinks all internal buffers to fit their current contents.
func (op *OnPair16) ShrinkToFit() {
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
func (op *OnPair16) GetDictionary() []byte {
	return op.dictionary
}

// GetTokenBoundaries returns the token boundaries for inspection
func (op *OnPair16) GetTokenBoundaries() []uint32 {
	return op.tokenBoundaries
}

// GetCompressedData returns the compressed data for inspection
func (op *OnPair16) GetCompressedData() []uint16 {
	return op.compressedData
}

// GetStringBoundaries returns the string boundaries for inspection
func (op *OnPair16) GetStringBoundaries() []int {
	return op.stringBoundaries
}
