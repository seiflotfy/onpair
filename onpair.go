package onpair

import (
	"errors"
	"math"
)

const (
	singleByteTokens = 256   // singleByteTokens is the number of single-byte tokens (0-255)
	maxTokenID       = 65535 // maxTokenID is the maximum token ID (uint16 max)
	maxTokenID12Bit  = 4095  // maxTokenID12Bit is the maximum token ID representable in 12 bits.
	tokenBitWidth12  = uint8(12)
	tokenBitWidth16  = uint8(16)
)

// Config holds configuration for the compressor.
type Config struct {
	Threshold     uint16 // Minimum frequency to merge tokens (0 = dynamic)
	MaxTokenID    uint16 // Maximum token ID (0 = default, max 65535)
	MaxTokenLen   int    // Maximum token length (0 = unlimited)
	TokenBitWidth uint8  // Encoded token bit-width for archives (0 = default 16, supported: 12 or 16)
}

// Option is a functional option for configuring the compressor.
type Option func(*Config)

// WithThreshold sets a fixed threshold for merging tokens.
func WithThreshold(t uint16) Option {
	return func(c *Config) {
		c.Threshold = t
	}
}

// WithMaxTokenID sets an explicit token ID limit.
// Valid range is [255, 65535]. Values outside the range are clamped.
func WithMaxTokenID(maxID uint16) Option {
	return func(c *Config) {
		c.MaxTokenID = maxID
	}
}

// WithMaxTokenLength sets a maximum length for tokens.
// Previously known as "16-byte constraint" when set to 16.
func WithMaxTokenLength(n int) Option {
	return func(c *Config) {
		c.MaxTokenLen = n
	}
}

// WithTokenBitWidth configures the encoded token bit-width used in archive
// storage calculations and serialization. Supported values: 12 or 16.
// Any other value falls back to 16.
func WithTokenBitWidth(bits uint8) Option {
	return func(c *Config) {
		c.TokenBitWidth = bits
	}
}

// Encoder trains the dictionary and compresses data.
type Encoder struct {
	config Config
}

var (
	// ErrShortBuffer indicates the provided destination buffer is too small.
	ErrShortBuffer = errors.New("short buffer")
	// ErrUntrainedModel indicates Encode was called before a model was trained.
	ErrUntrainedModel = errors.New("model is not trained")
)

// NewEncoder creates a new encoder with the given options.
func NewEncoder(opts ...Option) *Encoder {
	var cfg Config
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Encoder{config: cfg}
}

// train populates the dictionary based on the input data.
// Maximum sample size for training (in bytes) - larger data uses sampling
const maxTrainingSampleBytes = 1024 * 1024 // 1MB

func (e *Encoder) train(data []byte, endPositions []int) (*Matcher, []byte, []uint32) {
	tokenBoundaries := make([]uint32, 0, singleByteTokens+4096)
	tokenBoundaries = append(tokenBoundaries, 0)
	dictionary := make([]byte, 0, 1024*1024)

	matcher := newMatcher(e.config.MaxTokenLen)

	// Initialize single-byte tokens
	for i := 0; i < singleByteTokens; i++ {
		token := []byte{byte(i)}
		_ = matcher.insert(token, uint16(i))
		dictionary = append(dictionary, token...)
		tokenBoundaries = append(tokenBoundaries, uint32(len(dictionary)))
	}

	numStrings := len(endPositions) - 1
	if numStrings == 0 {
		return matcher, dictionary, tokenBoundaries
	}

	// Create shuffled indices
	shuffledIndices := make([]int, numStrings)
	for i := range shuffledIndices {
		shuffledIndices[i] = i
	}

	// Simple deterministic shuffle (LCG)
	state := uint64(42)
	for i := len(shuffledIndices) - 1; i > 0; i-- {
		state = state*6364136223846793005 + 1442695040888963407
		j := int(state % uint64(i+1))
		shuffledIndices[i], shuffledIndices[j] = shuffledIndices[j], shuffledIndices[i]
	}

	// Sample if data is large - use first N shuffled strings up to 1MB
	sampleIndices := shuffledIndices
	sampleBytes := len(data)
	if len(data) > maxTrainingSampleBytes {
		sampleSize := 0
		for i, idx := range shuffledIndices {
			strLen := endPositions[idx+1] - endPositions[idx]
			sampleSize += strLen
			if sampleSize >= maxTrainingSampleBytes {
				sampleIndices = shuffledIndices[:i+1]
				sampleBytes = sampleSize
				break
			}
		}
	}

	// Determine threshold
	threshold := e.config.Threshold
	if threshold == 0 {
		sampleSizeMiB := float64(sampleBytes) / (1024.0 * 1024.0)
		threshold = uint16(math.Max(2.0, math.Log2(sampleSizeMiB)))
	}

	// Determine limits
	limitTokenID := resolveTokenLimit(e.config)

	// Build merged tokens from sample
	dictionary, tokenBoundaries = e.buildTokens(
		data, endPositions, sampleIndices,
		matcher, dictionary, tokenBoundaries,
		threshold, limitTokenID,
	)

	return matcher, dictionary, tokenBoundaries
}

func resolveTokenLimit(cfg Config) uint16 {
	limit := uint16(maxTokenID)
	if cfg.MaxTokenID != 0 {
		if cfg.MaxTokenID < uint16(singleByteTokens-1) {
			limit = uint16(singleByteTokens - 1)
		} else if cfg.MaxTokenID > maxTokenID {
			limit = maxTokenID
		} else {
			limit = cfg.MaxTokenID
		}
	}

	if resolveTokenBitWidth(cfg) == tokenBitWidth12 && limit > maxTokenID12Bit {
		limit = maxTokenID12Bit
	}
	return limit
}

func resolveTokenBitWidth(cfg Config) uint8 {
	switch cfg.TokenBitWidth {
	case tokenBitWidth12:
		return tokenBitWidth12
	case tokenBitWidth16:
		return tokenBitWidth16
	default:
		return tokenBitWidth16
	}
}

// buildTokens discovers and creates merged tokens from the training data.
// Uses online merging: when a pair reaches threshold frequency, merge immediately.
// Processes all segments in a single loop using state machine pattern.
func (e *Encoder) buildTokens(
	data []byte,
	endPositions []int,
	shuffledIndices []int,
	matcher *Matcher,
	dictionary []byte,
	tokenBoundaries []uint32,
	threshold uint16,
	limitTokenID uint16,
) ([]byte, []uint32) {
	if len(shuffledIndices) == 0 {
		return dictionary, tokenBoundaries
	}

	nextTokenID := uint16(singleByteTokens)
	frequency := make(map[uint32]uint16, 4096)
	maxTokenLen := e.config.MaxTokenLen

	// State machine variables
	segIdx := 0
	pos := 0
	end := 0
	prevTokenID := uint16(0)
	prevLength := 0
	hasPrev := false

	// Single loop - advances through all segments and tokens
	for {
		// State: need to start a new segment
		if !hasPrev {
			// Find next non-empty segment
			for segIdx < len(shuffledIndices) {
				index := shuffledIndices[segIdx]
				start := endPositions[index]
				end = endPositions[index+1]
				segIdx++

				if start >= end {
					continue
				}

				// Get first token of segment
				tokenID, length, ok := matcher.find(data[start:end])
				if !ok {
					continue
				}

				prevTokenID = tokenID
				prevLength = length
				pos = start + length
				hasPrev = true
				break
			}

			// No more segments
			if !hasPrev {
				break
			}
			continue
		}

		// State: at end of current segment
		if pos >= end {
			hasPrev = false
			continue
		}

		// State: have previous token, get current token
		currTokenID, currLength, ok := matcher.find(data[pos:end])
		if !ok {
			hasPrev = false
			continue
		}

		// Check max token length constraint
		if maxTokenLen > 0 && prevLength+currLength > maxTokenLen {
			prevTokenID = currTokenID
			prevLength = currLength
			pos += currLength
			continue
		}

		// Count pair and check for merge
		pair := uint32(prevTokenID)<<16 | uint32(currTokenID)
		frequency[pair]++

		if frequency[pair] >= threshold {
			if nextTokenID > limitTokenID {
				return dictionary, tokenBoundaries
			}
			mergedToken := data[pos-prevLength : pos+currLength]
			if !matcher.insert(mergedToken, nextTokenID) {
				delete(frequency, pair)
				prevTokenID = currTokenID
				prevLength = currLength
				pos += currLength
				continue
			}
			dictionary = append(dictionary, mergedToken...)
			tokenBoundaries = append(tokenBoundaries, uint32(len(dictionary)))

			delete(frequency, pair)
			prevTokenID = nextTokenID
			prevLength = len(mergedToken)

			if nextTokenID == limitTokenID {
				return dictionary, tokenBoundaries
			}
			nextTokenID++
		} else {
			prevTokenID = currTokenID
			prevLength = currLength
		}
		pos += currLength
	}

	return dictionary, tokenBoundaries
}

// compress parses the data using the trained matcher.
func (e *Encoder) compress(data []byte, endPositions []int, matcher *Matcher) ([]uint16, []int) {
	compressedData := make([]uint16, 0, len(data)/2)
	stringBoundaries := make([]int, 0, len(endPositions))
	stringBoundaries = append(stringBoundaries, 0)

	for i := 0; i < len(endPositions)-1; i++ {
		start := endPositions[i]
		end := endPositions[i+1]

		if start == end {
			stringBoundaries = append(stringBoundaries, len(compressedData))
			continue
		}

		pos := start
		for pos < end {
			tokenID, length, ok := matcher.find(data[pos:end])
			if !ok {
				// Should not happen if single byte tokens are present
				break
			}
			compressedData = append(compressedData, tokenID)
			pos += length
		}
		stringBoundaries = append(stringBoundaries, len(compressedData))
	}
	return compressedData, stringBoundaries
}

// Helper to flatten strings
func flattenStrings(strings []string) ([]byte, []int) {
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}

	data := make([]byte, 0, totalLen)
	endPositions := make([]int, 0, len(strings)+1)
	endPositions = append(endPositions, 0)

	for _, s := range strings {
		data = append(data, s...)
		endPositions = append(endPositions, len(data))
	}

	return data, endPositions
}
