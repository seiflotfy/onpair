package onpair

import (
	"bytes"
	"errors"
	"math"
	"sort"
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
	Threshold           uint16 // Minimum frequency to merge tokens (0 = dynamic)
	MaxTokenID          uint16 // Maximum token ID (0 = default, max 65535)
	MaxTokenLen         int    // Maximum token length (0 = unlimited)
	TokenBitWidth       uint8  // Encoded token bit-width for archives (0 = default 16, supported: 12 or 16)
	TrainingSampleBytes int    // Maximum sampled training bytes (0 = default 1 MiB)
	TemplateStratified  bool   // Enable template-based stratified sampling for training.
	TemplateMaxClusters int    // Maximum number of template clusters for stratified sampling.
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

// WithTrainingSampleBytes sets the maximum number of sampled bytes used to
// train the dictionary. Non-positive values fall back to the default.
func WithTrainingSampleBytes(n int) Option {
	return func(c *Config) {
		c.TrainingSampleBytes = n
	}
}

// WithTemplateStratifiedSampling enables template-based stratified sampling
// when selecting rows used for dictionary training.
// maxClusters <= 0 uses the default cluster cap.
func WithTemplateStratifiedSampling(maxClusters int) Option {
	return func(c *Config) {
		c.TemplateStratified = true
		c.TemplateMaxClusters = maxClusters
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

const (
	defaultTemplateMaxClusters = 2048
	defaultTemplateTokens      = 12
	templateOtherClusterKey    = "__template_other__"
)

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

	// Sample if data is large - use first N shuffled strings up to the configured sample size.
	sampleIndices := shuffledIndices
	sampleBytes := len(data)
	trainingSampleBytes := resolveTrainingSampleBytes(e.config)
	if len(data) > trainingSampleBytes {
		if e.config.TemplateStratified {
			maxClusters := resolveTemplateMaxClusters(e.config)
			sampleIndices, sampleBytes = stratifiedSampleIndicesByTemplateKey(
				data, endPositions, shuffledIndices, trainingSampleBytes, maxClusters,
			)
		} else {
			sampleIndices, sampleBytes = sampleIndicesByBytes(shuffledIndices, endPositions, trainingSampleBytes)
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

func resolveTrainingSampleBytes(cfg Config) int {
	if cfg.TrainingSampleBytes > 0 {
		return cfg.TrainingSampleBytes
	}
	return maxTrainingSampleBytes
}

func resolveTemplateMaxClusters(cfg Config) int {
	if cfg.TemplateMaxClusters > 0 {
		return cfg.TemplateMaxClusters
	}
	return defaultTemplateMaxClusters
}

func sampleIndicesByBytes(shuffledIndices []int, endPositions []int, sampleLimit int) ([]int, int) {
	if sampleLimit <= 0 || len(shuffledIndices) == 0 {
		return shuffledIndices, 0
	}

	sampleSize := 0
	for i, idx := range shuffledIndices {
		strLen := endPositions[idx+1] - endPositions[idx]
		sampleSize += strLen
		if sampleSize >= sampleLimit {
			return shuffledIndices[:i+1], sampleSize
		}
	}
	return shuffledIndices, sampleSize
}

func stratifiedSampleIndicesByTemplateKey(
	data []byte,
	endPositions []int,
	shuffledIndices []int,
	sampleBytesLimit int,
	maxClusters int,
) ([]int, int) {
	if sampleBytesLimit <= 0 || len(shuffledIndices) == 0 {
		return shuffledIndices, 0
	}

	clusterGroups := make(map[string][]int, 256)
	clusterOrder := make([]string, 0, 256)
	totalPoolBytes := 0

	for _, idx := range shuffledIndices {
		start := endPositions[idx]
		end := endPositions[idx+1]
		totalPoolBytes += end - start
		key := templateKeyFromLine(data[start:end], defaultTemplateTokens)

		if _, exists := clusterGroups[key]; !exists {
			if maxClusters > 0 && len(clusterGroups) >= maxClusters {
				key = templateOtherClusterKey
				if _, hasOther := clusterGroups[key]; !hasOther {
					clusterGroups[key] = nil
					clusterOrder = append(clusterOrder, key)
				}
			} else {
				clusterGroups[key] = nil
				clusterOrder = append(clusterOrder, key)
			}
		}
		clusterGroups[key] = append(clusterGroups[key], idx)
	}

	if len(clusterOrder) == 0 {
		return sampleIndicesByBytes(shuffledIndices, endPositions, sampleBytesLimit)
	}

	totalRows := len(shuffledIndices)
	avgLen := float64(totalPoolBytes) / float64(totalRows)
	targetRows := int(float64(sampleBytesLimit) / avgLen)
	if targetRows < 1 {
		targetRows = 1
	}
	if targetRows > totalRows {
		targetRows = totalRows
	}

	type clusterQuota struct {
		key       string
		quota     int
		remainder float64
	}
	quotas := make([]clusterQuota, 0, len(clusterOrder))
	allocated := 0
	for _, key := range clusterOrder {
		count := len(clusterGroups[key])
		exact := float64(count) * float64(targetRows) / float64(totalRows)
		quota := int(exact)
		quotas = append(quotas, clusterQuota{
			key:       key,
			quota:     quota,
			remainder: exact - float64(quota),
		})
		allocated += quota
	}
	if allocated < targetRows {
		sort.SliceStable(quotas, func(i, j int) bool {
			return quotas[i].remainder > quotas[j].remainder
		})
		remaining := targetRows - allocated
		for i := 0; remaining > 0; i++ {
			idx := i % len(quotas)
			quotas[idx].quota++
			remaining--
		}
	}

	clusterPos := make(map[string]int, len(quotas))
	sampleIndices := make([]int, 0, targetRows)
	sampleBytes := 0

	for _, q := range quotas {
		group := clusterGroups[q.key]
		n := q.quota
		if n > len(group) {
			n = len(group)
		}
		if n <= 0 {
			continue
		}
		for i := 0; i < n; i++ {
			idx := group[i]
			sampleIndices = append(sampleIndices, idx)
			sampleBytes += endPositions[idx+1] - endPositions[idx]
		}
		clusterPos[q.key] = n
		if sampleBytes >= sampleBytesLimit {
			return sampleIndices, sampleBytes
		}
	}

	// Top up in round-robin order when byte budget isn't reached due row-length variance.
	orderedKeys := make([]string, 0, len(quotas))
	for _, q := range quotas {
		orderedKeys = append(orderedKeys, q.key)
	}
	for sampleBytes < sampleBytesLimit {
		progressed := false
		for _, key := range orderedKeys {
			group := clusterGroups[key]
			pos := clusterPos[key]
			if pos >= len(group) {
				continue
			}

			idx := group[pos]
			clusterPos[key] = pos + 1
			sampleIndices = append(sampleIndices, idx)
			sampleBytes += endPositions[idx+1] - endPositions[idx]
			progressed = true

			if sampleBytes >= sampleBytesLimit {
				break
			}
		}
		if !progressed {
			break
		}
	}

	if len(sampleIndices) == 0 {
		return sampleIndicesByBytes(shuffledIndices, endPositions, sampleBytesLimit)
	}
	return sampleIndices, sampleBytes
}

func templateKeyFromLine(line []byte, maxTokens int) string {
	if len(line) == 0 {
		return ""
	}
	fields := bytes.Fields(line)
	if len(fields) == 0 {
		return ""
	}
	if maxTokens > 0 && len(fields) > maxTokens {
		fields = fields[:maxTokens]
	}

	key := make([]byte, 0, len(line))
	for i, field := range fields {
		if i > 0 {
			key = append(key, ' ')
		}
		key = appendTemplateNormalizedToken(key, field)
	}
	return string(key)
}

func appendTemplateNormalizedToken(dst []byte, token []byte) []byte {
	trimmed := trimTemplateToken(token)
	if len(trimmed) == 0 {
		return append(dst, "<*>"...)
	}
	if eq := bytes.IndexByte(trimmed, '='); eq > 0 && eq < len(trimmed)-1 {
		for _, b := range trimmed[:eq+1] {
			dst = append(dst, toLowerASCII(b))
		}
		return appendTemplateNormalizedValue(dst, trimmed[eq+1:])
	}
	return appendTemplateNormalizedValue(dst, trimmed)
}

func appendTemplateNormalizedValue(dst []byte, token []byte) []byte {
	if len(token) == 0 {
		return append(dst, "<*>"...)
	}
	if looksIPv4Token(token) {
		return append(dst, "<IP>"...)
	}
	if looksUUIDToken(token) {
		return append(dst, "<UUID>"...)
	}
	if looksHexToken(token) {
		return append(dst, "<HEX>"...)
	}
	if looksNumberLikeToken(token) {
		return append(dst, "<NUM>"...)
	}

	limit := len(token)
	if limit > 32 {
		limit = 32
	}
	for _, b := range token[:limit] {
		dst = append(dst, toLowerASCII(b))
	}
	return dst
}

func trimTemplateToken(token []byte) []byte {
	start, end := 0, len(token)
	for start < end && isTemplateTrimPunct(token[start]) {
		start++
	}
	for end > start && isTemplateTrimPunct(token[end-1]) {
		end--
	}
	return token[start:end]
}

func isTemplateTrimPunct(b byte) bool {
	switch b {
	case '[', ']', '(', ')', '{', '}', '<', '>', ',', ';', ':', '\'', '"':
		return true
	default:
		return false
	}
}

func toLowerASCII(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

func looksNumberLikeToken(token []byte) bool {
	digits := 0
	for _, b := range token {
		if b >= '0' && b <= '9' {
			digits++
			continue
		}
		switch b {
		case '.', ',', '-', '_', ':', '/', '+':
			continue
		default:
			return false
		}
	}
	if digits == 0 {
		return false
	}
	return digits*2 >= len(token)
}

func looksHexToken(token []byte) bool {
	if len(token) < 8 {
		return false
	}
	hexCount := 0
	for _, b := range token {
		if (b >= '0' && b <= '9') ||
			(b >= 'a' && b <= 'f') ||
			(b >= 'A' && b <= 'F') {
			hexCount++
			continue
		}
		if b == '-' {
			continue
		}
		return false
	}
	return hexCount >= 8
}

func looksUUIDToken(token []byte) bool {
	if len(token) != 36 {
		return false
	}
	for i, b := range token {
		switch i {
		case 8, 13, 18, 23:
			if b != '-' {
				return false
			}
		default:
			if !((b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')) {
				return false
			}
		}
	}
	return true
}

func looksIPv4Token(token []byte) bool {
	parts := 0
	value := 0
	digits := 0
	for i, b := range token {
		if b >= '0' && b <= '9' {
			value = value*10 + int(b-'0')
			digits++
			if value > 255 {
				return false
			}
			continue
		}

		if b != '.' {
			return false
		}
		if digits == 0 {
			return false
		}
		parts++
		if parts > 3 {
			return false
		}
		value = 0
		digits = 0

		if i == len(token)-1 {
			return false
		}
	}
	return parts == 3 && digits > 0
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
