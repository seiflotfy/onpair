package onpair

import (
	"bytes"
	"cmp"
	"errors"
	"math/bits"
	"slices"
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

// train builds the dictionary in creation order, then sorts it into strict
// bytewise-lexicographic order — the property compressed-domain search binary-
// searches over. The matcher keeps creation-order ids; remap translates them
// to sorted ids at compress time.
func (e *Encoder) train(data []byte, endPositions []int) (*matcher, []byte, []uint32, []uint16) {
	matcher, dictionary, tokenBoundaries := e.trainUnsorted(data, endPositions)
	dictionary, tokenBoundaries, remap := sortDictionary(dictionary, tokenBoundaries)
	return matcher, dictionary, tokenBoundaries, remap
}

// sortDictionary reorders tokens into strict bytewise-lexicographic order,
// returning the sorted dictionary, its boundaries, and the remap from
// creation-order token id to sorted id. Byte-equal duplicates — a merge can
// recreate an existing token's bytes through a different pair split — collapse
// into one token, with every duplicate id remapped to it.
func sortDictionary(dictionary []byte, tokenBoundaries []uint32) ([]byte, []uint32, []uint16) {
	n := len(tokenBoundaries) - 1
	token := func(i int) []byte { return dictionary[tokenBoundaries[i]:tokenBoundaries[i+1]] }
	order := make([]int, n)
	// Big-endian first-8-bytes sort keys: most comparisons resolve on one
	// integer compare; byte-equal 8-byte prefixes (including zero-padding
	// ties) fall back to the full compare.
	keys := make([]uint64, n)
	for i := range order {
		order[i] = i
		tok := token(i)
		keys[i] = bits.ReverseBytes64(bytesToU64LE(tok, len(tok)))
	}
	slices.SortFunc(order, func(a, b int) int {
		if keys[a] != keys[b] {
			return cmp.Compare(keys[a], keys[b])
		}
		return bytes.Compare(token(a), token(b))
	})

	sortedDict := make([]byte, 0, len(dictionary))
	sortedBounds := make([]uint32, 1, n+1)
	remap := make([]uint16, n)
	var prev []byte
	for _, oldID := range order {
		tok := token(oldID)
		if len(sortedBounds) == 1 || !bytes.Equal(prev, tok) {
			sortedDict = append(sortedDict, tok...)
			sortedBounds = append(sortedBounds, uint32(len(sortedDict)))
			prev = sortedDict[sortedBounds[len(sortedBounds)-2]:]
		}
		remap[oldID] = uint16(len(sortedBounds) - 2)
	}
	return sortedDict, sortedBounds, remap
}

func (e *Encoder) trainUnsorted(data []byte, endPositions []int) (*matcher, []byte, []uint32) {
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

	// Build merged tokens from sample. A zero threshold selects the adaptive
	// controller inside buildTokens.
	limitTokenID := resolveTokenLimit(e.config)
	dictionary, tokenBoundaries = e.buildTokens(
		data, endPositions, sampleIndices, sampleBytes,
		matcher, dictionary, tokenBoundaries,
		e.config.Threshold, limitTokenID,
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

	// Group rows by template key, keeping shuffled order within each cluster
	// and first-seen order across clusters. Overflow clusters collapse into
	// one catch-all bucket.
	clusterGroups := make(map[string][]int, 256)
	clusterOrder := make([]string, 0, 256)
	for _, idx := range shuffledIndices {
		key := templateKeyFromLine(data[endPositions[idx]:endPositions[idx+1]], defaultTemplateTokens)
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

	// Round-robin one row per cluster per pass until the byte budget is met:
	// rare templates are represented before dominant ones can exhaust the
	// budget. The first pass always takes at least one row, so the sample is
	// never empty.
	sampleIndices := make([]int, 0, len(shuffledIndices))
	sampleBytes := 0
	for pos := 0; sampleBytes < sampleBytesLimit; pos++ {
		progressed := false
		for _, key := range clusterOrder {
			group := clusterGroups[key]
			if pos >= len(group) {
				continue
			}
			idx := group[pos]
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
// Uses online merging: when an adjacent token pair reaches the threshold
// frequency, the pair is merged into a new token immediately and becomes the
// left side of the next pair. A zero threshold enables the adaptive
// thresholdController; any other value is a fixed threshold.
func (e *Encoder) buildTokens(
	data []byte,
	endPositions []int,
	sampleIndices []int,
	sampleBytes int,
	matcher *matcher,
	dictionary []byte,
	tokenBoundaries []uint32,
	threshold uint16,
	limitTokenID uint16,
) ([]byte, []uint32) {
	if len(sampleIndices) == 0 {
		return dictionary, tokenBoundaries
	}

	// Pre-size the pair-frequency counter from the sampled byte count.
	// Roughly 1 pair per ~3 bytes of sampled data (empirical), capped.
	freqHint := sampleBytes / 3
	if freqHint < 4096 {
		freqHint = 4096
	}
	if freqHint > 1<<20 {
		freqHint = 1 << 20
	}
	frequency := newPairCounter(freqHint)
	maxTokenLen := e.config.MaxTokenLen
	nextTokenID := uint16(singleByteTokens)

	var ctrl *thresholdController
	if threshold == 0 {
		ctrl = newThresholdController(int(limitTokenID)-(singleByteTokens-1), sampleBytes)
		threshold = ctrl.threshold
	}

	for _, index := range sampleIndices {
		start := endPositions[index]
		end := endPositions[index+1]
		if start >= end {
			continue
		}
		seg := data[start:end]

		prevTokenID, prevLength, ok := matcher.find(seg)
		if !ok {
			continue
		}
		if ctrl != nil {
			ctrl.onBytesScanned(prevLength)
		}

		for pos := prevLength; pos < len(seg); {
			currTokenID, currLength, ok := matcher.find(seg[pos:])
			if !ok {
				break
			}
			if ctrl != nil {
				ctrl.onBytesScanned(currLength)
			}

			if maxTokenLen > 0 && prevLength+currLength > maxTokenLen {
				prevTokenID, prevLength = currTokenID, currLength
				pos += currLength
				continue
			}

			pair := uint32(prevTokenID)<<16 | uint32(currTokenID)
			if frequency.incr(pair) >= threshold {
				if nextTokenID > limitTokenID {
					return dictionary, tokenBoundaries
				}
				mergedToken := seg[pos-prevLength : pos+currLength]
				if matcher.insert(mergedToken, nextTokenID) {
					dictionary = append(dictionary, mergedToken...)
					tokenBoundaries = append(tokenBoundaries, uint32(len(dictionary)))
					frequency.remove(pair)
					prevTokenID, prevLength = nextTokenID, len(mergedToken)
					pos += currLength

					if nextTokenID == limitTokenID {
						return dictionary, tokenBoundaries
					}
					nextTokenID++
					if ctrl != nil {
						ctrl.onEntryCreated()
						threshold = ctrl.threshold
					}
					continue
				}
				frequency.remove(pair)
			}
			prevTokenID, prevLength = currTokenID, currLength
			pos += currLength
		}
	}

	return dictionary, tokenBoundaries
}

// compress parses the data using the trained matcher, emitting sorted token
// ids via remap (the matcher works in creation-order ids).
func (e *Encoder) compress(data []byte, endPositions []int, matcher *matcher, remap []uint16) ([]uint16, []int) {
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
			compressedData = append(compressedData, remap[tokenID])
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
