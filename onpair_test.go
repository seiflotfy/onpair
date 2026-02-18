package onpair

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// ============================================================================
// Helper Functions
// ============================================================================

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func mustEncode(enc *Encoder, strings []string) *Archive {
	archive, err := enc.Encode(strings)
	if err != nil {
		panic(err)
	}
	return archive
}

// Helper function to load testdata files as lines
func loadTestDataLines(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// ============================================================================
// Basic Compression Tests
// ============================================================================

func TestOnPairBasicCompression(t *testing.T) {
	strings := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	// Verify decompression
	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("String %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPair16BasicCompression(t *testing.T) {
	strings := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}

	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, strings)

	// Verify decompression
	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("String %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairEmptyStrings(t *testing.T) {
	strings := []string{"", "test", "", "data"}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("String %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPair16EmptyStrings(t *testing.T) {
	strings := []string{"", "test", "", "data"}

	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("String %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairRepeatedPatterns(t *testing.T) {
	strings := []string{
		"aaaaaaaaaa",
		"bbbbbbbbbb",
		"aaaaaaaaaa",
		"cccccccccc",
		"aaaaaaaaaa",
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("String %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairDecompressAllChecked(t *testing.T) {
	strings := []string{
		"hello",
		"world",
		"test",
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	// Calculate expected total length
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}

	buffer := make([]byte, totalLen+100) // Extra space for safety
	size, err := archive.DecompressAllChecked(buffer)
	if err != nil {
		t.Fatalf("DecompressAllChecked failed: %v", err)
	}

	expected := "helloworldtest"
	actual := string(buffer[:size])

	if actual != expected {
		t.Errorf("DecompressAllChecked: expected %q, got %q", expected, actual)
	}

	if size != totalLen {
		t.Errorf("DecompressAllChecked size: expected %d, got %d", totalLen, size)
	}
}

func TestOnPair16DecompressAllChecked(t *testing.T) {
	strings := []string{
		"hello",
		"world",
		"test",
	}

	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, strings)

	// Calculate expected total length
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}

	buffer := make([]byte, totalLen+100) // Extra space for safety
	size, err := archive.DecompressAllChecked(buffer)
	if err != nil {
		t.Fatalf("DecompressAllChecked failed: %v", err)
	}

	expected := "helloworldtest"
	actual := string(buffer[:size])

	if actual != expected {
		t.Errorf("DecompressAllChecked: expected %q, got %q", expected, actual)
	}

	if size != totalLen {
		t.Errorf("DecompressAllChecked size: expected %d, got %d", totalLen, size)
	}
}

func TestOnPairSpaceUsed(t *testing.T) {
	strings := []string{
		"user_000001",
		"user_000002",
		"user_000003",
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	space := archive.SpaceUsed()
	if space <= 0 {
		t.Errorf("SpaceUsed should be positive, got %d", space)
	}

	// Space used should be less than original size for repetitive data
	originalSize := 0
	for _, s := range strings {
		originalSize += len(s)
	}

	t.Logf("Original size: %d bytes, Compressed size: %d bytes", originalSize, space)
}

func TestModelTrainEncode(t *testing.T) {
	input := []string{
		"user_000001",
		"user_000002",
		"admin_001",
	}

	model, err := TrainModel(input, WithMaxTokenLength(16))
	if err != nil {
		t.Fatalf("TrainModel failed: %v", err)
	}
	if !model.Trained() {
		t.Fatalf("model should be trained")
	}

	archive, err := model.Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	buf := make([]byte, 256)
	for i, want := range input {
		n, err := archive.DecompressString(i, buf)
		if err != nil {
			t.Fatalf("DecompressString(%d) failed: %v", i, err)
		}
		if got := string(buf[:n]); got != want {
			t.Fatalf("row %d mismatch: got %q want %q", i, got, want)
		}
	}
}

func TestModelEncodeWithoutTrain(t *testing.T) {
	model := NewModel()
	_, err := model.Encode([]string{"x"})
	if !errors.Is(err, ErrUntrainedModel) {
		t.Fatalf("expected ErrUntrainedModel, got %v", err)
	}
}

func TestArchiveAppendRowAndDecodedLen(t *testing.T) {
	input := []string{"hello", "world", "test"}
	archive := mustEncode(NewEncoder(), input)

	if rows := archive.Rows(); rows != len(input) {
		t.Fatalf("Rows mismatch: got %d want %d", rows, len(input))
	}

	dst := make([]byte, 0, 16)
	for i, want := range input {
		wantLen, err := archive.DecodedLen(i)
		if err != nil {
			t.Fatalf("DecodedLen(%d) failed: %v", i, err)
		}
		if wantLen != len(want) {
			t.Fatalf("DecodedLen(%d): got %d want %d", i, wantLen, len(want))
		}

		dst = dst[:0]
		dst, err = archive.AppendRow(dst, i)
		if err != nil {
			t.Fatalf("AppendRow(%d) failed: %v", i, err)
		}
		if got := string(dst); got != want {
			t.Fatalf("AppendRow(%d): got %q want %q", i, got, want)
		}
	}
}

func TestArchiveStrictShortBuffer(t *testing.T) {
	input := []string{"hello", "world", "test"}
	archive := mustEncode(NewEncoder(), input)

	_, err := archive.DecompressString(0, make([]byte, 2))
	if !errors.Is(err, ErrShortBuffer) {
		t.Fatalf("DecompressString expected ErrShortBuffer, got %v", err)
	}

	_, err = archive.DecompressAllChecked(make([]byte, 4))
	if !errors.Is(err, ErrShortBuffer) {
		t.Fatalf("DecompressAllChecked expected ErrShortBuffer, got %v", err)
	}
}

func TestWithMaxTokenID(t *testing.T) {
	input := []string{
		"prefix_00001",
		"prefix_00002",
		"prefix_00003",
		"prefix_00004",
		"prefix_00005",
	}

	archive := mustEncode(NewEncoder(WithMaxTokenID(300)), input)
	buf := make([]byte, 64)
	for i, want := range input {
		n, err := archive.DecompressString(i, buf)
		if err != nil {
			t.Fatalf("DecompressString(%d) failed: %v", i, err)
		}
		if got := string(buf[:n]); got != want {
			t.Fatalf("row %d mismatch: got %q want %q", i, got, want)
		}
	}
}

func TestWithMaxTokenID255DisablesMerges(t *testing.T) {
	input := []string{
		"prefix_00001",
		"prefix_00002",
		"prefix_00003",
	}

	archive := mustEncode(NewEncoder(WithMaxTokenID(255)), input)
	for i, tok := range archive.CompressedData {
		if tok > 255 {
			t.Fatalf("token %d exceeds max token id: %d", i, tok)
		}
	}
}

func TestResolveTokenLimitWithTokenBitWidth12(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want uint16
	}{
		{
			name: "default max clipped for 12-bit",
			cfg:  Config{TokenBitWidth: tokenBitWidth12},
			want: maxTokenID12Bit,
		},
		{
			name: "explicit large max clipped for 12-bit",
			cfg:  Config{TokenBitWidth: tokenBitWidth12, MaxTokenID: 5000},
			want: maxTokenID12Bit,
		},
		{
			name: "explicit small max preserved",
			cfg:  Config{TokenBitWidth: tokenBitWidth12, MaxTokenID: 600},
			want: 600,
		},
		{
			name: "invalid width falls back to 16-bit",
			cfg:  Config{TokenBitWidth: 7},
			want: maxTokenID,
		},
	}

	for _, tc := range tests {
		got := resolveTokenLimit(tc.cfg)
		if got != tc.want {
			t.Fatalf("%s: got %d want %d", tc.name, got, tc.want)
		}
	}
}

func TestResolveTrainingSampleBytes(t *testing.T) {
	if got := resolveTrainingSampleBytes(Config{}); got != maxTrainingSampleBytes {
		t.Fatalf("default training sample bytes: got %d want %d", got, maxTrainingSampleBytes)
	}
	if got := resolveTrainingSampleBytes(Config{TrainingSampleBytes: 64 * 1024}); got != 64*1024 {
		t.Fatalf("custom training sample bytes: got %d want %d", got, 64*1024)
	}
}

func TestResolveDrainMaxClusters(t *testing.T) {
	if got := resolveDrainMaxClusters(Config{}); got != defaultDrainMaxClusters {
		t.Fatalf("default drain max clusters: got %d want %d", got, defaultDrainMaxClusters)
	}
	if got := resolveDrainMaxClusters(Config{DrainMaxClusters: 32}); got != 32 {
		t.Fatalf("custom drain max clusters: got %d want %d", got, 32)
	}
}

func TestDrainLikeTemplateKeyNormalizesDynamicTokens(t *testing.T) {
	line := []byte(`[2025-09-12T12:00:00Z] INFO client=10.1.2.3 req=550e8400-e29b-41d4-a716-446655440000 status=500`)
	key := drainLikeTemplateKey(line, 16)

	if !strings.Contains(key, "<IP>") {
		t.Fatalf("expected key to contain <IP>, got %q", key)
	}
	if !strings.Contains(key, "<UUID>") {
		t.Fatalf("expected key to contain <UUID>, got %q", key)
	}
	if !strings.Contains(key, "<NUM>") {
		t.Fatalf("expected key to contain <NUM>, got %q", key)
	}
}

func TestStratifiedSampleIndicesByDrainLike(t *testing.T) {
	rows := []string{
		"INFO service=a status=200 dur=10",
		"INFO service=a status=200 dur=11",
		"INFO service=a status=500 dur=12",
		"WARN service=b timeout=1234 host=10.2.3.4",
		"WARN service=b timeout=1500 host=10.2.3.5",
		"WARN service=b timeout=2000 host=10.2.3.6",
	}
	data, endPositions := flattenStrings(rows)
	shuffled := []int{0, 1, 2, 3, 4, 5}
	sampleLimit := len(rows[0]) + len(rows[3])

	sample, sampleBytes := stratifiedSampleIndicesByDrainLike(data, endPositions, shuffled, sampleLimit, 8)
	if len(sample) == 0 || sampleBytes == 0 {
		t.Fatalf("expected non-empty sample")
	}

	seenA := false
	seenB := false
	for _, idx := range sample {
		if idx <= 2 {
			seenA = true
		}
		if idx >= 3 {
			seenB = true
		}
	}
	if !seenA || !seenB {
		t.Fatalf("expected sample to include both clusters, got %v", sample)
	}
}

func TestOnPair16MatcherBucketBound(t *testing.T) {
	m := newMatcher(16)
	prefix := []byte("abcdefgh")
	prefixKey := bytesToU64LE(prefix, minMatch)

	inserted := 0
	for i := 0; i < maxOnPair16BucketSize+32; i++ {
		entry := append([]byte(nil), prefix...)
		entry = append(entry, byte(i), byte(i>>8))
		if m.insert(entry, uint16(inserted)) {
			inserted++
		}
	}

	if inserted != maxOnPair16BucketSize {
		t.Fatalf("inserted long-token count mismatch: got %d want %d", inserted, maxOnPair16BucketSize)
	}
	if got := len(m.longMatchBuckets[prefixKey]); got != maxOnPair16BucketSize {
		t.Fatalf("bucket size mismatch: got %d want %d", got, maxOnPair16BucketSize)
	}
}

func TestOnPair16MatcherFindLongToken(t *testing.T) {
	m := newMatcher(16)
	token := []byte("abcdefghXYZ")

	if !m.insert(token, 0) {
		t.Fatalf("insert failed")
	}
	id, n, ok := m.find([]byte("abcdefghXYZ_tail"))
	if !ok {
		t.Fatalf("expected long match")
	}
	if id != 0 {
		t.Fatalf("token id mismatch: got %d want 0", id)
	}
	if n != len(token) {
		t.Fatalf("token length mismatch: got %d want %d", n, len(token))
	}
}

// ============================================================================
// Edge Case Tests
// ============================================================================

func TestOnPairUnicodeStrings(t *testing.T) {
	strings := []string{
		"hello世界",
		"你好world",
		"🚀rocket",
		"café",
		"Ελληνικά",
		"مرحبا",
		"hello世界", // Repeat for pattern matching
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("Unicode string %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPair16UnicodeStrings(t *testing.T) {
	strings := []string{
		"hello世界",
		"你好world",
		"🚀rocket",
		"café",
	}

	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("Unicode string %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairLongStrings(t *testing.T) {
	// Test with strings longer than 16 bytes
	strings := []string{
		"this_is_a_very_long_string_that_exceeds_sixteen_bytes",
		"another_extremely_long_string_for_testing_purposes",
		"short",
		"this_is_a_very_long_string_that_exceeds_sixteen_bytes", // Repeat
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	buffer := make([]byte, 1024)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("Long string %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairSpecialCharacters(t *testing.T) {
	strings := []string{
		"tab\there",
		"newline\nhere",
		"null\x00byte",
		"special!@#$%^&*()",
		"quote\"here",
		"backslash\\here",
		"tab\there", // Repeat
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("Special char string %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairSingleByteStrings(t *testing.T) {
	strings := []string{"a", "b", "c", "a", "b", "a"}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("Single byte string %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairMaxTokenLength(t *testing.T) {
	// Test with patterns that could create very long tokens
	baseStr := "abcdefghijklmnopqrstuvwxyz0123456789"
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = baseStr // Same long string repeated
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("Max token string %d: expected %q, got %q", i, expected, actual)
		}
	}
}

// ============================================================================
// Large Dataset Tests
// ============================================================================

func TestOnPairLargeDataset(t *testing.T) {
	// Generate 100K strings with realistic patterns
	strings := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		switch i % 5 {
		case 0:
			strings[i] = "user_" + fmt.Sprintf("%06d", i)
		case 1:
			strings[i] = "admin_" + fmt.Sprintf("%06d", i)
		case 2:
			strings[i] = "guest_" + fmt.Sprintf("%06d", i)
		case 3:
			strings[i] = "system_" + fmt.Sprintf("%06d", i)
		case 4:
			strings[i] = "service_" + fmt.Sprintf("%06d", i)
		}
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	// Verify random samples
	buffer := make([]byte, 256)
	samples := []int{0, 100, 1000, 10000, 50000, 99999}
	for _, idx := range samples {
		size, err := archive.DecompressString(idx, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != strings[idx] {
			t.Errorf("String %d: expected %q, got %q", idx, strings[idx], actual)
		}
	}

	t.Logf("Successfully compressed and decompressed %d strings", len(strings))
}

func TestOnPair16LargeDataset(t *testing.T) {
	// Generate 100K strings
	strings := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		strings[i] = "id_" + fmt.Sprintf("%08d", i)
	}

	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, strings)

	// Verify random samples
	buffer := make([]byte, 256)
	samples := []int{0, 1000, 50000, 99999}
	for _, idx := range samples {
		size, err := archive.DecompressString(idx, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		actual := string(buffer[:size])
		if actual != strings[idx] {
			t.Errorf("String %d: expected %q, got %q", idx, strings[idx], actual)
		}
	}
}

// ============================================================================
// Compression Ratio Tests
// ============================================================================

func TestOnPairCompressionRatio(t *testing.T) {
	// Test with highly repetitive data
	strings := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		strings[i] = "user_000001" // Same string repeated
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	originalSize := len(strings) * len(strings[0])
	compressedSize := archive.SpaceUsed()
	ratio := float64(originalSize) / float64(compressedSize)

	t.Logf("Highly repetitive data - Original: %d bytes, Compressed: %d bytes, Ratio: %.2fx",
		originalSize, compressedSize, ratio)

	// Should achieve good compression on repetitive data
	if compressedSize >= originalSize {
		t.Errorf("Expected compression, but compressed size (%d) >= original size (%d)",
			compressedSize, originalSize)
	}
}

func TestOnPairCompressionRatioVariedData(t *testing.T) {
	// Test with varied patterns
	strings := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		strings[i] = "prefix_" + fmt.Sprintf("%d", i) + "_suffix"
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	originalSize := 0
	for _, s := range strings {
		originalSize += len(s)
	}
	compressedSize := archive.SpaceUsed()
	ratio := float64(originalSize) / float64(compressedSize)

	t.Logf("Varied data - Original: %d bytes, Compressed: %d bytes, Ratio: %.2fx",
		originalSize, compressedSize, ratio)
}

// ============================================================================
// Concurrency Tests
// ============================================================================

func TestOnPairConcurrentDecompression(t *testing.T) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_" + fmt.Sprintf("%04d", i)
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	// Decompress concurrently from multiple goroutines
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			buffer := make([]byte, 256)

			for i := 0; i < len(strings); i++ {
				size, err := archive.DecompressString(i, buffer)
				if err != nil {
					errors <- fmt.Errorf("DecompressString failed: %v", err)
					return
				}
				actual := string(buffer[:size])
				if actual != strings[i] {
					errors <- fmt.Errorf("Goroutine %d, string %d: expected %q, got %q",
						goroutineID, i, strings[i], actual)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

func TestOnPair16ConcurrentDecompression(t *testing.T) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "id_" + fmt.Sprintf("%04d", i)
	}

	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, strings)

	// Decompress concurrently
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			buffer := make([]byte, 256)

			for i := 0; i < len(strings); i++ {
				size, err := archive.DecompressString(i, buffer)
				if err != nil {
					errors <- fmt.Errorf("DecompressString failed: %v", err)
					return
				}
				actual := string(buffer[:size])
				if actual != strings[i] {
					errors <- fmt.Errorf("Goroutine %d, string %d: expected %q, got %q",
						goroutineID, i, strings[i], actual)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// ============================================================================
// Testdata File Tests
// ============================================================================

func TestAllTestdataFiles(t *testing.T) {
	testdataDir := "testdata"

	files, err := os.ReadDir(testdataDir)
	if err != nil {
		t.Fatalf("Failed to read testdata directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		t.Run(filename, func(t *testing.T) {
			filepath := filepath.Join(testdataDir, filename)

			data, err := os.ReadFile(filepath)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", filename, err)
			}

			// Split into lines
			content := string(data)
			lines := strings.Split(content, "\n")

			t.Run("OnPair", func(t *testing.T) {
				testOnPairCompression(t, lines, data)
			})

			t.Run("OnPair16", func(t *testing.T) {
				testOnPair16Compression(t, lines, data)
			})
		})
	}
}

func testOnPairCompression(t *testing.T, lines []string, originalData []byte) {
	// Compress
	enc := NewEncoder()
	archive := mustEncode(enc, lines)

	// Verify compression
	if len(archive.CompressedData) == 0 {
		t.Error("Compressed data is empty")
	}
	expectedSize := 0
	for _, line := range lines {
		expectedSize += len(line)
	}

	// Test DecompressAllChecked
	t.Run("DecompressAllChecked", func(t *testing.T) {
		buffer := make([]byte, len(originalData)+16) // Extra space for safety
		decompressedSize, err := archive.DecompressAllChecked(buffer)
		if err != nil {
			t.Fatalf("DecompressAllChecked failed: %v", err)
		}

		if decompressedSize != expectedSize {
			t.Errorf("DecompressAllChecked size mismatch: got %d, want %d", decompressedSize, expectedSize)
		}

		// Reconstruct what we expect (lines joined without separators)
		var expected bytes.Buffer
		for _, line := range lines {
			expected.WriteString(line)
		}

		if !bytes.Equal(buffer[:decompressedSize], expected.Bytes()) {
			t.Errorf("DecompressAllChecked data mismatch")
			// Show first difference
			exp := expected.Bytes()
			for i := 0; i < len(exp) && i < decompressedSize; i++ {
				if buffer[i] != exp[i] {
					t.Errorf("First difference at byte %d: got %d, want %d", i, buffer[i], exp[i])
					if i > 0 {
						t.Errorf("Context: ...%q vs ...%q", buffer[max(0, i-10):min(decompressedSize, i+10)], exp[max(0, i-10):min(len(exp), i+10)])
					}
					break
				}
			}
		}
	})

	// Test DecompressString for each line
	t.Run("DecompressString", func(t *testing.T) {
		buffer := make([]byte, len(originalData)+16)

		// Verify each line decompresses correctly
		for i, expectedLine := range lines {
			size, err := archive.DecompressString(i, buffer)
			if err != nil {
				t.Errorf("DecompressString failed: %v", err)
				continue
			}
			decompressed := string(buffer[:size])

			if decompressed != expectedLine {
				t.Errorf("Line %d mismatch:\n  got: %q\n  want: %q", i, decompressed, expectedLine)
				if len(decompressed) != len(expectedLine) {
					t.Errorf("  Size: got %d, want %d", len(decompressed), len(expectedLine))
				}
			}
		}
	})

	// Verify compression actually happened
	t.Run("VerifyCompression", func(t *testing.T) {
		compressedSize := archive.SpaceUsed()
		t.Logf("Original: %d bytes, Compressed: %d bytes, Ratio: %.2fx",
			len(originalData), compressedSize, float64(len(originalData))/float64(compressedSize))

		// For very small files, compression might not help
		if len(originalData) < 100 {
			return
		}

		// For larger files, we should see some benefit or at worst not expand too much
		if compressedSize > len(originalData)*3 {
			t.Errorf("Compression ratio too poor: %d -> %d (%.2fx expansion)",
				len(originalData), compressedSize, float64(compressedSize)/float64(len(originalData)))
		}
	})
}

func testOnPair16Compression(t *testing.T, lines []string, originalData []byte) {
	// Compress with constraint
	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, lines)

	// Verify compression
	if len(archive.CompressedData) == 0 {
		t.Error("Compressed data is empty")
	}
	expectedSize := 0
	for _, line := range lines {
		expectedSize += len(line)
	}

	// Test DecompressAllChecked
	t.Run("DecompressAllChecked", func(t *testing.T) {
		buffer := make([]byte, len(originalData)+16) // Extra space for safety

		// Catch panics
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("DecompressAllChecked panicked: %v", r)
			}
		}()

		decompressedSize, err := archive.DecompressAllChecked(buffer)
		if err != nil {
			t.Fatalf("DecompressAllChecked failed: %v", err)
		}

		if decompressedSize != expectedSize {
			t.Errorf("DecompressAllChecked size mismatch: got %d, want %d", decompressedSize, expectedSize)
		}

		// Reconstruct what we expect (lines joined without separators)
		var expected bytes.Buffer
		for _, line := range lines {
			expected.WriteString(line)
		}

		if !bytes.Equal(buffer[:decompressedSize], expected.Bytes()) {
			t.Errorf("DecompressAllChecked data mismatch")
			// Show first difference
			exp := expected.Bytes()
			for i := 0; i < len(exp) && i < decompressedSize; i++ {
				if buffer[i] != exp[i] {
					t.Errorf("First difference at byte %d: got %d, want %d", i, buffer[i], exp[i])
					if i > 0 {
						t.Errorf("Context: ...%q vs ...%q", buffer[max(0, i-10):min(decompressedSize, i+10)], exp[max(0, i-10):min(len(exp), i+10)])
					}
					break
				}
			}
		}
	})

	// Test DecompressString for each line
	t.Run("DecompressString", func(t *testing.T) {
		buffer := make([]byte, len(originalData)+16)

		// Catch panics
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("DecompressString panicked: %v", r)
			}
		}()

		// Verify each line decompresses correctly
		for i, expectedLine := range lines {
			size, err := archive.DecompressString(i, buffer)
			if err != nil {
				t.Errorf("DecompressString failed: %v", err)
				continue
			}
			decompressed := string(buffer[:size])

			if decompressed != expectedLine {
				t.Errorf("Line %d mismatch:\n  got: %q\n  want: %q", i, decompressed, expectedLine)
				if len(decompressed) != len(expectedLine) {
					t.Errorf("  Size: got %d, want %d", len(decompressed), len(expectedLine))
				}
			}
		}
	})

	// Verify compression actually happened
	t.Run("VerifyCompression", func(t *testing.T) {
		compressedSize := archive.SpaceUsed()
		t.Logf("Original: %d bytes, Compressed: %d bytes, Ratio: %.2fx",
			len(originalData), compressedSize, float64(len(originalData))/float64(compressedSize))

		// For very small files, compression might not help
		if len(originalData) < 100 {
			return
		}

		// For larger files, we should see some benefit or at worst not expand too much
		if compressedSize > len(originalData)*3 {
			t.Errorf("Compression ratio too poor: %d -> %d (%.2fx expansion)",
				len(originalData), compressedSize, float64(compressedSize)/float64(len(originalData)))
		}
	})
}

// ============================================================================
// Serialization Tests
// ============================================================================

func TestSerialization(t *testing.T) {
	strings := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}

	// Create and compress with OnPair
	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	// Serialize
	var buf bytes.Buffer
	n, err := archive.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	if n == 0 {
		t.Fatalf("WriteTo wrote 0 bytes")
	}

	// Deserialize
	archive2 := &Archive{}
	n2, err := archive2.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if n2 != n {
		t.Errorf("ReadFrom read %d bytes, WriteTo wrote %d bytes", n2, n)
	}

	// Verify all fields match
	// MaxTokenLen is not stored in Archive anymore, so we skip this check
	// if archive2.MaxTokenLen != archive.MaxTokenLen { ... }
	if len(archive2.CompressedData) != len(archive.CompressedData) {
		t.Errorf("CompressedData length mismatch: got %d, want %d", len(archive2.CompressedData), len(archive.CompressedData))
	}
	if len(archive2.StringBoundaries) != len(archive.StringBoundaries) {
		t.Errorf("StringBoundaries length mismatch: got %d, want %d", len(archive2.StringBoundaries), len(archive.StringBoundaries))
	}
	// MaxTokenLen is not stored in Archive anymore, so we skip this check
	// if archive2.MaxTokenLen != archive.MaxTokenLen { ... }
	if len(archive2.TokenBoundaries) != len(archive.TokenBoundaries) {
		t.Errorf("TokenBoundaries length mismatch: got %d, want %d", len(archive2.TokenBoundaries), len(archive.TokenBoundaries))
	}

	// Verify decompression works correctly
	buffer := make([]byte, 256)
	for i := 0; i < len(strings); i++ {
		size, err := archive2.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		decompressed := string(buffer[:size])
		if decompressed != strings[i] {
			t.Errorf("Decompression mismatch at index %d: got %q, want %q", i, decompressed, strings[i])
		}
	}
}

func TestSerialization16(t *testing.T) {
	strings := []string{
		"user_001",
		"user_002",
		"user_003",
		"admin_01",
	}

	// Create and compress with OnPair16
	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, strings)

	// Serialize
	var buf bytes.Buffer
	n, err := archive.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Deserialize
	archive2 := &Archive{}
	n2, err := archive2.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if n2 != n {
		t.Errorf("ReadFrom read %d bytes, WriteTo wrote %d bytes", n2, n)
	}

	// Verify MaxTokenLen is preserved (16 for OnPair16)
	// MaxTokenLen is not stored in Archive anymore
	// if archive2.MaxTokenLen != 16 { ... }

	// Verify decompression works correctly
	buffer := make([]byte, 256)
	for i := 0; i < len(strings); i++ {
		size, err := archive2.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		decompressed := string(buffer[:size])
		if decompressed != strings[i] {
			t.Errorf("Decompression mismatch at index %d: got %q, want %q", i, decompressed, strings[i])
		}
	}
}

func TestSerializationPacked12Bit(t *testing.T) {
	input := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}

	archive := mustEncode(NewEncoder(WithTokenBitWidth(12)), input)
	if archive.tokenBitWidth() != tokenBitWidth12 {
		t.Fatalf("token bit-width mismatch: got %d want %d", archive.tokenBitWidth(), tokenBitWidth12)
	}

	expectedCompressedBytes := packed12ByteSize(len(archive.CompressedData))
	expectedSpaceUsed := expectedCompressedBytes + len(archive.Dictionary) + len(archive.TokenBoundaries)*4
	if got := archive.SpaceUsed(); got != expectedSpaceUsed {
		t.Fatalf("SpaceUsed mismatch: got %d want %d", got, expectedSpaceUsed)
	}

	var buf bytes.Buffer
	if _, err := archive.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		t.Fatalf("read magic failed: %v", err)
	}
	if string(magic[:]) != archiveMagic {
		t.Fatalf("magic mismatch: got %q want %q", string(magic[:]), archiveMagic)
	}

	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		t.Fatalf("read version failed: %v", err)
	}
	if version != archiveVersion {
		t.Fatalf("version mismatch: got %d want %d", version, archiveVersion)
	}

	var stageCount uint16
	if err := binary.Read(r, binary.LittleEndian, &stageCount); err != nil {
		t.Fatalf("read stage count failed: %v", err)
	}

	foundCompressed := false
	for i := 0; i < int(stageCount); i++ {
		header, _, err := readStageHeader(r)
		if err != nil {
			t.Fatalf("readStageHeader(%d) failed: %v", i, err)
		}
		params := make([]byte, header.paramLen)
		if _, err := io.ReadFull(r, params); err != nil {
			t.Fatalf("read params(%d) failed: %v", i, err)
		}

		if header.name == stageCompressedData {
			foundCompressed = true
			if len(params) != 1 {
				t.Fatalf("compressed_data params length mismatch: got %d want 1", len(params))
			}
			if params[0] != stageCompressedDataParamWidth12 && params[0] != stageCompressedDataParamWidth12Flate {
				t.Fatalf("compressed_data params mismatch: got %v want [%d or %d]", params, stageCompressedDataParamWidth12, stageCompressedDataParamWidth12Flate)
			}
			if params[0] == stageCompressedDataParamWidth12 {
				expectedPayloadLen := uint32(4 + expectedCompressedBytes)
				if header.dataLen != expectedPayloadLen {
					t.Fatalf("compressed_data payload length mismatch: got %d want %d", header.dataLen, expectedPayloadLen)
				}
			}
		}

		if _, err := io.CopyN(io.Discard, r, int64(header.dataLen)); err != nil {
			t.Fatalf("skip payload(%d) failed: %v", i, err)
		}
	}
	if !foundCompressed {
		t.Fatalf("compressed_data stage not found")
	}

	loaded := &Archive{}
	if _, err := loaded.ReadFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if loaded.tokenBitWidth() != tokenBitWidth12 {
		t.Fatalf("loaded token bit-width mismatch: got %d want %d", loaded.tokenBitWidth(), tokenBitWidth12)
	}
	if !slices.Equal(loaded.CompressedData, archive.CompressedData) {
		t.Fatalf("compressed data mismatch after round-trip")
	}
}

func TestSerializationCompressedDataUsesFlateWhenSmaller16Bit(t *testing.T) {
	rows := make([]string, 30000)
	for i := range rows {
		rows[i] = "GET /api/v1/users/42 HTTP/1.1"
	}

	archive := mustEncode(NewEncoder(), rows)
	if archive.tokenBitWidth() != tokenBitWidth16 {
		t.Fatalf("token bit-width mismatch: got %d want %d", archive.tokenBitWidth(), tokenBitWidth16)
	}

	var blob bytes.Buffer
	if _, err := archive.WriteTo(&blob); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	r := bytes.NewReader(blob.Bytes())
	if _, err := io.CopyN(io.Discard, r, 8); err != nil {
		t.Fatalf("skip archive header failed: %v", err)
	}

	var stageCount uint16
	if err := binary.Read(bytes.NewReader(blob.Bytes()[6:8]), binary.LittleEndian, &stageCount); err != nil {
		t.Fatalf("read stage count failed: %v", err)
	}

	foundCompressed := false
	for i := 0; i < int(stageCount); i++ {
		header, _, err := readStageHeader(r)
		if err != nil {
			t.Fatalf("readStageHeader(%d) failed: %v", i, err)
		}
		params := make([]byte, header.paramLen)
		if _, err := io.ReadFull(r, params); err != nil {
			t.Fatalf("read params(%d) failed: %v", i, err)
		}
		payload := make([]byte, header.dataLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			t.Fatalf("read payload(%d) failed: %v", i, err)
		}

		if header.name != stageCompressedData {
			continue
		}
		foundCompressed = true
		if len(params) != 1 || params[0] != stageCompressedDataParamWidth16Flate {
			t.Fatalf("compressed_data params mismatch: got %v want [%d]", params, stageCompressedDataParamWidth16Flate)
		}

		raw, err := encodeCompressedDataStage16(archive.CompressedData)
		if err != nil {
			t.Fatalf("encodeCompressedDataStage16 failed: %v", err)
		}
		if len(payload) >= len(raw) {
			t.Fatalf("flate compressed payload not smaller: got %d want < %d", len(payload), len(raw))
		}
	}
	if !foundCompressed {
		t.Fatalf("compressed_data stage not found")
	}

	loaded := &Archive{}
	if _, err := loaded.ReadFrom(bytes.NewReader(blob.Bytes())); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if !slices.Equal(loaded.CompressedData, archive.CompressedData) {
		t.Fatalf("compressed data mismatch after round-trip")
	}
}

func TestSerializationCompressedDataUsesFlateWhenSmaller12Bit(t *testing.T) {
	rows := make([]string, 30000)
	for i := range rows {
		rows[i] = "GET /api/v1/users/42 HTTP/1.1"
	}

	archive := mustEncode(NewEncoder(WithTokenBitWidth(12)), rows)
	if archive.tokenBitWidth() != tokenBitWidth12 {
		t.Fatalf("token bit-width mismatch: got %d want %d", archive.tokenBitWidth(), tokenBitWidth12)
	}

	var blob bytes.Buffer
	if _, err := archive.WriteTo(&blob); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	r := bytes.NewReader(blob.Bytes())
	if _, err := io.CopyN(io.Discard, r, 8); err != nil {
		t.Fatalf("skip archive header failed: %v", err)
	}

	var stageCount uint16
	if err := binary.Read(bytes.NewReader(blob.Bytes()[6:8]), binary.LittleEndian, &stageCount); err != nil {
		t.Fatalf("read stage count failed: %v", err)
	}

	foundCompressed := false
	for i := 0; i < int(stageCount); i++ {
		header, _, err := readStageHeader(r)
		if err != nil {
			t.Fatalf("readStageHeader(%d) failed: %v", i, err)
		}
		params := make([]byte, header.paramLen)
		if _, err := io.ReadFull(r, params); err != nil {
			t.Fatalf("read params(%d) failed: %v", i, err)
		}
		payload := make([]byte, header.dataLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			t.Fatalf("read payload(%d) failed: %v", i, err)
		}

		if header.name != stageCompressedData {
			continue
		}
		foundCompressed = true
		if len(params) != 1 || params[0] != stageCompressedDataParamWidth12Flate {
			t.Fatalf("compressed_data params mismatch: got %v want [%d]", params, stageCompressedDataParamWidth12Flate)
		}

		raw, err := encodeCompressedDataStage12(archive.CompressedData)
		if err != nil {
			t.Fatalf("encodeCompressedDataStage12 failed: %v", err)
		}
		if len(payload) >= len(raw) {
			t.Fatalf("flate compressed payload not smaller: got %d want < %d", len(payload), len(raw))
		}
	}
	if !foundCompressed {
		t.Fatalf("compressed_data stage not found")
	}

	loaded := &Archive{}
	if _, err := loaded.ReadFrom(bytes.NewReader(blob.Bytes())); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if !slices.Equal(loaded.CompressedData, archive.CompressedData) {
		t.Fatalf("compressed data mismatch after round-trip")
	}
}

func TestDecodeCompressedDataStageRejectsInvalidFlatePayload(t *testing.T) {
	dst := &Archive{}
	err := decodeCompressedDataStage(dst, []byte{stageCompressedDataParamWidth16Flate}, []byte{0x00, 0x01, 0x02, 0x03})
	if err == nil {
		t.Fatalf("expected decodeCompressedDataStage to fail on invalid flate payload")
	}
}

func TestSerializationPacked12BitRejectsOutOfRangeToken(t *testing.T) {
	tokenID := uint16(maxTokenID12Bit + 1)
	tokenBoundaries := make([]uint32, int(tokenID)+2)
	dictionary := make([]byte, int(tokenID)+1)
	for i := range tokenBoundaries {
		tokenBoundaries[i] = uint32(i)
	}

	archive := &Archive{
		CompressedData:          []uint16{tokenID},
		StringBoundaries:        []int{0, 1},
		Dictionary:              dictionary,
		TokenBoundaries:         tokenBoundaries,
		compressedTokenBitWidth: tokenBitWidth12,
	}

	if _, err := archive.WriteTo(io.Discard); err == nil {
		t.Fatalf("expected WriteTo to fail for out-of-range 12-bit token")
	}
}

func TestReadFromSkipsUnknownStage(t *testing.T) {
	input := []string{"user_001", "user_002", "admin_001"}
	archive := mustEncode(NewEncoder(), input)

	var buf bytes.Buffer
	if _, err := archive.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	serialized := append([]byte(nil), buf.Bytes()...)
	if len(serialized) < 8 {
		t.Fatalf("serialized archive too small: %d", len(serialized))
	}

	stageCount := binary.LittleEndian.Uint16(serialized[6:8])
	binary.LittleEndian.PutUint16(serialized[6:8], stageCount+1)

	var extra bytes.Buffer
	if _, err := writeStage(&extra, "unknown.stage", []byte{0x42}, []byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("writeStage failed: %v", err)
	}
	serialized = append(serialized, extra.Bytes()...)

	decoded := &Archive{}
	if _, err := decoded.ReadFrom(bytes.NewReader(serialized)); err != nil {
		t.Fatalf("ReadFrom failed with unknown stage: %v", err)
	}

	buffer := make([]byte, 256)
	for i, expected := range input {
		n, err := decoded.DecompressString(i, buffer)
		if err != nil {
			t.Fatalf("DecompressString(%d) failed: %v", i, err)
		}
		if got := string(buffer[:n]); got != expected {
			t.Fatalf("row %d mismatch: got %q want %q", i, got, expected)
		}
	}
}

func TestReadFromRejectsOversizedStagePayload(t *testing.T) {
	var buf bytes.Buffer
	if _, err := buf.Write([]byte(archiveMagic)); err != nil {
		t.Fatalf("write magic failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, archiveVersion); err != nil {
		t.Fatalf("write version failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint16(1)); err != nil {
		t.Fatalf("write stage count failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint8(1)); err != nil {
		t.Fatalf("write stage name len failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint16(0)); err != nil {
		t.Fatalf("write stage param len failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(maxStagePayloadBytes+1)); err != nil {
		t.Fatalf("write stage payload len failed: %v", err)
	}

	decoded := &Archive{}
	_, err := decoded.ReadFrom(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatalf("expected oversized stage payload error")
	}
}

func TestReadFromRejectsMissingRequiredStages(t *testing.T) {
	var buf bytes.Buffer
	if _, err := buf.Write([]byte(archiveMagic)); err != nil {
		t.Fatalf("write magic failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, archiveVersion); err != nil {
		t.Fatalf("write version failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint16(1)); err != nil {
		t.Fatalf("write stage count failed: %v", err)
	}
	if _, err := writeStage(&buf, "unknown.only", nil, []byte{9, 9, 9}); err != nil {
		t.Fatalf("writeStage failed: %v", err)
	}

	decoded := &Archive{}
	_, err := decoded.ReadFrom(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatalf("expected missing stage error")
	}
}

func TestDecompressAllCheckedReturnsErrorOnFailure(t *testing.T) {
	archive := mustEncode(NewEncoder(), []string{"hello", "world"})
	if len(archive.CompressedData) == 0 {
		t.Fatalf("expected compressed data")
	}

	corrupt := &Archive{
		CompressedData:   append([]uint16(nil), archive.CompressedData...),
		StringBoundaries: append([]int(nil), archive.StringBoundaries...),
		Dictionary:       append([]byte(nil), archive.Dictionary...),
		TokenBoundaries:  append([]uint32(nil), archive.TokenBoundaries...),
	}
	corrupt.CompressedData[0] = uint16(len(corrupt.TokenBoundaries) + 1)

	if _, err := corrupt.DecompressAllChecked(make([]byte, 32)); err == nil {
		t.Fatalf("DecompressAllChecked should return an error on decode failure")
	}
}

func TestReadFromErrorIncludesOffsetAndStageIndex(t *testing.T) {
	archive := mustEncode(NewEncoder(), []string{"user_001", "admin_001"})

	var buf bytes.Buffer
	if _, err := archive.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	serialized := buf.Bytes()
	if len(serialized) < 2 {
		t.Fatalf("serialized archive too small: %d", len(serialized))
	}

	truncated := serialized[:len(serialized)-1]
	decoded := &Archive{}
	_, err := decoded.ReadFrom(bytes.NewReader(truncated))
	if err == nil {
		t.Fatalf("expected read error from truncated archive")
	}
	msg := err.Error()
	if !strings.Contains(msg, "offset") {
		t.Fatalf("expected error to include offset, got: %v", err)
	}
	if !strings.Contains(msg, "stage index") {
		t.Fatalf("expected error to include stage index, got: %v", err)
	}
}

func TestSerializationLargeFile(t *testing.T) {
	testFile := "testdata/art_of_war.txt"
	lines, err := loadTestDataLines(testFile)
	if err != nil {
		t.Skipf("Failed to load %s: %v", testFile, err)
	}

	// Calculate original size
	originalSize := 0
	for _, line := range lines {
		originalSize += len(line)
	}

	// Compress
	enc := NewEncoder()
	archive := mustEncode(enc, lines)

	// Serialize
	var buf bytes.Buffer
	_, err = archive.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	serializedSize := buf.Len()
	ratio := float64(originalSize) / float64(serializedSize)
	t.Logf("Original: %d bytes, Serialized: %d bytes, Ratio: %.2fx", originalSize, serializedSize, ratio)

	// Deserialize
	archive2 := &Archive{}
	_, err = archive2.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify all strings decompress correctly
	buffer := make([]byte, 4096)
	for i := 0; i < len(lines); i++ {
		size, err := archive2.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		decompressed := string(buffer[:size])
		if decompressed != lines[i] {
			t.Errorf("Line %d mismatch after serialization/deserialization", i)
			break
		}
	}
}

func TestDeltaEncodingSavings(t *testing.T) {
	testFile := "testdata/logs_apache_2k.log"
	lines, err := loadTestDataLines(testFile)
	if err != nil {
		t.Skipf("Failed to load %s: %v", testFile, err)
	}

	// Compress
	enc := NewEncoder()
	archive := mustEncode(enc, lines)

	// Serialize with delta encoding
	var buf bytes.Buffer
	_, err = archive.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	actualSerializedSize := buf.Len()
	numBoundaries := len(archive.StringBoundaries)

	// Calculate what the size would have been with absolute encoding
	// Original format: 4 bytes (length) + numBoundaries * 8 bytes
	absoluteBoundariesSize := 4 + numBoundaries*8

	// Calculate current delta encoding size by manually encoding
	deltaOverhead := 4 + 8 + 4 // length + first boundary + delta buf length
	deltaBufSize := 0
	varintBuf := make([]byte, 10)
	for i := 1; i < len(archive.StringBoundaries); i++ {
		delta := archive.StringBoundaries[i] - archive.StringBoundaries[i-1]
		n := binary.PutUvarint(varintBuf, uint64(delta))
		deltaBufSize += n
	}
	deltaBoundariesSize := deltaOverhead + deltaBufSize

	t.Logf("StringBoundaries encoding comparison:")
	t.Logf("  Number of boundaries: %d", numBoundaries)
	t.Logf("  Absolute encoding: 4 (len) + %d × 8 = %d bytes", numBoundaries, absoluteBoundariesSize)
	t.Logf("  Delta encoding: 4 (len) + 8 (first) + 4 (delta len) + %d (deltas) = %d bytes",
		deltaBufSize, deltaBoundariesSize)

	savings := absoluteBoundariesSize - deltaBoundariesSize
	savingsPercent := float64(savings) / float64(absoluteBoundariesSize) * 100

	t.Logf("  Savings: %d bytes (%.1f%% reduction)", savings, savingsPercent)
	t.Logf("  Total serialized size: %d bytes", actualSerializedSize)

	if savingsPercent < 50 {
		t.Errorf("Expected at least 50%% savings from delta encoding, got %.1f%%", savingsPercent)
	}

	// Verify deserialization still works
	archive2 := &Archive{}
	_, err = archive2.ReadFrom(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify boundaries match
	if len(archive2.StringBoundaries) != len(archive.StringBoundaries) {
		t.Fatalf("Boundary count mismatch: got %d, want %d", len(archive2.StringBoundaries), len(archive.StringBoundaries))
	}

	for i := range archive.StringBoundaries {
		if archive2.StringBoundaries[i] != archive.StringBoundaries[i] {
			t.Errorf("Boundary %d mismatch: got %d, want %d", i, archive2.StringBoundaries[i], archive.StringBoundaries[i])
		}
	}
}

func TestSerializationSizes(t *testing.T) {
	testFiles := []string{
		"testdata/art_of_war.txt",
		"testdata/en_bible_kjv.txt",
		"testdata/en_mobydick.txt",
		"testdata/en_shakespeare.txt",
		"testdata/logs_apache_2k.log",
		"testdata/logs_hdfs_2k.log",
		"testdata/zh_tao_te_ching_en.txt",
	}

	configs := []struct {
		name string
		opts []Option
	}{
		{"Default", nil},
		{"Threshold=10", []Option{WithThreshold(10)}},
		{"MaxTokenID=4095", []Option{WithMaxTokenID(4095)}},
		{"Threshold=10 + MaxTokenID=4095", []Option{WithThreshold(10), WithMaxTokenID(4095)}},
	}

	for _, cfg := range configs {
		t.Logf("\n=== OnPair Serialization Sizes (%s) ===", cfg.name)
		t.Logf("%-25s | %12s | %12s | %12s | %12s | %s", "File", "Original", "SpaceUsed", "Full In-Mem", "Serialized", "Ratio")
		t.Log("--------------------------|--------------|--------------|--------------|--------------|-------")

		for _, testFile := range testFiles {
			lines, err := loadTestDataLines(testFile)
			if err != nil {
				t.Logf("Skipping %s: %v", testFile, err)
				continue
			}

			// Calculate original size
			originalSize := 0
			for _, line := range lines {
				originalSize += len(line)
			}

			// Compress with OnPair
			enc := NewEncoder(cfg.opts...)
			archive := mustEncode(enc, lines)
			spaceUsed := archive.SpaceUsed()

			// Calculate full in-memory size (including StringBoundaries)
			// StringBoundaries stored as []int (8 bytes each on 64-bit systems)
			fullInMemory := spaceUsed + len(archive.StringBoundaries)*8

			// Serialize
			var buf bytes.Buffer
			_, err = archive.WriteTo(&buf)
			if err != nil {
				t.Errorf("WriteTo failed for %s: %v", testFile, err)
				continue
			}

			serializedSize := buf.Len()
			ratio := float64(originalSize) / float64(serializedSize)

			// Extract filename
			name := testFile[9:] // Remove "testdata/"
			t.Logf("%-25s | %12d | %12d | %12d | %12d | %.2fx", name, originalSize, spaceUsed, fullInMemory, serializedSize, ratio)
		}
	}

	t.Log("\n=== OnPair16 Serialization Sizes (Legacy Mode) ===")
	t.Logf("%-25s | %12s | %12s | %12s | %12s | %s", "File", "Original", "SpaceUsed", "Full In-Mem", "Serialized", "Ratio")
	t.Log("--------------------------|--------------|--------------|--------------|--------------|-------")

	for _, testFile := range testFiles {
		lines, err := loadTestDataLines(testFile)
		if err != nil {
			continue
		}

		// Calculate original size
		originalSize := 0
		for _, line := range lines {
			originalSize += len(line)
		}

		// Compress with OnPair16 equivalent
		enc := NewEncoder(WithMaxTokenLength(16))
		archive := mustEncode(enc, lines)
		spaceUsed := archive.SpaceUsed()

		// Calculate full in-memory size (including StringBoundaries)
		fullInMemory := spaceUsed + len(archive.StringBoundaries)*8

		// Serialize
		var buf bytes.Buffer
		_, err = archive.WriteTo(&buf)
		if err != nil {
			t.Errorf("WriteTo failed for %s: %v", testFile, err)
			continue
		}

		serializedSize := buf.Len()
		ratio := float64(originalSize) / float64(serializedSize)

		// Extract filename
		name := testFile[9:] // Remove "testdata/"
		t.Logf("%-25s | %12d | %12d | %12d | %12d | %.2fx", name, originalSize, spaceUsed, fullInMemory, serializedSize, ratio)
	}
}

// ============================================================================
// Analysis Tests
// ============================================================================

func TestVarintEncoding(t *testing.T) {
	testFile := "testdata/art_of_war.txt"
	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Skipf("Skipping test, file not found: %v", err)
		return
	}

	content := string(data)
	lines := strings.Split(content, "\n")

	t.Logf("Testing: %s (%d bytes)\n", testFile, len(data))

	// Compress with variable-length encoding
	enc := NewEncoder()
	archive := mustEncode(enc, lines)

	ratio := float64(len(data)) / float64(archive.SpaceUsed())

	compData := archive.CompressedData
	dictData := archive.Dictionary
	bounds := archive.TokenBoundaries

	t.Logf("Variable-length encoding:")
	t.Logf("  Tokens: %d", len(bounds)-1)
	t.Logf("  Compressed data: %d bytes (variable-length token IDs)", len(compData))
	t.Logf("  Dictionary: %d bytes", len(dictData))
	t.Logf("  Token boundaries: %d bytes (%d entries × 4)", len(bounds)*4, len(bounds))
	t.Logf("  Total: %d bytes", archive.SpaceUsed())
	t.Logf("  Ratio: %.2fx\n", ratio)

	// Show breakdown
	if ratio >= 1.0 {
		t.Logf("SUCCESS: Data compressed to %.1f%% of original size!", 100.0/ratio)
	} else {
		t.Logf("Data expanded to %.1f%% of original size", 100.0*ratio)
	}

	// Verify decompression works
	buffer := make([]byte, len(data)+16) // Add padding for safety
	for i := 0; i < len(lines); i++ {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		decompressed := string(buffer[:size])
		if decompressed != lines[i] {
			t.Errorf("Decompression mismatch at line %d: expected %q, got %q", i, lines[i], decompressed)
		}
	}
}

func TestRealBaseline(t *testing.T) {
	testFile := "testdata/art_of_war.txt"
	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Skipf("Skipping test, file not found: %v", err)
		return
	}

	t.Logf("Original file: %d bytes\n", len(data))

	// Variable-length encoding for single-byte tokens (0-127 use 1 byte each)
	// Most bytes in UTF-8 text are single-byte (ASCII) or multibyte but still <128

	// Count how many bytes have value < 128
	singleByteCount := 0
	for _, b := range data {
		if b < 128 {
			singleByteCount++
		}
	}
	doubleByteCount := len(data) - singleByteCount

	t.Logf("Byte distribution:")
	t.Logf("  Values 0-127: %d bytes (encode as 1 byte each)", singleByteCount)
	t.Logf("  Values 128-255: %d bytes (encode as 2 bytes each)", doubleByteCount)

	// Variable-length single-byte encoding
	varLengthSize := singleByteCount*1 + doubleByteCount*2 + 256 + 256*4
	t.Logf("\nVariable-length single-byte encoding:")
	t.Logf("  Compressed data: %d bytes", singleByteCount+doubleByteCount*2)
	t.Logf("  Dictionary: 256 bytes")
	t.Logf("  Token boundaries: %d bytes (256 × 4)", 256*4)
	t.Logf("  Total: %d bytes", varLengthSize)
	t.Logf("  Ratio: %.2fx", float64(len(data))/float64(varLengthSize))
}

func TestComparison(t *testing.T) {
	testFile := "testdata/art_of_war.txt"
	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Skipf("Skipping test, file not found: %v", err)
		return
	}

	content := string(data)
	lines := strings.Split(content, "\n")

	t.Logf("Testing: %s (%d bytes)\n", testFile, len(data))

	// Test new multi-round
	enc := NewEncoder()
	archive := mustEncode(enc, lines)

	ratio := float64(len(data)) / float64(archive.SpaceUsed())
	t.Logf("Multi-round training:")
	t.Logf("  Tokens: %d", len(archive.TokenBoundaries)-1)
	t.Logf("  Dict size: %d bytes", len(archive.Dictionary))
	t.Logf("  Compressed: %d bytes", archive.SpaceUsed())
	t.Logf("  Ratio: %.2fx", ratio)
	t.Logf("")

	// Show first 10 multi-byte tokens
	t.Logf("First 10 multi-byte tokens:")
	tokenBounds := archive.TokenBoundaries
	dictData := archive.Dictionary
	for i := 256; i < 266 && i < len(tokenBounds)-1; i++ {
		start := tokenBounds[i]
		end := tokenBounds[i+1]
		token := dictData[start:end]
		t.Logf("  Token %d (len=%d): %q", i, len(token), string(token))
	}

	// Verify decompression works
	buffer := make([]byte, len(data)+16) // Add padding for safety
	for i := 0; i < len(lines); i++ {
		size, err := archive.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		decompressed := string(buffer[:size])
		if decompressed != lines[i] {
			t.Errorf("Decompression mismatch at line %d: expected %q, got %q", i, lines[i], decompressed)
		}
	}
}

func TestBaseline(t *testing.T) {
	testFile := "testdata/art_of_war.txt"
	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Skipf("Skipping test, file not found: %v", err)
		return
	}

	content := string(data)
	lines := strings.Split(content, "\n")

	t.Logf("Testing: %s (%d bytes)\n", testFile, len(data))

	// Single-byte only (theoretical baseline)
	// compressedData: len(data) * 2 bytes
	// dictionary: 256 bytes
	// tokenBoundaries: 256 * 4 bytes
	singleByteSize := len(data)*2 + 256 + 256*4
	t.Logf("Single-byte encoding only:")
	t.Logf("  Space: %d bytes", singleByteSize)
	t.Logf("  Ratio: %.2fx\n", float64(len(data))/float64(singleByteSize))

	// Current multi-byte approach
	enc := NewEncoder()
	archive := mustEncode(enc, lines)

	ratio := float64(len(data)) / float64(archive.SpaceUsed())
	t.Logf("Multi-byte tokens:")
	t.Logf("  Tokens: %d", len(archive.TokenBoundaries)-1)
	t.Logf("  Space: %d bytes", archive.SpaceUsed())
	t.Logf("  Ratio: %.2fx", ratio)
	t.Logf("  Improvement vs single-byte: %.1f%%",
		100.0*(1.0-float64(archive.SpaceUsed())/float64(singleByteSize)))
}

func TestTokenAnalysis(t *testing.T) {
	testFile := "testdata/art_of_war.txt"
	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Skipf("Skipping test, file not found: %v", err)
		return
	}

	content := string(data)
	lines := strings.Split(content, "\n")

	enc := NewEncoder()
	archive := mustEncode(enc, lines)

	bounds := archive.TokenBoundaries
	dictData := archive.Dictionary

	// Analyze token usage
	multiByteTokens := 0
	multiByteSize := 0
	for i := 256; i < len(bounds)-1; i++ {
		start := bounds[i]
		end := bounds[i+1]
		tokenLen := int(end - start)
		multiByteTokens++
		multiByteSize += tokenLen
	}

	t.Logf("Token breakdown:")
	t.Logf("  Single-byte tokens (0-255): 256 tokens, %d bytes dict", 256)
	t.Logf("  Multi-byte tokens (256+): %d tokens, %d bytes dict", multiByteTokens, multiByteSize)
	t.Logf("  Total dict: %d bytes\n", len(dictData))

	t.Logf("If we don't store single-byte tokens:")
	t.Logf("  Dictionary: %d bytes (was %d, save 256)", multiByteSize, len(dictData))
	t.Logf("  Token boundaries: %d bytes (was %d, save %d)",
		multiByteTokens*4, len(bounds)*4, (len(bounds)-multiByteTokens)*4)
	t.Logf("  Compressed data: %d bytes (unchanged)", len(archive.CompressedData))

	newTotal := len(archive.CompressedData) + multiByteSize + multiByteTokens*4
	t.Logf("  New total: %d bytes", newTotal)
	t.Logf("  New ratio: %.2fx", float64(len(data))/float64(newTotal))

	if newTotal < len(data) {
		t.Logf("\n✓ SUCCESS: Compressed to %.1f%% of original!", 100.0*float64(newTotal)/float64(len(data)))
	}
}

func TestCompressionRatioSummary(t *testing.T) {
	testFiles := []string{
		"testdata/art_of_war.txt",
		"testdata/logs_apache_2k.log",
		"testdata/logs_hdfs_2k.log",
		"testdata/zh_tao_te_ching_en.txt",
	}

	fmt.Println("\n=== OnPair Compression Ratio Summary ===")
	fmt.Println("Dataset                        | Original | OnPair  | Ratio | OnPair16 | Ratio")
	fmt.Println("-------------------------------|----------|---------|-------|----------|-------")

	for _, testFile := range testFiles {
		lines, err := loadTestDataLines(testFile)
		if err != nil {
			continue
		}

		parts := strings.Split(testFile, "/")
		name := parts[len(parts)-1]

		originalSize := 0
		for _, line := range lines {
			originalSize += len(line)
		}

		// OnPair
		enc := NewEncoder()
		archive := mustEncode(enc, lines)
		onpairSize := archive.SpaceUsed()
		onpairRatio := float64(originalSize) / float64(onpairSize)

		// OnPair16
		enc16 := NewEncoder(WithMaxTokenLength(16))
		archive16 := mustEncode(enc16, lines)
		onpair16Size := archive16.SpaceUsed()
		onpair16Ratio := float64(originalSize) / float64(onpair16Size)

		fmt.Printf("%-30s | %8d | %7d | %5.2fx | %8d | %5.2fx\n",
			name, originalSize, onpairSize, onpairRatio, onpair16Size, onpair16Ratio)
	}
	fmt.Println()
}

// ============================================================================
// Fuzz Tests
// ============================================================================

func FuzzOnPairCompression(f *testing.F) {
	// Seed corpus with interesting test cases
	f.Add("hello")
	f.Add("user_000001")
	f.Add("hello世界")
	f.Add("🚀rocket")
	f.Add("")
	f.Add("a")
	f.Add("abcdefghijklmnopqrstuvwxyz")
	f.Add("tab\there")
	f.Add("null\x00byte")

	f.Fuzz(func(t *testing.T, input string) {
		// Create a slice with the input string repeated to enable pattern matching
		strings := []string{input, input, input}

		enc := NewEncoder()
		archive := mustEncode(enc, strings)

		// Verify decompression
		buffer := make([]byte, len(input)*2+100) // Extra space for safety
		for i, expected := range strings {
			size, err := archive.DecompressString(i, buffer)
			if err != nil {
				t.Errorf("DecompressString failed: %v", err)
				continue
			}
			actual := string(buffer[:size])
			if actual != expected {
				t.Errorf("String %d: expected %q, got %q", i, expected, actual)
			}
		}
	})
}

func FuzzOnPair16Compression(f *testing.F) {
	// Seed corpus with interesting test cases
	f.Add("hello")
	f.Add("user_001")
	f.Add("hello世界")
	f.Add("🚀")
	f.Add("")
	f.Add("x")
	f.Add("1234567890abcdef") // Exactly 16 bytes

	f.Fuzz(func(t *testing.T, input string) {
		// Create a slice with the input string repeated
		strings := []string{input, input, input}

		enc := NewEncoder(WithMaxTokenLength(16))
		archive := mustEncode(enc, strings)

		// Verify decompression
		buffer := make([]byte, len(input)*2+100) // Extra space for safety
		for i, expected := range strings {
			size, err := archive.DecompressString(i, buffer)
			if err != nil {
				t.Errorf("DecompressString failed: %v", err)
				continue
			}
			actual := string(buffer[:size])
			if actual != expected {
				t.Errorf("String %d: expected %q, got %q", i, expected, actual)
			}
		}
	})
}

func FuzzOnPairMultipleStrings(f *testing.F) {
	// Seed with pairs of strings
	f.Add("hello", "world")
	f.Add("user_", "admin_")
	f.Add("café", "naïve")

	f.Fuzz(func(t *testing.T, s1, s2 string) {
		// Create various combinations
		strings := []string{s1, s2, s1, s2, s1 + s2, s2 + s1}

		enc := NewEncoder()
		archive := mustEncode(enc, strings)

		// Verify decompression
		maxLen := len(s1) + len(s2) + 100
		buffer := make([]byte, maxLen)
		for i, expected := range strings {
			size, err := archive.DecompressString(i, buffer)
			if err != nil {
				t.Errorf("DecompressString failed: %v", err)
				continue
			}
			actual := string(buffer[:size])
			if actual != expected {
				t.Errorf("String %d: expected %q, got %q", i, expected, actual)
			}
		}
	})
}

func FuzzOnPairDecompressAllChecked(f *testing.F) {
	f.Add("foo", "bar", "baz")

	f.Fuzz(func(t *testing.T, s1, s2, s3 string) {
		strings := []string{s1, s2, s3}

		enc := NewEncoder()
		archive := mustEncode(enc, strings)

		// Calculate expected result
		expected := s1 + s2 + s3
		totalLen := len(expected)

		// Decompress all
		buffer := make([]byte, totalLen+100) // Extra space for safety
		size, err := archive.DecompressAllChecked(buffer)
		if err != nil {
			t.Fatalf("DecompressAllChecked failed: %v", err)
		}
		actual := string(buffer[:size])

		if actual != expected {
			t.Errorf("DecompressAllChecked: expected %q, got %q", expected, actual)
		}

		if size != totalLen {
			t.Errorf("DecompressAllChecked size: expected %d, got %d", totalLen, size)
		}
	})
}

// ============================================================================
// Basic Benchmarks
// ============================================================================

func BenchmarkOnPairCompression(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc := NewEncoder()
		mustEncode(enc, strings)
	}
}

func BenchmarkOnPair16Compression(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc := NewEncoder(WithMaxTokenLength(16))
		mustEncode(enc, strings)
	}
}

func BenchmarkOnPairDecompression(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)
	buffer := make([]byte, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(strings); j++ {
			if _, err := archive.DecompressString(j, buffer); err != nil {
				b.Fatalf("DecompressString failed: %v", err)
			}
		}
	}
}

func BenchmarkOnPair16Decompression(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, strings)
	buffer := make([]byte, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(strings); j++ {
			if _, err := archive.DecompressString(j, buffer); err != nil {
				b.Fatalf("DecompressString failed: %v", err)
			}
		}
	}
}

func BenchmarkOnPairLargeDatasetCompression(b *testing.B) {
	strings := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		strings[i] = "user_" + fmt.Sprintf("%06d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc := NewEncoder()
		mustEncode(enc, strings)
	}
}

func BenchmarkOnPairLargeDatasetDecompression(b *testing.B) {
	strings := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		strings[i] = "user_" + fmt.Sprintf("%06d", i)
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)
	buffer := make([]byte, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % len(strings)
		if _, err := archive.DecompressString(idx, buffer); err != nil {
			b.Fatalf("DecompressString failed: %v", err)
		}
	}
}

func BenchmarkOnPairDecompressAllChecked(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}
	buffer := make([]byte, totalLen+100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := archive.DecompressAllChecked(buffer); err != nil {
			b.Fatalf("DecompressAllChecked failed: %v", err)
		}
	}
}

// ============================================================================
// Testdata Benchmarks
// ============================================================================

func BenchmarkTestdataCompression(b *testing.B) {
	testdataDir := "testdata"

	files, err := os.ReadDir(testdataDir)
	if err != nil {
		b.Fatalf("Failed to read testdata directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		filepath := filepath.Join(testdataDir, filename)

		data, err := os.ReadFile(filepath)
		if err != nil {
			b.Fatalf("Failed to read %s: %v", filename, err)
		}

		content := string(data)
		lines := strings.Split(content, "\n")

		b.Run(filename+"/OnPair/compress", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				enc := NewEncoder()
				mustEncode(enc, lines)
			}
		})

		b.Run(filename+"/OnPair16/compress", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				enc := NewEncoder(WithMaxTokenLength(16))
				mustEncode(enc, lines)
			}
		})

		// Decompress benchmarks
		enc := NewEncoder()
		archive := mustEncode(enc, lines)
		buffer := make([]byte, len(data)+16)

		b.Run(filename+"/OnPair/decompress", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := archive.DecompressAllChecked(buffer); err != nil {
					b.Fatalf("DecompressAllChecked failed: %v", err)
				}
			}
		})

		enc16 := NewEncoder(WithMaxTokenLength(16))
		archive16 := mustEncode(enc16, lines)

		b.Run(filename+"/OnPair16/decompress", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := archive16.DecompressAllChecked(buffer); err != nil {
					b.Fatalf("DecompressAllChecked failed: %v", err)
				}
			}
		})
	}
}

// ============================================================================
// Comparison Benchmarks (FSST testdata)
// ============================================================================

func BenchmarkOnPairWithFSSTTestData(b *testing.B) {
	testFiles := []string{
		"testdata/art_of_war.txt",
		"testdata/logs_apache_2k.log",
		"testdata/logs_hdfs_2k.log",
		"testdata/zh_tao_te_ching_en.txt",
	}

	for _, testFile := range testFiles {
		lines, err := loadTestDataLines(testFile)
		if err != nil {
			b.Skipf("Failed to load %s: %v", testFile, err)
			continue
		}

		if len(lines) == 0 {
			b.Skipf("No lines in %s", testFile)
			continue
		}

		// Get base filename for reporting
		parts := strings.Split(testFile, "/")
		name := parts[len(parts)-1]

		b.Run(name, func(b *testing.B) {
			// Calculate original size
			originalSize := 0
			for _, line := range lines {
				originalSize += len(line)
			}

			b.Run("OnPair/compress", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(originalSize))
				b.ResetTimer()

				var archive *Archive
				for i := 0; i < b.N; i++ {
					enc := NewEncoder()
					archive = mustEncode(enc, lines)
				}

				// Report compression ratio
				if archive != nil {
					compressedSize := archive.SpaceUsed()
					ratio := float64(originalSize) / float64(compressedSize)
					b.ReportMetric(ratio, "ratio")
					b.ReportMetric(float64(compressedSize), "compressed_bytes")
				}
			})

			b.Run("OnPair/decompress", func(b *testing.B) {
				enc := NewEncoder()
				archive := mustEncode(enc, lines)
				buffer := make([]byte, 4096)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					for j := 0; j < len(lines); j++ {
						if _, err := archive.DecompressString(j, buffer); err != nil {
							b.Fatalf("DecompressString failed: %v", err)
						}
					}
				}
			})

			b.Run("OnPair16/compress", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(originalSize))
				b.ResetTimer()

				var archive16 *Archive
				for i := 0; i < b.N; i++ {
					enc16 := NewEncoder(WithMaxTokenLength(16))
					archive16 = mustEncode(enc16, lines)
				}

				// Report compression ratio
				if archive16 != nil {
					compressedSize := archive16.SpaceUsed()
					ratio := float64(originalSize) / float64(compressedSize)
					b.ReportMetric(ratio, "ratio")
					b.ReportMetric(float64(compressedSize), "compressed_bytes")
				}
			})

			b.Run("OnPair16/decompress", func(b *testing.B) {
				enc16 := NewEncoder(WithMaxTokenLength(16))
				archive16 := mustEncode(enc16, lines)
				buffer := make([]byte, 4096)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					for j := 0; j < len(lines); j++ {
						if _, err := archive16.DecompressString(j, buffer); err != nil {
							b.Fatalf("DecompressString failed: %v", err)
						}
					}
				}
			})
		})
	}
}

func BenchmarkOnPairWithLargeFiles(b *testing.B) {
	largeFiles := []string{
		"testdata/en_bible_kjv.txt",
		"testdata/en_shakespeare.txt",
		"testdata/en_mobydick.txt",
	}

	for _, testFile := range largeFiles {
		lines, err := loadTestDataLines(testFile)
		if err != nil {
			b.Skipf("Failed to load %s: %v", testFile, err)
			continue
		}

		// Limit to first 10K lines for reasonable benchmark time
		if len(lines) > 10000 {
			lines = lines[:10000]
		}

		parts := strings.Split(testFile, "/")
		name := parts[len(parts)-1]

		b.Run(name, func(b *testing.B) {
			originalSize := 0
			for _, line := range lines {
				originalSize += len(line)
			}

			b.Run("OnPair/compress", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(originalSize))
				b.ResetTimer()

				var archive *Archive
				for i := 0; i < b.N; i++ {
					enc := NewEncoder()
					archive = mustEncode(enc, lines)
				}
				if archive != nil {
					compressedSize := archive.SpaceUsed()
					ratio := float64(originalSize) / float64(compressedSize)
					b.ReportMetric(ratio, "ratio")
				}
			})

			b.Run("OnPair16/compress", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(originalSize))
				b.ResetTimer()

				var archive16 *Archive
				for i := 0; i < b.N; i++ {
					enc16 := NewEncoder(WithMaxTokenLength(16))
					archive16 = mustEncode(enc16, lines)
				}
				if archive16 != nil {
					compressedSize := archive16.SpaceUsed()
					ratio := float64(originalSize) / float64(compressedSize)
					b.ReportMetric(ratio, "ratio")
				}
			})
		})
	}
}

const maxFuzzInputBytes = 8 * 1024

func verifyArchiveRoundTrip(t *testing.T, archive *Archive, rows []string) {
	t.Helper()

	if archive.Rows() != len(rows) {
		t.Fatalf("Rows mismatch: got %d want %d", archive.Rows(), len(rows))
	}

	expectedAll := strings.Join(rows, "")

	for i, want := range rows {
		gotLen, err := archive.DecodedLen(i)
		if err != nil {
			t.Fatalf("DecodedLen(%d) failed: %v", i, err)
		}
		if gotLen != len(want) {
			t.Fatalf("DecodedLen(%d): got %d want %d", i, gotLen, len(want))
		}

		gotAppend, err := archive.AppendRow(nil, i)
		if err != nil {
			t.Fatalf("AppendRow(%d) failed: %v", i, err)
		}
		if string(gotAppend) != want {
			t.Fatalf("AppendRow(%d) mismatch: got %q want %q", i, string(gotAppend), want)
		}

		buf := make([]byte, len(want))
		n, err := archive.DecompressString(i, buf)
		if err != nil {
			t.Fatalf("DecompressString(%d) failed: %v", i, err)
		}
		if n != len(want) {
			t.Fatalf("DecompressString(%d) size mismatch: got %d want %d", i, n, len(want))
		}
		if string(buf[:n]) != want {
			t.Fatalf("DecompressString(%d) mismatch: got %q want %q", i, string(buf[:n]), want)
		}

		if len(want) > 0 {
			_, err = archive.DecompressString(i, make([]byte, len(want)-1))
			if !errors.Is(err, ErrShortBuffer) {
				t.Fatalf("DecompressString(%d) expected ErrShortBuffer, got %v", i, err)
			}
		}
	}

	all, err := archive.AppendAll(nil)
	if err != nil {
		t.Fatalf("AppendAll failed: %v", err)
	}
	if string(all) != expectedAll {
		t.Fatalf("AppendAll mismatch: got %q want %q", string(all), expectedAll)
	}

	allBuf := make([]byte, len(expectedAll))
	n, err := archive.DecompressAllChecked(allBuf)
	if err != nil {
		t.Fatalf("DecompressAllChecked failed: %v", err)
	}
	if n != len(expectedAll) {
		t.Fatalf("DecompressAllChecked size mismatch: got %d want %d", n, len(expectedAll))
	}
	if string(allBuf[:n]) != expectedAll {
		t.Fatalf("DecompressAllChecked mismatch: got %q want %q", string(allBuf[:n]), expectedAll)
	}

	if len(expectedAll) > 0 {
		_, err = archive.DecompressAllChecked(make([]byte, len(expectedAll)-1))
		if !errors.Is(err, ErrShortBuffer) {
			t.Fatalf("DecompressAllChecked expected ErrShortBuffer, got %v", err)
		}
	}

	var blob bytes.Buffer
	if _, err := archive.WriteTo(&blob); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	var loaded Archive
	if _, err := loaded.ReadFrom(bytes.NewReader(blob.Bytes())); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	loadedAll, err := loaded.AppendAll(nil)
	if err != nil {
		t.Fatalf("AppendAll after ReadFrom failed: %v", err)
	}
	if string(loadedAll) != expectedAll {
		t.Fatalf("round-trip mismatch: got %q want %q", string(loadedAll), expectedAll)
	}
}

func limitFuzzSize(total int) bool {
	return total > maxFuzzInputBytes
}

func FuzzArchiveRoundTrip(f *testing.F) {
	f.Add([]byte("hello"), []byte("world"), []byte("user_000001"))
	f.Add([]byte(""), []byte(""), []byte(""))
	f.Add([]byte("null\x00byte"), []byte("tab\there"), []byte("🚀rocket"))
	f.Add([]byte("aaaaaaaaaaaa"), []byte("bbbbbbbbbbbb"), []byte("cccccccccccc"))

	f.Fuzz(func(t *testing.T, a, b, c []byte) {
		total := len(a) + len(b) + len(c)
		if limitFuzzSize(total) {
			t.Skip()
		}

		rows := []string{
			string(a),
			string(b),
			string(c),
			string(a) + string(b),
			string(b) + string(c),
			string(c) + string(a),
		}

		cases := []struct {
			name string
			opts []Option
		}{
			{name: "default"},
			{name: "maxlen16", opts: []Option{WithMaxTokenLength(16)}},
			{name: "maxid4095", opts: []Option{WithMaxTokenID(4095)}},
			{name: "bitwidth12", opts: []Option{WithTokenBitWidth(12)}},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				archive := mustEncode(NewEncoder(tc.opts...), rows)
				verifyArchiveRoundTrip(t, archive, rows)
			})
		}
	})
}

func FuzzModelLifecycle(f *testing.F) {
	f.Add([]byte("user_"), []byte("0001"), []byte("event"))
	f.Add([]byte(""), []byte(""), []byte(""))
	f.Add([]byte("prefix"), []byte("_suffix"), []byte("payload"))

	f.Fuzz(func(t *testing.T, trainA, trainB, query []byte) {
		total := len(trainA) + len(trainB) + len(query)
		if limitFuzzSize(total) {
			t.Skip()
		}

		trainRows := []string{
			string(trainA),
			string(trainB),
			string(trainA) + string(trainB),
			string(trainA),
		}
		queryRows := []string{
			string(query),
			string(trainA),
			string(query) + string(trainB),
			string(query),
		}

		model, err := TrainModel(trainRows, WithMaxTokenLength(16))
		if err != nil {
			t.Fatalf("TrainModel failed: %v", err)
		}
		if !model.Trained() {
			t.Fatalf("model should be trained")
		}

		archive1, err := model.Encode(queryRows)
		if err != nil {
			t.Fatalf("first model encode failed: %v", err)
		}
		archive2, err := model.Encode(queryRows)
		if err != nil {
			t.Fatalf("second model encode failed: %v", err)
		}

		if !slices.Equal(archive1.CompressedData, archive2.CompressedData) {
			t.Fatalf("non-deterministic compressed data")
		}
		if !slices.Equal(archive1.StringBoundaries, archive2.StringBoundaries) {
			t.Fatalf("non-deterministic string boundaries")
		}
		if !slices.Equal(archive1.Dictionary, archive2.Dictionary) {
			t.Fatalf("non-deterministic dictionary")
		}
		if !slices.Equal(archive1.TokenBoundaries, archive2.TokenBoundaries) {
			t.Fatalf("non-deterministic token boundaries")
		}

		verifyArchiveRoundTrip(t, archive1, queryRows)
	})
}

func FuzzArchiveCorruptionPaths(f *testing.F) {
	f.Add(uint8(0), uint16(0), uint32(0))
	f.Add(uint8(1), uint16(4), uint32(1024))
	f.Add(uint8(2), uint16(8), uint32(1<<31))

	f.Fuzz(func(t *testing.T, op uint8, idx uint16, value uint32) {
		base := mustEncode(NewEncoder(), []string{
			"alpha",
			"beta",
			"gamma",
			"delta",
		})

		archive := &Archive{
			CompressedData:   append([]uint16(nil), base.CompressedData...),
			StringBoundaries: append([]int(nil), base.StringBoundaries...),
			Dictionary:       append([]byte(nil), base.Dictionary...),
			TokenBoundaries:  append([]uint32(nil), base.TokenBoundaries...),
		}

		switch op % 6 {
		case 0:
			if len(archive.CompressedData) > 0 {
				archive.CompressedData[int(idx)%len(archive.CompressedData)] = uint16(value)
			}
		case 1:
			if len(archive.TokenBoundaries) > 0 {
				archive.TokenBoundaries[int(idx)%len(archive.TokenBoundaries)] = value
			}
		case 2:
			if len(archive.StringBoundaries) > 0 {
				archive.StringBoundaries[int(idx)%len(archive.StringBoundaries)] = int(value)
			}
		case 3:
			if len(archive.Dictionary) > 0 {
				archive.Dictionary[int(idx)%len(archive.Dictionary)] = byte(value)
			}
		case 4:
			if len(archive.StringBoundaries) > 2 {
				i := int(idx)%(len(archive.StringBoundaries)-1) + 1
				archive.StringBoundaries[i-1], archive.StringBoundaries[i] = archive.StringBoundaries[i], archive.StringBoundaries[i-1]
			}
		case 5:
			if len(archive.TokenBoundaries) > 1 {
				archive.TokenBoundaries = archive.TokenBoundaries[:len(archive.TokenBoundaries)-1]
			}
		}

		_, _ = archive.DecodedLen(0)
		_, _ = archive.AppendRow(nil, 0)
		_, _ = archive.AppendAll(nil)
		_, _ = archive.DecompressString(0, make([]byte, 64))
		_, _ = archive.DecompressAllChecked(make([]byte, 64))
	})
}

type benchmarkDataset struct {
	name      string
	rows      []string
	totalSize int
	maxRowLen int
}

func newBenchmarkDataset(name string, rows []string) benchmarkDataset {
	totalSize := 0
	maxRowLen := 0
	for _, row := range rows {
		l := len(row)
		totalSize += l
		if l > maxRowLen {
			maxRowLen = l
		}
	}
	return benchmarkDataset{
		name:      name,
		rows:      rows,
		totalSize: totalSize,
		maxRowLen: maxRowLen,
	}
}

func makeSyntheticIDRows(n int) []string {
	rows := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, fmt.Sprintf("tenant=%02d|user_%06d|region=us-east-1", i%32, i%4000))
	}
	return rows
}

func makeSyntheticMixedRows(n int) []string {
	rows := make([]string, 0, n)
	for i := 0; i < n; i++ {
		switch i % 6 {
		case 0:
			rows = append(rows, fmt.Sprintf("GET /api/v1/items/%d HTTP/1.1", i%300))
		case 1:
			rows = append(rows, fmt.Sprintf("user_%06d", i%5000))
		case 2:
			rows = append(rows, fmt.Sprintf("device:%04x event:click slot:%02d", i%65535, i%24))
		case 3:
			rows = append(rows, fmt.Sprintf("lat=%.4f lon=%.4f", float64(i%90), float64(i%180)))
		case 4:
			rows = append(rows, fmt.Sprintf("prefix_%d_suffix_%d", i%1000, (i*7)%1000))
		default:
			rows = append(rows, fmt.Sprintf("service=auth code=%d", 200+(i%5)))
		}
	}
	return rows
}

func loadBenchmarkDataset(path string, maxRows int) (benchmarkDataset, error) {
	rows, err := loadTestDataLines(path)
	if err != nil {
		return benchmarkDataset{}, err
	}
	if maxRows > 0 && len(rows) > maxRows {
		rows = rows[:maxRows]
	}
	return newBenchmarkDataset(filepath.Base(path), rows), nil
}

func benchmarkDatasets(b *testing.B) []benchmarkDataset {
	datasets := []benchmarkDataset{
		newBenchmarkDataset("synthetic_ids_4k", makeSyntheticIDRows(4000)),
		newBenchmarkDataset("synthetic_mixed_8k", makeSyntheticMixedRows(8000)),
	}

	testdataFiles := []string{
		"testdata/logs_apache_2k.log",
		"testdata/art_of_war.txt",
		"testdata/en_shakespeare.txt",
	}
	for _, path := range testdataFiles {
		ds, err := loadBenchmarkDataset(path, 5000)
		if err != nil {
			b.Logf("skipping %s: %v", path, err)
			continue
		}
		datasets = append(datasets, ds)
	}

	return datasets
}

func BenchmarkAPIThorough(b *testing.B) {
	for _, ds := range benchmarkDatasets(b) {
		ds := ds
		if len(ds.rows) == 0 || ds.totalSize == 0 {
			continue
		}

		b.Run(ds.name, func(b *testing.B) {
			avgRowLen := ds.totalSize / len(ds.rows)
			if avgRowLen == 0 {
				avgRowLen = 1
			}

			b.Run("encode/default", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(ds.totalSize))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = mustEncode(NewEncoder(), ds.rows)
				}
			})

			b.Run("encode/maxlen16", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(ds.totalSize))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = mustEncode(NewEncoder(WithMaxTokenLength(16)), ds.rows)
				}
			})

			b.Run("encode/maxid4095", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(ds.totalSize))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = mustEncode(NewEncoder(WithMaxTokenID(4095)), ds.rows)
				}
			})

			b.Run("model/train", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(ds.totalSize))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := TrainModel(ds.rows, WithMaxTokenLength(16))
					if err != nil {
						b.Fatalf("TrainModel failed: %v", err)
					}
				}
			})

			b.Run("model/encode_reuse", func(b *testing.B) {
				model, err := TrainModel(ds.rows, WithMaxTokenLength(16))
				if err != nil {
					b.Fatalf("TrainModel failed: %v", err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(ds.totalSize))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := model.Encode(ds.rows); err != nil {
						b.Fatalf("Encode failed: %v", err)
					}
				}
			})

			archive := mustEncode(NewEncoder(), ds.rows)

			b.Run("decode/decompress_string", func(b *testing.B) {
				buf := make([]byte, ds.maxRowLen)
				b.ReportAllocs()
				b.SetBytes(int64(avgRowLen))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx := i % len(ds.rows)
					if _, err := archive.DecompressString(idx, buf); err != nil {
						b.Fatalf("DecompressString failed: %v", err)
					}
				}
			})

			b.Run("decode/append_row", func(b *testing.B) {
				dst := make([]byte, 0, ds.maxRowLen)
				b.ReportAllocs()
				b.SetBytes(int64(avgRowLen))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					idx := i % len(ds.rows)
					var err error
					dst = dst[:0]
					dst, err = archive.AppendRow(dst, idx)
					if err != nil {
						b.Fatalf("AppendRow failed: %v", err)
					}
				}
			})

			b.Run("decode/all_checked", func(b *testing.B) {
				buf := make([]byte, ds.totalSize)
				b.ReportAllocs()
				b.SetBytes(int64(ds.totalSize))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := archive.DecompressAllChecked(buf); err != nil {
						b.Fatalf("DecompressAllChecked failed: %v", err)
					}
				}
			})

			b.Run("decode/append_all", func(b *testing.B) {
				dst := make([]byte, 0, ds.totalSize)
				b.ReportAllocs()
				b.SetBytes(int64(ds.totalSize))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var err error
					dst = dst[:0]
					dst, err = archive.AppendAll(dst)
					if err != nil {
						b.Fatalf("AppendAll failed: %v", err)
					}
				}
			})

			b.Run("serialize/write", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(archive.SpaceUsed()))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := archive.WriteTo(io.Discard); err != nil {
						b.Fatalf("WriteTo failed: %v", err)
					}
				}
			})

			var blob bytes.Buffer
			if _, err := archive.WriteTo(&blob); err != nil {
				b.Fatalf("prepare blob failed: %v", err)
			}
			encoded := blob.Bytes()

			b.Run("serialize/read", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(encoded)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var loaded Archive
					if _, err := loaded.ReadFrom(bytes.NewReader(encoded)); err != nil {
						b.Fatalf("ReadFrom failed: %v", err)
					}
				}
			})

			b.Run("integrity/roundtrip_all", func(b *testing.B) {
				expected := strings.Join(ds.rows, "")
				buf := make([]byte, len(expected))
				b.ReportAllocs()
				b.SetBytes(int64(ds.totalSize))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					n, err := archive.DecompressAllChecked(buf)
					if err != nil {
						b.Fatalf("DecompressAllChecked failed: %v", err)
					}
					if n != len(expected) {
						b.Fatalf("size mismatch: got %d want %d", n, len(expected))
					}
				}
			})
		})
	}
}
