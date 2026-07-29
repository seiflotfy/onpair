package onpair

import (
	"bufio"
	"bytes"
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

func TestResolveTemplateMaxClusters(t *testing.T) {
	if got := resolveTemplateMaxClusters(Config{}); got != defaultTemplateMaxClusters {
		t.Fatalf("default template max clusters: got %d want %d", got, defaultTemplateMaxClusters)
	}
	if got := resolveTemplateMaxClusters(Config{TemplateMaxClusters: 32}); got != 32 {
		t.Fatalf("custom template max clusters: got %d want %d", got, 32)
	}
}

func TestWithTemplateStratifiedSamplingOption(t *testing.T) {
	enc := NewEncoder(WithTemplateStratifiedSampling(512))
	if !enc.config.TemplateStratified {
		t.Fatalf("template stratified sampling should be enabled")
	}
	if enc.config.TemplateMaxClusters != 512 {
		t.Fatalf("template max clusters mismatch: got %d want %d", enc.config.TemplateMaxClusters, 512)
	}
}

func TestTemplateKeyFromLineNormalizesDynamicTokens(t *testing.T) {
	line := []byte(`[2025-09-12T12:00:00Z] INFO client=10.1.2.3 req=550e8400-e29b-41d4-a716-446655440000 status=500`)
	key := templateKeyFromLine(line, 16)

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

func TestStratifiedSampleIndicesByTemplateKey(t *testing.T) {
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

	sample, sampleBytes := stratifiedSampleIndicesByTemplateKey(data, endPositions, shuffled, sampleLimit, 8)
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
				testOnPairCompression(t, lines, data, WithMaxTokenLength(16))
			})
		})
	}
}

func testOnPairCompression(t *testing.T, lines []string, originalData []byte, opts ...Option) {
	// Compress
	enc := NewEncoder(opts...)
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

func limitFuzzSize(total int) bool {
	return total > maxFuzzInputBytes
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

func BenchmarkThreshold(b *testing.B) {
	data, err := os.ReadFile(filepath.Join("testdata", "logs_apache_2k.log"))
	if err != nil {
		b.Fatal(err)
	}

	b.Run("DefaultThreshold", func(b *testing.B) {
		var a *Archive
		for i := 0; i < b.N; i++ {
			enc := NewEncoder()
			a = mustEncode(enc, []string{string(data)})
		}
		b.ReportMetric(float64(len(data))/float64(a.SpaceUsed()), "ratio")
	})

	b.Run("Threshold10", func(b *testing.B) {
		var a *Archive
		for i := 0; i < b.N; i++ {
			enc := NewEncoder(WithThreshold(10))
			a = mustEncode(enc, []string{string(data)})
		}
		b.ReportMetric(float64(len(data))/float64(a.SpaceUsed()), "ratio")
	})

	b.Run("MaxTokenID4095", func(b *testing.B) {
		var a *Archive
		for i := 0; i < b.N; i++ {
			enc := NewEncoder(WithMaxTokenID(4095))
			a = mustEncode(enc, []string{string(data)})
		}
		b.ReportMetric(float64(len(data))/float64(a.SpaceUsed()), "ratio")
	})

	b.Run("Threshold10_MaxTokenID4095", func(b *testing.B) {
		var a *Archive
		for i := 0; i < b.N; i++ {
			enc := NewEncoder(WithThreshold(10), WithMaxTokenID(4095))
			a = mustEncode(enc, []string{string(data)})
		}
		b.ReportMetric(float64(len(data))/float64(a.SpaceUsed()), "ratio")
	})
}

func TestCompressParallelMatchesSerial(t *testing.T) {
	lines, err := loadTestDataLines("testdata/logs_apache_2k.log")
	if err != nil {
		t.Skipf("testdata unavailable: %v", err)
	}
	// Repeat the corpus past parallelEncodeMinBytes, mixing in empty rows.
	rows := make([]string, 0, len(lines)*8+16)
	for i := range 8 {
		rows = append(rows, lines...)
		rows = append(rows, "", lines[i])
	}
	model, err := TrainModel(rows)
	if err != nil {
		t.Fatalf("TrainModel failed: %v", err)
	}

	enc := &Encoder{config: model.config}
	data, endPositions := flattenStrings(rows)
	if len(data) < parallelEncodeMinBytes {
		t.Fatalf("corpus too small to exercise the parallel path: %d bytes", len(data))
	}
	serialCodes, serialBounds := enc.compressRange(data, endPositions, 0, len(rows), model.matcher)
	parCodes, parBounds := enc.compressParallel(data, endPositions, model.matcher, 8)

	if !slices.Equal(serialCodes, parCodes) {
		t.Fatal("parallel codes differ from serial")
	}
	if !slices.Equal(serialBounds, parBounds) {
		t.Fatal("parallel boundaries differ from serial")
	}

	// Parallelism is opt-in: default compress must not shard, and the
	// WithParallelism dispatch must still produce identical output.
	defCodes, defBounds := enc.compress(data, endPositions, model.matcher)
	parEnc := NewEncoder(WithParallelism(8))
	optCodes, optBounds := parEnc.compress(data, endPositions, model.matcher)
	if !slices.Equal(serialCodes, defCodes) || !slices.Equal(serialBounds, defBounds) {
		t.Fatal("default compress differs from serial")
	}
	if !slices.Equal(serialCodes, optCodes) || !slices.Equal(serialBounds, optBounds) {
		t.Fatal("WithParallelism compress differs from serial")
	}
}
