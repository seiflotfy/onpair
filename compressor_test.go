package onpair

import (
	"fmt"
	"sync"
	"testing"

	"github.com/seif/onpair/compressor"
)

func TestOnPairBasicCompression(t *testing.T) {
	strings := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	// Verify decompression
	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair.DecompressString(i, buffer)
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

	onpair16 := compressor.New16()
	onpair16.CompressStrings(strings)

	// Verify decompression
	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair16.DecompressString(i, buffer)
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("String %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairEmptyStrings(t *testing.T) {
	strings := []string{"", "test", "", "data"}

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair.DecompressString(i, buffer)
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("String %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPair16EmptyStrings(t *testing.T) {
	strings := []string{"", "test", "", "data"}

	onpair16 := compressor.New16()
	onpair16.CompressStrings(strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair16.DecompressString(i, buffer)
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

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair.DecompressString(i, buffer)
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("String %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairDecompressAll(t *testing.T) {
	strings := []string{
		"hello",
		"world",
		"test",
	}

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	// Calculate expected total length
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}

	buffer := make([]byte, totalLen+100) // Extra space for safety
	size := onpair.DecompressAll(buffer)

	expected := "helloworldtest"
	actual := string(buffer[:size])

	if actual != expected {
		t.Errorf("DecompressAll: expected %q, got %q", expected, actual)
	}

	if size != totalLen {
		t.Errorf("DecompressAll size: expected %d, got %d", totalLen, size)
	}
}

func TestOnPair16DecompressAll(t *testing.T) {
	strings := []string{
		"hello",
		"world",
		"test",
	}

	onpair16 := compressor.New16()
	onpair16.CompressStrings(strings)

	// Calculate expected total length
	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}

	buffer := make([]byte, totalLen+100) // Extra space for safety
	size := onpair16.DecompressAll(buffer)

	expected := "helloworldtest"
	actual := string(buffer[:size])

	if actual != expected {
		t.Errorf("DecompressAll: expected %q, got %q", expected, actual)
	}

	if size != totalLen {
		t.Errorf("DecompressAll size: expected %d, got %d", totalLen, size)
	}
}

func TestOnPairSpaceUsed(t *testing.T) {
	strings := []string{
		"user_000001",
		"user_000002",
		"user_000003",
	}

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	space := onpair.SpaceUsed()
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

func BenchmarkOnPairCompression(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		onpair := compressor.New()
		onpair.CompressStrings(strings)
	}
}

func BenchmarkOnPair16Compression(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		onpair16 := compressor.New16()
		onpair16.CompressStrings(strings)
	}
}

func BenchmarkOnPairDecompression(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	onpair := compressor.New()
	onpair.CompressStrings(strings)
	buffer := make([]byte, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(strings); j++ {
			onpair.DecompressString(j, buffer)
		}
	}
}

func BenchmarkOnPair16Decompression(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	onpair16 := compressor.New16()
	onpair16.CompressStrings(strings)
	buffer := make([]byte, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(strings); j++ {
			onpair16.DecompressString(j, buffer)
		}
	}
}

// Large dataset tests
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

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	// Verify random samples
	buffer := make([]byte, 256)
	samples := []int{0, 100, 1000, 10000, 50000, 99999}
	for _, idx := range samples {
		size := onpair.DecompressString(idx, buffer)
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

	onpair16 := compressor.New16()
	onpair16.CompressStrings(strings)

	// Verify random samples
	buffer := make([]byte, 256)
	samples := []int{0, 1000, 50000, 99999}
	for _, idx := range samples {
		size := onpair16.DecompressString(idx, buffer)
		actual := string(buffer[:size])
		if actual != strings[idx] {
			t.Errorf("String %d: expected %q, got %q", idx, strings[idx], actual)
		}
	}
}

// Compression ratio tests
func TestOnPairCompressionRatio(t *testing.T) {
	// Test with highly repetitive data
	strings := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		strings[i] = "user_000001" // Same string repeated
	}

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	originalSize := len(strings) * len(strings[0])
	compressedSize := onpair.SpaceUsed()
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

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	originalSize := 0
	for _, s := range strings {
		originalSize += len(s)
	}
	compressedSize := onpair.SpaceUsed()
	ratio := float64(originalSize) / float64(compressedSize)

	t.Logf("Varied data - Original: %d bytes, Compressed: %d bytes, Ratio: %.2fx",
		originalSize, compressedSize, ratio)
}

// Edge case tests
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

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair.DecompressString(i, buffer)
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

	onpair16 := compressor.New16()
	onpair16.CompressStrings(strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair16.DecompressString(i, buffer)
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

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	buffer := make([]byte, 1024)
	for i, expected := range strings {
		size := onpair.DecompressString(i, buffer)
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

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair.DecompressString(i, buffer)
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("Special char string %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairSingleByteStrings(t *testing.T) {
	strings := []string{"a", "b", "c", "a", "b", "a"}

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair.DecompressString(i, buffer)
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("Single byte string %d: expected %q, got %q", i, expected, actual)
		}
	}
}

func TestOnPairMaxTokenLength(t *testing.T) {
	// Test OnPair with patterns that could create very long tokens
	baseStr := "abcdefghijklmnopqrstuvwxyz0123456789"
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = baseStr // Same long string repeated
	}

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	buffer := make([]byte, 256)
	for i, expected := range strings {
		size := onpair.DecompressString(i, buffer)
		actual := string(buffer[:size])
		if actual != expected {
			t.Errorf("Max token string %d: expected %q, got %q", i, expected, actual)
		}
	}
}

// Concurrency tests
func TestOnPairConcurrentDecompression(t *testing.T) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_" + fmt.Sprintf("%04d", i)
	}

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	// Decompress concurrently from multiple goroutines
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			buffer := make([]byte, 256)

			for i := 0; i < len(strings); i++ {
				size := onpair.DecompressString(i, buffer)
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

	onpair16 := compressor.New16()
	onpair16.CompressStrings(strings)

	// Decompress concurrently
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			buffer := make([]byte, 256)

			for i := 0; i < len(strings); i++ {
				size := onpair16.DecompressString(i, buffer)
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

// Benchmarks for large datasets
func BenchmarkOnPairLargeDatasetCompression(b *testing.B) {
	strings := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		strings[i] = "user_" + fmt.Sprintf("%06d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		onpair := compressor.New()
		onpair.CompressStrings(strings)
	}
}

func BenchmarkOnPairLargeDatasetDecompression(b *testing.B) {
	strings := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		strings[i] = "user_" + fmt.Sprintf("%06d", i)
	}

	onpair := compressor.New()
	onpair.CompressStrings(strings)
	buffer := make([]byte, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % len(strings)
		onpair.DecompressString(idx, buffer)
	}
}

func BenchmarkOnPairDecompressAll(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	onpair := compressor.New()
	onpair.CompressStrings(strings)

	totalLen := 0
	for _, s := range strings {
		totalLen += len(s)
	}
	buffer := make([]byte, totalLen+100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		onpair.DecompressAll(buffer)
	}
}
