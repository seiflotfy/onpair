package compressor

import (
	"fmt"
	"testing"
)

// TestCompressionQuality demonstrates that the new API maintains compression quality.
func TestCompressionQuality(t *testing.T) {
	// Create a realistic dataset
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = fmt.Sprintf("user_%06d@example.com", i)
	}

	// Old API
	oldOnPair := New()
	oldOnPair.CompressStrings(strings)

	// New API
	dict := TrainStrings(strings)

	// Compare compression ratios
	originalSize := 0
	for _, s := range strings {
		originalSize += len(s)
	}

	oldTokens := len(oldOnPair.GetCompressedData())
	oldSize := oldOnPair.SpaceUsed()
	oldRatio := float64(originalSize) / float64(oldSize)

	newTokens := 0
	for _, s := range strings {
		compressed := dict.Compress([]byte(s))
		newTokens += len(compressed) / 2 // Each token is 2 bytes
	}
	newSize := newTokens*2 + dict.SpaceUsed()
	newRatio := float64(originalSize) / float64(newSize)

	t.Logf("Original size: %d bytes", originalSize)
	t.Logf("Old API: %d tokens, %d bytes, %.2fx compression", oldTokens, oldSize, oldRatio)
	t.Logf("New API: %d tokens, %d bytes, %.2fx compression", newTokens, newSize, newRatio)

	// Verify compression is identical
	if oldTokens != newTokens {
		t.Errorf("Token count differs: old=%d, new=%d", oldTokens, newTokens)
	}

	// Verify compression ratio is good (at least 2x)
	if newRatio < 2.0 {
		t.Errorf("Compression ratio too low: %.2fx", newRatio)
	}

	// Verify decompression correctness
	buffer := make([]byte, 256)
	for i := 0; i < 10; i++ { // Spot check first 10 strings
		// Old API
		oldSize := oldOnPair.DecompressString(i, buffer)
		oldResult := string(buffer[:oldSize])

		// New API
		compressed := dict.Compress([]byte(strings[i]))
		newSize := dict.Decompress(compressed, buffer)
		newResult := string(buffer[:newSize])

		if oldResult != newResult || oldResult != strings[i] {
			t.Errorf("String %d mismatch: expected %q, old=%q, new=%q",
				i, strings[i], oldResult, newResult)
		}
	}
}

// BenchmarkCompressionQualityComparison compares old vs new API performance.
func BenchmarkCompressionQualityComparison(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = fmt.Sprintf("user_%06d", i)
	}

	b.Run("OldAPI_TrainAndCompress", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			onpair := New()
			onpair.CompressStrings(strings)
		}
	})

	b.Run("NewAPI_Train", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			TrainStrings(strings)
		}
	})

	dict := TrainStrings(strings)
	b.Run("NewAPI_Compress", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dict.Compress([]byte("user_000001"))
		}
	})
}
