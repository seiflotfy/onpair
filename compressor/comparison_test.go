package compressor

import (
	"testing"
)

// TestCompressionEquivalence verifies that the new Dictionary API produces
// the same compression results as the old OnPair API.
func TestCompressionEquivalence(t *testing.T) {
	strings := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}

	// Old API - batch compression
	oldOnPair := New()
	oldOnPair.CompressStrings(strings)
	oldSpace := oldOnPair.SpaceUsed()

	// New API - train once, compress individually
	dict := TrainStrings(strings)

	// Compress all strings and count total tokens
	newTotalTokens := 0
	for _, s := range strings {
		compressed := dict.Compress([]byte(s))
		newTotalTokens += len(compressed) / 2 // Each token is 2 bytes
	}

	// Old API token count
	oldTotalTokens := len(oldOnPair.GetCompressedData())

	t.Logf("Old API: %d tokens, %d bytes total", oldTotalTokens, oldSpace)
	t.Logf("New API: %d tokens, %d bytes (dict)", newTotalTokens, dict.SpaceUsed())
	t.Logf("Dictionary size: Old=%d, New=%d",
		len(oldOnPair.GetDictionary()), len(dict.dictionary))

	// The number of tokens should be identical
	if oldTotalTokens != newTotalTokens {
		t.Errorf("Token count mismatch: old=%d, new=%d", oldTotalTokens, newTotalTokens)
	}

	// Verify decompression produces same results
	buffer := make([]byte, 256)
	for i, expected := range strings {
		// Old API
		oldSize := oldOnPair.DecompressString(i, buffer)
		oldResult := string(buffer[:oldSize])

		// New API
		compressed := dict.Compress([]byte(expected))
		newSize := dict.Decompress(compressed, buffer)
		newResult := string(buffer[:newSize])

		if oldResult != newResult || oldResult != expected {
			t.Errorf("String %d: expected %q, old=%q, new=%q",
				i, expected, oldResult, newResult)
		}
	}
}

// TestCompressionEquivalence16 verifies Dictionary16 vs OnPair16.
// Note: OnPair16 uses non-deterministic shuffling (math/rand without seed),
// so compression may differ between runs. This test verifies correctness
// rather than exact token count equivalence.
func TestCompressionEquivalence16(t *testing.T) {
	strings := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}

	// Old API
	oldOnPair := New16()
	oldOnPair.CompressStrings(strings)
	oldSpace := oldOnPair.SpaceUsed()

	// New API
	dict := Train16Strings(strings)

	newTotalTokens := 0
	for _, s := range strings {
		compressed := dict.Compress([]byte(s))
		newTotalTokens += len(compressed) / 2 // Each token is 2 bytes
	}

	oldTotalTokens := len(oldOnPair.GetCompressedData())

	t.Logf("Old API: %d tokens, %d bytes total", oldTotalTokens, oldSpace)
	t.Logf("New API: %d tokens, %d bytes (dict)", newTotalTokens, dict.SpaceUsed())

	// Token counts may differ due to non-deterministic shuffling
	// Just verify both produce reasonable compression
	if newTotalTokens > len(strings)*20 {
		t.Errorf("New API compression seems unreasonably bad: %d tokens for %d strings",
			newTotalTokens, len(strings))
	}

	// Verify decompression
	buffer := make([]byte, 256)
	for i, expected := range strings {
		oldSize := oldOnPair.DecompressString(i, buffer)
		oldResult := string(buffer[:oldSize])

		compressed := dict.Compress([]byte(expected))
		newSize := dict.Decompress(compressed, buffer)
		newResult := string(buffer[:newSize])

		if oldResult != newResult || oldResult != expected {
			t.Errorf("String %d: expected %q, old=%q, new=%q",
				i, expected, oldResult, newResult)
		}
	}
}

// TestLargeDatasetEquivalence tests on a larger dataset.
func TestLargeDatasetEquivalence(t *testing.T) {
	// Generate 10K strings
	strings := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		if i%3 == 0 {
			strings[i] = "user_" + string(rune(i%1000))
		} else if i%3 == 1 {
			strings[i] = "admin_" + string(rune(i%1000))
		} else {
			strings[i] = "guest_" + string(rune(i%1000))
		}
	}

	// Old API
	oldOnPair := New()
	oldOnPair.CompressStrings(strings)
	oldTokens := len(oldOnPair.GetCompressedData())

	// New API
	dict := TrainStrings(strings)
	newTotalTokens := 0
	for _, s := range strings {
		compressed := dict.Compress([]byte(s))
		newTotalTokens += len(compressed) / 2 // Each token is 2 bytes
	}

	t.Logf("Large dataset: Old=%d tokens, New=%d tokens", oldTokens, newTotalTokens)

	if oldTokens != newTotalTokens {
		t.Errorf("Token count mismatch on large dataset: old=%d, new=%d",
			oldTokens, newTotalTokens)
	}
}
