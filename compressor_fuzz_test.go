package onpair

import (
	"testing"

	"github.com/seif/onpair/compressor"
)

// Fuzz test for OnPair compression/decompression
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

		onpair := compressor.New()
		onpair.CompressStrings(strings)

		// Verify decompression
		buffer := make([]byte, len(input)*2+100) // Extra space for safety
		for i, expected := range strings {
			size := onpair.DecompressString(i, buffer)
			actual := string(buffer[:size])
			if actual != expected {
				t.Errorf("String %d: expected %q, got %q", i, expected, actual)
			}
		}
	})
}

// Fuzz test for OnPair16 compression/decompression
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

		onpair16 := compressor.New16()
		onpair16.CompressStrings(strings)

		// Verify decompression
		buffer := make([]byte, len(input)*2+100) // Extra space for safety
		for i, expected := range strings {
			size := onpair16.DecompressString(i, buffer)
			actual := string(buffer[:size])
			if actual != expected {
				t.Errorf("String %d: expected %q, got %q", i, expected, actual)
			}
		}
	})
}

// Fuzz test for multiple varied strings
func FuzzOnPairMultipleStrings(f *testing.F) {
	// Seed with pairs of strings
	f.Add("hello", "world")
	f.Add("user_", "admin_")
	f.Add("café", "naïve")

	f.Fuzz(func(t *testing.T, s1, s2 string) {
		// Create various combinations
		strings := []string{s1, s2, s1, s2, s1 + s2, s2 + s1}

		onpair := compressor.New()
		onpair.CompressStrings(strings)

		// Verify decompression
		maxLen := len(s1) + len(s2) + 100
		buffer := make([]byte, maxLen)
		for i, expected := range strings {
			size := onpair.DecompressString(i, buffer)
			actual := string(buffer[:size])
			if actual != expected {
				t.Errorf("String %d: expected %q, got %q", i, expected, actual)
			}
		}
	})
}

// Fuzz test for DecompressAll
func FuzzOnPairDecompressAll(f *testing.F) {
	f.Add("foo", "bar", "baz")

	f.Fuzz(func(t *testing.T, s1, s2, s3 string) {
		strings := []string{s1, s2, s3}

		onpair := compressor.New()
		onpair.CompressStrings(strings)

		// Calculate expected result
		expected := s1 + s2 + s3
		totalLen := len(expected)

		// Decompress all
		buffer := make([]byte, totalLen+100) // Extra space for safety
		size := onpair.DecompressAll(buffer)
		actual := string(buffer[:size])

		if actual != expected {
			t.Errorf("DecompressAll: expected %q, got %q", expected, actual)
		}

		if size != totalLen {
			t.Errorf("DecompressAll size: expected %d, got %d", totalLen, size)
		}
	})
}
