package onpair

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seif/onpair/compressor"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// TestAllTestdataFiles tests compression and decompression on all files in testdata/
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
	op := compressor.New()
	op.CompressStrings(lines)

	// Calculate expected decompressed size (lines without newlines)
	expectedSize := 0
	for _, line := range lines {
		expectedSize += len(line)
	}

	// Test DecompressAll
	t.Run("DecompressAll", func(t *testing.T) {
		buffer := make([]byte, len(originalData)+16) // Extra space for safety
		decompressedSize := op.DecompressAll(buffer)

		if decompressedSize != expectedSize {
			t.Errorf("DecompressAll size mismatch: got %d, want %d", decompressedSize, expectedSize)
		}

		// Reconstruct what we expect (lines joined without separators)
		var expected bytes.Buffer
		for _, line := range lines {
			expected.WriteString(line)
		}

		if !bytes.Equal(buffer[:decompressedSize], expected.Bytes()) {
			t.Errorf("DecompressAll data mismatch")
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
			size := op.DecompressString(i, buffer)
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
		compressedSize := op.SpaceUsed()
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
	// Compress
	op := compressor.New16()
	op.CompressStrings(lines)

	// Calculate expected decompressed size (lines without newlines)
	expectedSize := 0
	for _, line := range lines {
		expectedSize += len(line)
	}

	// Test DecompressAll
	t.Run("DecompressAll", func(t *testing.T) {
		buffer := make([]byte, len(originalData)+16) // Extra space for safety

		// Catch panics
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("DecompressAll panicked: %v", r)
			}
		}()

		decompressedSize := op.DecompressAll(buffer)

		if decompressedSize != expectedSize {
			t.Errorf("DecompressAll size mismatch: got %d, want %d", decompressedSize, expectedSize)
		}

		// Reconstruct what we expect (lines joined without separators)
		var expected bytes.Buffer
		for _, line := range lines {
			expected.WriteString(line)
		}

		if !bytes.Equal(buffer[:decompressedSize], expected.Bytes()) {
			t.Errorf("DecompressAll data mismatch")
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
			size := op.DecompressString(i, buffer)
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
		compressedSize := op.SpaceUsed()
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

// BenchmarkTestdataCompression benchmarks compression on testdata files
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
				op := compressor.New()
				op.CompressStrings(lines)
			}
		})

		b.Run(filename+"/OnPair16/compress", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				op := compressor.New16()
				op.CompressStrings(lines)
			}
		})

		// Decompress benchmarks
		op := compressor.New()
		op.CompressStrings(lines)
		buffer := make([]byte, len(data)+16)

		b.Run(filename+"/OnPair/decompress", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				op.DecompressAll(buffer)
			}
		})

		op16 := compressor.New16()
		op16.CompressStrings(lines)

		b.Run(filename+"/OnPair16/decompress", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				op16.DecompressAll(buffer)
			}
		})
	}
}
