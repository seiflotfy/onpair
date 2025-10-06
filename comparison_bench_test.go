package onpair

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/seif/onpair/compressor"
)

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

// Benchmark OnPair with FSST testdata
func BenchmarkOnPairWithFSSTTestData(b *testing.B) {
	testFiles := []string{
		"../fsst/testdata/art_of_war.txt",
		"../fsst/testdata/logs_apache_2k.log",
		"../fsst/testdata/logs_hdfs_2k.log",
		"../fsst/testdata/zh_tao_te_ching_en.txt",
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

				var op *compressor.OnPair
				for i := 0; i < b.N; i++ {
					op = compressor.New()
					op.CompressStrings(lines)
				}

				// Report compression ratio
				if op != nil {
					compressedSize := op.SpaceUsed()
					ratio := float64(originalSize) / float64(compressedSize)
					b.ReportMetric(ratio, "ratio")
					b.ReportMetric(float64(compressedSize), "compressed_bytes")
				}
			})

			b.Run("OnPair/decompress", func(b *testing.B) {
				op := compressor.New()
				op.CompressStrings(lines)
				buffer := make([]byte, 4096)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					for j := 0; j < len(lines); j++ {
						op.DecompressString(j, buffer)
					}
				}
			})

			b.Run("OnPair16/compress", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(originalSize))
				b.ResetTimer()

				var op16 *compressor.OnPair16
				for i := 0; i < b.N; i++ {
					op16 = compressor.New16()
					op16.CompressStrings(lines)
				}

				// Report compression ratio
				if op16 != nil {
					compressedSize := op16.SpaceUsed()
					ratio := float64(originalSize) / float64(compressedSize)
					b.ReportMetric(ratio, "ratio")
					b.ReportMetric(float64(compressedSize), "compressed_bytes")
				}
			})

			b.Run("OnPair16/decompress", func(b *testing.B) {
				op16 := compressor.New16()
				op16.CompressStrings(lines)
				buffer := make([]byte, 4096)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					for j := 0; j < len(lines); j++ {
						op16.DecompressString(j, buffer)
					}
				}
			})
		})
	}
}

// Benchmark large files (Bible, Shakespeare, Moby Dick)
func BenchmarkOnPairWithLargeFiles(b *testing.B) {
	largeFiles := []string{
		"../fsst/testdata/en_bible_kjv.txt",
		"../fsst/testdata/en_shakespeare.txt",
		"../fsst/testdata/en_mobydick.txt",
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

				var op *compressor.OnPair
				for i := 0; i < b.N; i++ {
					op = compressor.New()
					op.CompressStrings(lines)
				}

				if op != nil {
					compressedSize := op.SpaceUsed()
					ratio := float64(originalSize) / float64(compressedSize)
					b.ReportMetric(ratio, "ratio")
				}
			})

			b.Run("OnPair16/compress", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(originalSize))
				b.ResetTimer()

				var op16 *compressor.OnPair16
				for i := 0; i < b.N; i++ {
					op16 = compressor.New16()
					op16.CompressStrings(lines)
				}

				if op16 != nil {
					compressedSize := op16.SpaceUsed()
					ratio := float64(originalSize) / float64(compressedSize)
					b.ReportMetric(ratio, "ratio")
				}
			})
		})
	}
}

// Test compression ratio summary
func TestCompressionRatioSummary(t *testing.T) {
	testFiles := []string{
		"../fsst/testdata/art_of_war.txt",
		"../fsst/testdata/logs_apache_2k.log",
		"../fsst/testdata/logs_hdfs_2k.log",
		"../fsst/testdata/zh_tao_te_ching_en.txt",
	}

	fmt.Println("\n=== OnPair Compression Ratio Summary ===")
	fmt.Println("Dataset                        | Original | OnPair   | Ratio | OnPair16 | Ratio")
	fmt.Println("-------------------------------|----------|----------|-------|----------|-------")

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
		op := compressor.New()
		op.CompressStrings(lines)
		opSize := op.SpaceUsed()
		opRatio := float64(originalSize) / float64(opSize)

		// OnPair16
		op16 := compressor.New16()
		op16.CompressStrings(lines)
		op16Size := op16.SpaceUsed()
		op16Ratio := float64(originalSize) / float64(op16Size)

		fmt.Printf("%-30s | %8d | %8d | %5.2fx | %8d | %5.2fx\n",
			name, originalSize, opSize, opRatio, op16Size, op16Ratio)
	}
	fmt.Println()
}
