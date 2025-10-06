package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/seif/onpair/compressor"
)

func main() {
	testFile := "testdata/art_of_war.txt"
	data, err := os.ReadFile(testFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	content := string(data)
	lines := strings.Split(content, "\n")

	fmt.Printf("Testing: %s (%d bytes)\n\n", testFile, len(data))

	// Single-byte only (theoretical baseline)
	// compressedData: len(data) * 2 bytes
	// dictionary: 256 bytes
	// tokenBoundaries: 256 * 4 bytes
	singleByteSize := len(data)*2 + 256 + 256*4
	fmt.Printf("Single-byte encoding only:\n")
	fmt.Printf("  Space: %d bytes\n", singleByteSize)
	fmt.Printf("  Ratio: %.2fx\n\n", float64(len(data))/float64(singleByteSize))

	// Current multi-byte approach
	op := compressor.New()
	op.CompressStrings(lines)

	ratio := float64(len(data)) / float64(op.SpaceUsed())
	fmt.Printf("Multi-byte tokens:\n")
	fmt.Printf("  Tokens: %d\n", len(op.GetTokenBoundaries())-1)
	fmt.Printf("  Space: %d bytes\n", op.SpaceUsed())
	fmt.Printf("  Ratio: %.2fx\n", ratio)
	fmt.Printf("  Improvement vs single-byte: %.1f%%\n", 
		100.0 * (1.0 - float64(op.SpaceUsed())/float64(singleByteSize)))
}
