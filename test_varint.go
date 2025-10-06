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

	// Compress with variable-length encoding
	op := compressor.New()
	op.CompressStrings(lines)

	ratio := float64(len(data)) / float64(op.SpaceUsed())
	
	compData := op.GetCompressedData()
	dict := op.GetDictionary()
	bounds := op.GetTokenBoundaries()
	
	fmt.Printf("Variable-length encoding:\n")
	fmt.Printf("  Tokens: %d\n", len(bounds)-1)
	fmt.Printf("  Compressed data: %d bytes (variable-length token IDs)\n", len(compData))
	fmt.Printf("  Dictionary: %d bytes\n", len(dict))
	fmt.Printf("  Token boundaries: %d bytes (%d entries × 4)\n", len(bounds)*4, len(bounds))
	fmt.Printf("  Total: %d bytes\n", op.SpaceUsed())
	fmt.Printf("  Ratio: %.2fx\n\n", ratio)
	
	// Show breakdown
	if ratio >= 1.0 {
		fmt.Printf("SUCCESS: Data compressed to %.1f%% of original size!\n", 100.0 / ratio)
	} else {
		fmt.Printf("Data expanded to %.1f%% of original size\n", 100.0 * ratio)
	}
}
