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

	op := compressor.New()
	op.CompressStrings(lines)

	bounds := op.GetTokenBoundaries()
	dict := op.GetDictionary()
	
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
	
	fmt.Printf("Token breakdown:\n")
	fmt.Printf("  Single-byte tokens (0-255): 256 tokens, %d bytes dict\n", 256)
	fmt.Printf("  Multi-byte tokens (256+): %d tokens, %d bytes dict\n", multiByteTokens, multiByteSize)
	fmt.Printf("  Total dict: %d bytes\n\n", len(dict))
	
	fmt.Printf("If we don't store single-byte tokens:\n")
	fmt.Printf("  Dictionary: %d bytes (was %d, save 256)\n", multiByteSize, len(dict))
	fmt.Printf("  Token boundaries: %d bytes (was %d, save %d)\n", 
		multiByteTokens*4, len(bounds)*4, (len(bounds)-multiByteTokens)*4)
	fmt.Printf("  Compressed data: %d bytes (unchanged)\n", len(op.GetCompressedData()))
	
	newTotal := len(op.GetCompressedData()) + multiByteSize + multiByteTokens*4
	fmt.Printf("  New total: %d bytes\n", newTotal)
	fmt.Printf("  New ratio: %.2fx\n", float64(len(data))/float64(newTotal))
	
	if newTotal < len(data) {
		fmt.Printf("\n✓ SUCCESS: Compressed to %.1f%% of original!\n", 100.0*float64(newTotal)/float64(len(data)))
	}
}
