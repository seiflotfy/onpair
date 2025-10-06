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

	// Test new multi-round
	op := compressor.New()
	op.CompressStrings(lines)

	ratio := float64(len(data)) / float64(op.SpaceUsed())
	fmt.Printf("Multi-round training:\n")
	fmt.Printf("  Tokens: %d\n", len(op.GetTokenBoundaries())-1)
	fmt.Printf("  Dict size: %d bytes\n", len(op.GetDictionary()))
	fmt.Printf("  Compressed: %d bytes\n", op.SpaceUsed())
	fmt.Printf("  Ratio: %.2fx\n", ratio)
	fmt.Println()

	// Show first 10 multi-byte tokens
	fmt.Printf("First 10 multi-byte tokens:\n")
	tokenBounds := op.GetTokenBoundaries()
	dict := op.GetDictionary()
	for i := 256; i < 266 && i < len(tokenBounds)-1; i++ {
		start := tokenBounds[i]
		end := tokenBounds[i+1]
		token := dict[start:end]
		fmt.Printf("  Token %d (len=%d): %q\n", i, len(token), string(token))
	}
}
