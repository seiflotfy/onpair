package main

import (
	"fmt"
	"os"
)

func main() {
	testFile := "testdata/art_of_war.txt"
	data, err := os.ReadFile(testFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Original file: %d bytes\n\n", len(data))

	// Variable-length encoding for single-byte tokens (0-127 use 1 byte each)
	// Most bytes in UTF-8 text are single-byte (ASCII) or multibyte but still <128
	
	// Count how many bytes have value < 128
	singleByteCount := 0
	for _, b := range data {
		if b < 128 {
			singleByteCount++
		}
	}
	doubleByteCount := len(data) - singleByteCount
	
	fmt.Printf("Byte distribution:\n")
	fmt.Printf("  Values 0-127: %d bytes (encode as 1 byte each)\n", singleByteCount)
	fmt.Printf("  Values 128-255: %d bytes (encode as 2 bytes each)\n", doubleByteCount)
	
	// Variable-length single-byte encoding
	varLengthSize := singleByteCount*1 + doubleByteCount*2 + 256 + 256*4
	fmt.Printf("\nVariable-length single-byte encoding:\n")
	fmt.Printf("  Compressed data: %d bytes\n", singleByteCount + doubleByteCount*2)
	fmt.Printf("  Dictionary: 256 bytes\n")
	fmt.Printf("  Token boundaries: %d bytes (256 × 4)\n", 256*4)
	fmt.Printf("  Total: %d bytes\n", varLengthSize)
	fmt.Printf("  Ratio: %.2fx\n", float64(len(data))/float64(varLengthSize))
}
