package compressor_test

import (
	"bytes"
	"fmt"

	"github.com/axiomhq/axiom/pkg/kirby/x/onpair/compressor"
)

// ExampleDictionary demonstrates the basic usage of the idiomatic OnPair API.
func ExampleDictionary() {
	// Step 1: Train a dictionary from a collection of strings
	trainingData := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}
	dict := compressor.TrainStrings(trainingData)

	// Step 2: Use the dictionary to compress individual strings
	compressed := dict.Compress([]byte("user_000001"))
	fmt.Printf("Compressed to %d bytes\n", len(compressed))

	// Step 3: Decompress back to original
	buffer := make([]byte, 100)
	n := dict.Decompress(compressed, buffer)
	fmt.Printf("Decompressed: %s\n", string(buffer[:n]))

	// Output:
	// Compressed to 8 bytes
	// Decompressed: user_000001
}

// ExampleDictionary_serialization demonstrates how to serialize and load a dictionary.
func ExampleDictionary_serialization() {
	// Train a dictionary
	trainingData := []string{"hello", "world", "hello world"}
	dict := compressor.TrainStrings(trainingData)

	// Serialize the dictionary
	var buf bytes.Buffer
	dict.WriteTo(&buf)
	fmt.Printf("Serialized dictionary: %d bytes\n", buf.Len())

	// Later, load the dictionary
	loadedDict := &compressor.Dictionary{}
	loadedDict.ReadFrom(&buf)

	// Use the loaded dictionary
	compressed := loadedDict.Compress([]byte("hello world"))
	buffer := make([]byte, 100)
	n := loadedDict.Decompress(compressed, buffer)
	fmt.Printf("Result: %s\n", string(buffer[:n]))

	// Output:
	// Serialized dictionary: 1324 bytes
	// Result: hello world
}

// ExampleDictionary_reuse demonstrates reusing a dictionary for multiple compressions.
func ExampleDictionary_reuse() {
	// Train once
	trainingData := []string{
		"prefix_001_suffix",
		"prefix_002_suffix",
		"prefix_003_suffix",
	}
	dict := compressor.TrainStrings(trainingData)

	// Compress many strings with the same dictionary
	testStrings := []string{
		"prefix_001_suffix",
		"prefix_999_suffix",
		"prefix_abc_suffix",
	}

	buffer := make([]byte, 100)
	for _, s := range testStrings {
		compressed := dict.Compress([]byte(s))
		n := dict.Decompress(compressed, buffer)
		fmt.Printf("%s -> %d bytes -> %s\n", s, len(compressed), string(buffer[:n]))
	}

	// Output:
	// prefix_001_suffix -> 12 bytes -> prefix_001_suffix
	// prefix_999_suffix -> 16 bytes -> prefix_999_suffix
	// prefix_abc_suffix -> 16 bytes -> prefix_abc_suffix
}

// ExampleDictionary16 demonstrates the 16-byte constrained version.
func ExampleDictionary16() {
	// Train a dictionary with 16-byte token constraint
	trainingData := []string{
		"user_id_12345",
		"user_id_67890",
		"admin_id_001",
	}
	dict := compressor.Train16Strings(trainingData)

	// Compress a string
	compressed := dict.Compress([]byte("user_id_12345"))
	fmt.Printf("Compressed to %d bytes\n", len(compressed))

	// Decompress
	buffer := make([]byte, 100)
	n := dict.Decompress(compressed, buffer)
	fmt.Printf("Decompressed: %s\n", string(buffer[:n]))

	// Output:
	// Compressed to 18 bytes
	// Decompressed: user_id_12345
}
