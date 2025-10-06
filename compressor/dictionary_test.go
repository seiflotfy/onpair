package compressor

import (
	"bytes"
	"testing"
)

func TestDictionaryBasic(t *testing.T) {
	// Train a dictionary
	strings := []string{"hello", "world", "hello", "test"}
	dict := TrainStrings(strings)

	// Compress individual strings
	helloCompressed := dict.Compress([]byte("hello"))
	worldCompressed := dict.Compress([]byte("world"))

	// Decompress
	buffer := make([]byte, 100)

	n := dict.Decompress(helloCompressed, buffer)
	if string(buffer[:n]) != "hello" {
		t.Errorf("Expected 'hello', got %q", string(buffer[:n]))
	}

	n = dict.Decompress(worldCompressed, buffer)
	if string(buffer[:n]) != "world" {
		t.Errorf("Expected 'world', got %q", string(buffer[:n]))
	}
}

func TestDictionary16Basic(t *testing.T) {
	// Train a dictionary
	strings := []string{"hello", "world", "hello", "test"}
	dict := Train16Strings(strings)

	// Compress individual strings
	helloCompressed := dict.Compress([]byte("hello"))
	worldCompressed := dict.Compress([]byte("world"))

	// Decompress
	buffer := make([]byte, 100)

	n := dict.Decompress(helloCompressed, buffer)
	if string(buffer[:n]) != "hello" {
		t.Errorf("Expected 'hello', got %q", string(buffer[:n]))
	}

	n = dict.Decompress(worldCompressed, buffer)
	if string(buffer[:n]) != "world" {
		t.Errorf("Expected 'world', got %q", string(buffer[:n]))
	}
}

func TestDictionarySerialization(t *testing.T) {
	// Train a dictionary
	strings := []string{"hello", "world", "hello", "test", "hello world"}
	originalDict := TrainStrings(strings)

	// Serialize
	var buf bytes.Buffer
	_, err := originalDict.WriteTo(&buf)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Deserialize
	loadedDict := &Dictionary{}
	_, err = loadedDict.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	// Test that loaded dictionary works the same
	testData := []byte("hello world")
	originalCompressed := originalDict.Compress(testData)
	loadedCompressed := loadedDict.Compress(testData)

	if len(originalCompressed) != len(loadedCompressed) {
		t.Fatalf("Compressed size mismatch: %d vs %d", len(originalCompressed), len(loadedCompressed))
	}

	for i := range originalCompressed {
		if originalCompressed[i] != loadedCompressed[i] {
			t.Errorf("Byte mismatch at index %d: %d vs %d", i, originalCompressed[i], loadedCompressed[i])
		}
	}

	// Decompress and verify
	buffer := make([]byte, 100)
	n := loadedDict.Decompress(loadedCompressed, buffer)
	if string(buffer[:n]) != string(testData) {
		t.Errorf("Decompression mismatch: expected %q, got %q", testData, buffer[:n])
	}
}

func TestDictionary16Serialization(t *testing.T) {
	// Train a dictionary
	strings := []string{"hello", "world", "hello", "test", "hello world"}
	originalDict := Train16Strings(strings)

	// Serialize
	var buf bytes.Buffer
	_, err := originalDict.WriteTo(&buf)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Deserialize
	loadedDict := &Dictionary16{}
	_, err = loadedDict.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	// Test that loaded dictionary works the same
	testData := []byte("hello world")
	originalCompressed := originalDict.Compress(testData)
	loadedCompressed := loadedDict.Compress(testData)

	if len(originalCompressed) != len(loadedCompressed) {
		t.Fatalf("Compressed size mismatch: %d vs %d", len(originalCompressed), len(loadedCompressed))
	}

	for i := range originalCompressed {
		if originalCompressed[i] != loadedCompressed[i] {
			t.Errorf("Byte mismatch at index %d: %d vs %d", i, originalCompressed[i], loadedCompressed[i])
		}
	}

	// Decompress and verify
	buffer := make([]byte, 100)
	n := loadedDict.Decompress(loadedCompressed, buffer)
	if string(buffer[:n]) != string(testData) {
		t.Errorf("Decompression mismatch: expected %q, got %q", testData, buffer[:n])
	}
}

func TestDictionaryEmptyString(t *testing.T) {
	dict := TrainStrings([]string{"hello", "world"})

	compressed := dict.Compress([]byte(""))
	if len(compressed) != 0 {
		t.Errorf("Expected 0 bytes for empty string, got %d", len(compressed))
	}

	buffer := make([]byte, 10)
	n := dict.Decompress(compressed, buffer)
	if n != 0 {
		t.Errorf("Expected 0 bytes decompressed, got %d", n)
	}
}

func TestDictionaryUnicode(t *testing.T) {
	strings := []string{
		"hello世界",
		"你好world",
		"hello世界",
		"🚀rocket",
	}

	dict := TrainStrings(strings)

	for _, s := range strings {
		compressed := dict.Compress([]byte(s))
		buffer := make([]byte, 100)
		n := dict.Decompress(compressed, buffer)

		result := string(buffer[:n])
		if result != s {
			t.Errorf("Unicode mismatch: expected %q, got %q", s, result)
		}
	}
}

func TestDictionaryReusability(t *testing.T) {
	// Train once
	trainingData := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
	}
	dict := TrainStrings(trainingData)

	// Use the dictionary to compress many strings
	testCases := []string{
		"user_000001",
		"user_999999",
		"admin_001",
		"guest_123",
	}

	buffer := make([]byte, 100)
	for _, testCase := range testCases {
		compressed := dict.Compress([]byte(testCase))
		n := dict.Decompress(compressed, buffer)

		result := string(buffer[:n])
		if result != testCase {
			t.Errorf("Reusability test failed: expected %q, got %q", testCase, result)
		}
	}
}

func BenchmarkDictionaryTrain(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TrainStrings(strings)
	}
}

func BenchmarkDictionaryCompress(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}
	dict := TrainStrings(strings)
	testData := []byte("user_000001")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dict.Compress(testData)
	}
}

func BenchmarkDictionaryDecompress(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}
	dict := TrainStrings(strings)
	testData := []byte("user_000001")
	compressed := dict.Compress(testData)
	buffer := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dict.Decompress(compressed, buffer)
	}
}

func BenchmarkDictionary16Train(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Train16Strings(strings)
	}
}

func BenchmarkDictionary16Compress(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}
	dict := Train16Strings(strings)
	testData := []byte("user_000001")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dict.Compress(testData)
	}
}

func BenchmarkDictionary16Decompress(b *testing.B) {
	strings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		strings[i] = "user_000001"
	}
	dict := Train16Strings(strings)
	testData := []byte("user_000001")
	compressed := dict.Compress(testData)
	buffer := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dict.Decompress(compressed, buffer)
	}
}
