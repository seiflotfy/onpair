package onpair

import "testing"

func benchLines(b *testing.B) []string {
	lines, err := loadTestDataLines("testdata/en_shakespeare.txt")
	if err != nil {
		b.Skip(err)
	}
	return lines
}

func BenchmarkProfTrain(b *testing.B) {
	lines := benchLines(b)
	data, _ := flattenStrings(lines)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := TrainModel(lines); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProfCompress(b *testing.B) {
	lines := benchLines(b)
	model, err := TrainModel(lines)
	if err != nil {
		b.Fatal(err)
	}
	enc := &Encoder{config: model.config}
	data, endPositions := flattenStrings(lines)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.compressRange(data, endPositions, 0, len(lines), model.matcher)
	}
}

func BenchmarkProfCompressParallel(b *testing.B) {
	lines := benchLines(b)
	model, err := TrainModel(lines)
	if err != nil {
		b.Fatal(err)
	}
	enc := NewEncoder(WithParallelism(0))
	data, endPositions := flattenStrings(lines)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.compress(data, endPositions, model.matcher)
	}
}
