package onpair

import (
	"path/filepath"
	"testing"
)

// benchTrainEncodeDatasets lists representative files for isolated train vs
// encode benchmarks. Keep small to limit bench runtime; pick datasets that
// exercise distinct token-length distributions.
var benchTrainEncodeDatasets = []string{
	"testdata/logs_apache_2k.log",
	"testdata/logs_hdfs_2k.log",
	"testdata/en_mobydick.txt",
	"testdata/en_shakespeare.txt",
}

func benchDatasetBytes(lines []string) int64 {
	n := 0
	for _, line := range lines {
		n += len(line)
	}
	return int64(n)
}

// BenchmarkModelTrain measures Model.Train() in isolation.
func BenchmarkModelTrain(b *testing.B) {
	for _, path := range benchTrainEncodeDatasets {
		lines, err := loadTestDataLines(path)
		if err != nil || len(lines) == 0 {
			b.Logf("skip %s: %v", path, err)
			continue
		}
		name := filepath.Base(path)
		total := benchDatasetBytes(lines)

		b.Run(name+"/OnPair", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(total)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := NewModel()
				if err := m.Train(lines); err != nil {
					b.Fatalf("train: %v", err)
				}
			}
		})

		b.Run(name+"/OnPair16", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(total)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := NewModel(WithMaxTokenLength(16))
				if err := m.Train(lines); err != nil {
					b.Fatalf("train: %v", err)
				}
			}
		})
	}
}

// BenchmarkModelEncode measures Model.Encode() in isolation using a pre-trained model.
func BenchmarkModelEncode(b *testing.B) {
	for _, path := range benchTrainEncodeDatasets {
		lines, err := loadTestDataLines(path)
		if err != nil || len(lines) == 0 {
			b.Logf("skip %s: %v", path, err)
			continue
		}
		name := filepath.Base(path)
		total := benchDatasetBytes(lines)

		model, err := TrainModel(lines)
		if err != nil {
			b.Fatalf("pre-train %s: %v", path, err)
		}
		model16, err := TrainModel(lines, WithMaxTokenLength(16))
		if err != nil {
			b.Fatalf("pre-train16 %s: %v", path, err)
		}

		b.Run(name+"/OnPair", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(total)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := model.Encode(lines); err != nil {
					b.Fatalf("encode: %v", err)
				}
			}
		})

		b.Run(name+"/OnPair16", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(total)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := model16.Encode(lines); err != nil {
					b.Fatalf("encode: %v", err)
				}
			}
		})
	}
}
