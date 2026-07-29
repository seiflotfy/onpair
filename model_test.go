package onpair

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

func TestModelTrainEncode(t *testing.T) {
	input := []string{
		"user_000001",
		"user_000002",
		"admin_001",
	}

	model, err := TrainModel(input, WithMaxTokenLength(16))
	if err != nil {
		t.Fatalf("TrainModel failed: %v", err)
	}
	if !model.Trained() {
		t.Fatalf("model should be trained")
	}

	archive, err := model.Encode(input)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	buf := make([]byte, 256)
	for i, want := range input {
		n, err := archive.DecompressString(i, buf)
		if err != nil {
			t.Fatalf("DecompressString(%d) failed: %v", i, err)
		}
		if got := string(buf[:n]); got != want {
			t.Fatalf("row %d mismatch: got %q want %q", i, got, want)
		}
	}
}

func TestModelEncodeWithoutTrain(t *testing.T) {
	model := NewModel()
	_, err := model.Encode([]string{"x"})
	if !errors.Is(err, ErrUntrainedModel) {
		t.Fatalf("expected ErrUntrainedModel, got %v", err)
	}
}

func FuzzModelLifecycle(f *testing.F) {
	f.Add([]byte("user_"), []byte("0001"), []byte("event"))
	f.Add([]byte(""), []byte(""), []byte(""))
	f.Add([]byte("prefix"), []byte("_suffix"), []byte("payload"))

	f.Fuzz(func(t *testing.T, trainA, trainB, query []byte) {
		total := len(trainA) + len(trainB) + len(query)
		if limitFuzzSize(total) {
			t.Skip()
		}

		trainRows := []string{
			string(trainA),
			string(trainB),
			string(trainA) + string(trainB),
			string(trainA),
		}
		queryRows := []string{
			string(query),
			string(trainA),
			string(query) + string(trainB),
			string(query),
		}

		model, err := TrainModel(trainRows, WithMaxTokenLength(16))
		if err != nil {
			t.Fatalf("TrainModel failed: %v", err)
		}
		if !model.Trained() {
			t.Fatalf("model should be trained")
		}

		archive1, err := model.Encode(queryRows)
		if err != nil {
			t.Fatalf("first model encode failed: %v", err)
		}
		archive2, err := model.Encode(queryRows)
		if err != nil {
			t.Fatalf("second model encode failed: %v", err)
		}

		if !slices.Equal(archive1.CompressedData, archive2.CompressedData) {
			t.Fatalf("non-deterministic compressed data")
		}
		if !slices.Equal(archive1.StringBoundaries, archive2.StringBoundaries) {
			t.Fatalf("non-deterministic string boundaries")
		}
		if !slices.Equal(archive1.Dictionary, archive2.Dictionary) {
			t.Fatalf("non-deterministic dictionary")
		}
		if !slices.Equal(archive1.TokenBoundaries, archive2.TokenBoundaries) {
			t.Fatalf("non-deterministic token boundaries")
		}

		verifyArchiveRoundTrip(t, archive1, queryRows)
	})
}

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
