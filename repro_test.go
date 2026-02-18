package onpair

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkThreshold(b *testing.B) {
	data, err := os.ReadFile(filepath.Join("testdata", "logs_apache_2k.log"))
	if err != nil {
		b.Fatal(err)
	}

	// Helper to run compression with a specific threshold (we'll need to hack this in or modify the code first)
	// Since we can't easily modify the code just for the benchmark without changing the API,
	// I will first modify onpair.go to accept a threshold option, but for now I'll just
	// create a copy of the trainDictionary function in this test file to simulate it.

	// Actually, to properly test this without modifying the code yet, I should probably
	// just implement the configurable threshold first as part of the plan,
	// OR I can copy the trainDictionary logic here. Copying is safer for reproduction.

	b.Run("DefaultThreshold", func(b *testing.B) {
		var a *Archive
		for i := 0; i < b.N; i++ {
			enc := NewEncoder()
			a = mustEncode(enc, []string{string(data)})
		}
		b.ReportMetric(float64(len(data))/float64(a.SpaceUsed()), "ratio")
	})

	b.Run("Threshold10", func(b *testing.B) {
		var a *Archive
		for i := 0; i < b.N; i++ {
			enc := NewEncoder(WithThreshold(10))
			a = mustEncode(enc, []string{string(data)})
		}
		b.ReportMetric(float64(len(data))/float64(a.SpaceUsed()), "ratio")
	})

	b.Run("MaxTokenID4095", func(b *testing.B) {
		var a *Archive
		for i := 0; i < b.N; i++ {
			enc := NewEncoder(WithMaxTokenID(4095))
			a = mustEncode(enc, []string{string(data)})
		}
		b.ReportMetric(float64(len(data))/float64(a.SpaceUsed()), "ratio")
	})

	b.Run("Threshold10_MaxTokenID4095", func(b *testing.B) {
		var a *Archive
		for i := 0; i < b.N; i++ {
			enc := NewEncoder(WithThreshold(10), WithMaxTokenID(4095))
			a = mustEncode(enc, []string{string(data)})
		}
		b.ReportMetric(float64(len(data))/float64(a.SpaceUsed()), "ratio")
	})
}
