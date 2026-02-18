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
