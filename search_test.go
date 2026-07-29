package onpair

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
)

func mustSearcher(t *testing.T, rows []string, opts ...Option) (*Searcher, *Archive) {
	t.Helper()
	archive := mustEncode(NewEncoder(opts...), rows)
	s, err := archive.Searcher()
	if err != nil {
		t.Fatalf("Searcher failed: %v", err)
	}
	return s, archive
}

func decodeRow(t *testing.T, a *Archive, k int) []byte {
	t.Helper()
	row, err := a.AppendRow(nil, k)
	if err != nil {
		t.Fatalf("AppendRow(%d) failed: %v", k, err)
	}
	return row
}

// checkSearch drives all three searches for every needle and compares each
// against a brute-force decode oracle.
func checkSearch(t *testing.T, rows []string, needles []string, opts ...Option) {
	t.Helper()
	s, archive := mustSearcher(t, rows, opts...)
	decoded := make([][]byte, archive.Rows())
	for k := range decoded {
		decoded[k] = decodeRow(t, archive, k)
	}

	oracle := func(match func(row []byte) bool) []int {
		var want []int
		for k, row := range decoded {
			if match(row) {
				want = append(want, k)
			}
		}
		return want
	}

	for _, needle := range needles {
		n := []byte(needle)
		if got, want := s.RowsEqualTo(n), oracle(func(r []byte) bool { return bytes.Equal(r, n) }); !slices.Equal(got, want) {
			t.Errorf("RowsEqualTo(%q) = %v, want %v", needle, got, want)
		}
		if got, want := s.RowsStartingWith(n), oracle(func(r []byte) bool { return bytes.HasPrefix(r, n) }); !slices.Equal(got, want) {
			t.Errorf("RowsStartingWith(%q) = %v, want %v", needle, got, want)
		}
		got, err := s.RowsContaining(n)
		if err != nil {
			t.Fatalf("RowsContaining(%q) failed: %v", needle, err)
		}
		if want := oracle(func(r []byte) bool { return bytes.Contains(r, n) }); !slices.Equal(got, want) {
			t.Errorf("RowsContaining(%q) = %v, want %v", needle, got, want)
		}
	}
}

// searchCorpusURLs is repetitive enough to train rich multi-byte tokens.
func searchCorpusURLs() []string {
	rows := make([]string, 0, 200)
	for i := range 100 {
		rows = append(rows,
			"https://www.example.com/page/"+strings.Repeat("a", i%7)+string(rune('a'+i%26)),
			"ftp://files.example.org/dir"+strings.Repeat("b", i%5)+"/file",
		)
	}
	return rows
}

// searchCorpusBinary covers the full byte range, including 0x00 and 0xFF,
// via a deterministic LCG.
func searchCorpusBinary(n, maxLen int, seed uint64) []string {
	state := seed
	next := func() uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return state >> 33
	}
	rows := make([]string, n)
	for i := range rows {
		row := make([]byte, next()%uint64(maxLen+1))
		for j := range row {
			row[j] = byte(next())
		}
		rows[i] = string(row)
	}
	return rows
}

func TestSearcher_TokenizeMatchesEncoder(t *testing.T) {
	corpora := map[string][]string{
		"urls":   searchCorpusURLs(),
		"binary": searchCorpusBinary(60, 40, 13),
	}
	for name, rows := range corpora {
		for _, opts := range [][]Option{nil, {WithMaxTokenLength(16)}, {WithMaxTokenID(511)}} {
			s, archive := mustSearcher(t, rows, opts...)
			for k, row := range rows {
				codes, err := archive.rowCodes(k)
				if err != nil {
					t.Fatalf("%s: rowCodes(%d) failed: %v", name, k, err)
				}
				if got := s.Tokenize([]byte(row)); !slices.Equal(got, codes) {
					t.Fatalf("%s: Tokenize(row %d) = %v, want encoder codes %v", name, k, got, codes)
				}
			}
		}
	}
}

func TestSearcher_RowsEqualTo(t *testing.T) {
	rows := []string{"cat", "dog", "cat", "bird", "cat", "", "ab", "abc", "abcd", "ab"}
	s, _ := mustSearcher(t, rows)
	if got := s.RowsEqualTo([]byte("cat")); !slices.Equal(got, []int{0, 2, 4}) {
		t.Errorf("duplicates: got %v", got)
	}
	if got := s.RowsEqualTo([]byte("")); !slices.Equal(got, []int{5}) {
		t.Errorf("empty needle: got %v", got)
	}
	checkSearch(t, rows, []string{"cat", "dog", "ab", "abc", "abcd", "a", "abcde", "", "CAT", "zzz"})
}

func TestSearcher_RowsStartingWith(t *testing.T) {
	rows := []string{"alpha", "alpine", "beta", "al", "", "abcdef", "abcxyz", "abxxxx", "abc", "ab"}
	prefixes := []string{"", "a", "al", "alp", "alpha", "alphas", "b", "abc", "abcd", "abcx", "abz", "z"}
	checkSearch(t, rows, prefixes)
}

func TestSearcher_RowsContaining(t *testing.T) {
	rows := []string{
		"hello world", "world peace", "helloworld", "hell",
		"abcabcabc", "xabcabcy", "ababab", "cab",
		"aaaaab", "aabaab", "aaa", "",
	}
	patterns := []string{
		"", "hello", "o w", "llowo", "xyz", "hello world!",
		"abc", "bca", "cab", "bcabca", "abcabcabc", "ba",
		"aa", "aaa", "aab", "abab", "aaaa", "aabaa",
	}
	checkSearch(t, rows, patterns)
}

func TestSearcher_SearchOnTrainedCorpora(t *testing.T) {
	urls := searchCorpusURLs()
	needles := []string{"", "https", "https://www.example.com/", "ftp://", ".com", "://", "/page", "zzz", urls[0], urls[3]}
	checkSearch(t, urls, needles)
	checkSearch(t, urls, needles, WithMaxTokenLength(16))

	binary := searchCorpusBinary(50, 30, 7)
	binNeedles := []string{"", "\x00", "\xff", "\x00\x01"}
	for _, row := range binary[:10] {
		binNeedles = append(binNeedles, row, row[:len(row)/2])
	}
	checkSearch(t, binary, binNeedles)
	checkSearch(t, binary, binNeedles, WithMaxTokenLength(16))
}

func TestSearcher_SurvivesSerializationRoundTrip(t *testing.T) {
	rows := []string{"cat", "dog", "cat", "concatenate", "catalog"}
	archive := mustEncode(NewEncoder(), rows)
	var buf bytes.Buffer
	if _, err := archive.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	decoded := &Archive{}
	if _, err := decoded.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	s, err := decoded.Searcher()
	if err != nil {
		t.Fatalf("Searcher on deserialized archive failed: %v", err)
	}
	if got := s.RowsEqualTo([]byte("cat")); !slices.Equal(got, []int{0, 2}) {
		t.Errorf("RowsEqualTo = %v, want [0 2]", got)
	}
	if got := s.RowsStartingWith([]byte("cat")); !slices.Equal(got, []int{0, 2, 4}) {
		t.Errorf("RowsStartingWith = %v, want [0 2 4]", got)
	}
	got, err := s.RowsContaining([]byte("cat"))
	if err != nil {
		t.Fatalf("RowsContaining failed: %v", err)
	}
	if !slices.Equal(got, []int{0, 2, 3, 4}) {
		t.Errorf("RowsContaining = %v, want [0 2 3 4]", got)
	}
}

func TestTrainModel_DictionarySorted(t *testing.T) {
	model, err := TrainModel(searchCorpusURLs())
	if err != nil {
		t.Fatalf("TrainModel failed: %v", err)
	}
	bounds, dict := model.tokenBoundaries, model.dictionary
	singles := 0
	for i := 0; i < len(bounds)-2; i++ {
		cur := dict[bounds[i]:bounds[i+1]]
		next := dict[bounds[i+1]:bounds[i+2]]
		if bytes.Compare(cur, next) >= 0 {
			t.Fatalf("dictionary not strictly sorted at token %d: %q >= %q", i, cur, next)
		}
	}
	for i := 0; i < len(bounds)-1; i++ {
		if bounds[i+1]-bounds[i] == 1 {
			singles++
		}
	}
	if singles != 256 {
		t.Fatalf("dictionary has %d single-byte tokens, want 256", singles)
	}
}

func TestArchive_SearcherRejectsBadDictionaries(t *testing.T) {
	base := mustEncode(NewEncoder(), []string{"hello", "world"})
	cases := map[string]func(a *Archive){
		"unsorted": func(a *Archive) {
			// Swap the byte content of tokens 'a' and 'b' in place.
			ta := a.TokenBoundaries['a']
			tb := a.TokenBoundaries['b']
			a.Dictionary[ta], a.Dictionary[tb] = a.Dictionary[tb], a.Dictionary[ta]
		},
		"incomplete": func(a *Archive) {
			// Drop the last token so only 255 single-byte tokens remain.
			a.TokenBoundaries = a.TokenBoundaries[:256]
		},
	}
	for name, corrupt := range cases {
		archive := mustEncode(NewEncoder(), []string{"x"})
		archive.Dictionary = append([]byte(nil), base.Dictionary...)
		archive.TokenBoundaries = append([]uint32(nil), base.TokenBoundaries...)
		corrupt(archive)
		if _, err := archive.Searcher(); !errors.Is(err, ErrDictionaryNotSearchable) {
			t.Errorf("%s: Searcher() error = %v, want ErrDictionaryNotSearchable", name, err)
		}
	}
}

func BenchmarkSearch(b *testing.B) {
	lines, err := loadTestDataLines("testdata/logs_apache_2k.log")
	if err != nil {
		b.Skipf("testdata unavailable: %v", err)
	}
	archive := mustEncode(NewEncoder(), lines)
	s, err := archive.Searcher()
	if err != nil {
		b.Fatalf("Searcher failed: %v", err)
	}
	needle := []byte(lines[100])
	prefix := []byte(lines[100][:10])
	pattern := []byte("error")

	b.Run("Searcher", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := archive.Searcher(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Tokenize", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.Tokenize(needle)
		}
	})
	b.Run("RowsEqualTo", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.RowsEqualTo(needle)
		}
	})
	b.Run("RowsStartingWith", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s.RowsStartingWith(prefix)
		}
	})
	b.Run("RowsContaining", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := s.RowsContaining(pattern); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func FuzzSearcher(f *testing.F) {
	f.Add([]byte("hello world\nworld peace\nhelloworld"), []byte("o w"))
	f.Add([]byte("abcabcabc\nxabcabcy\nababab"), []byte("bca"))
	f.Add([]byte("aaaaab\naabaab\naaa\n"), []byte("aab"))
	f.Fuzz(func(t *testing.T, data []byte, needle []byte) {
		if len(data) > 4096 || len(needle) > 64 {
			return
		}
		rows := strings.Split(string(data), "\n")
		archive, err := NewEncoder().Encode(rows)
		if err != nil {
			t.Skip()
		}
		s, err := archive.Searcher()
		if err != nil {
			t.Fatalf("Searcher failed on fresh archive: %v", err)
		}

		var wantEq, wantPre, wantSub []int
		for k, row := range rows {
			if string(needle) == row {
				wantEq = append(wantEq, k)
			}
			if strings.HasPrefix(row, string(needle)) {
				wantPre = append(wantPre, k)
			}
			if strings.Contains(row, string(needle)) {
				wantSub = append(wantSub, k)
			}
		}
		if got := s.RowsEqualTo(needle); !slices.Equal(got, wantEq) {
			t.Errorf("RowsEqualTo(%q) = %v, want %v", needle, got, wantEq)
		}
		if got := s.RowsStartingWith(needle); !slices.Equal(got, wantPre) {
			t.Errorf("RowsStartingWith(%q) = %v, want %v", needle, got, wantPre)
		}
		got, err := s.RowsContaining(needle)
		if err != nil {
			t.Fatalf("RowsContaining failed: %v", err)
		}
		if !slices.Equal(got, wantSub) {
			t.Errorf("RowsContaining(%q) = %v, want %v", needle, got, wantSub)
		}
	})
}
