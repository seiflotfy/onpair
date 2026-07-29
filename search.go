package onpair

// Compressed-domain search: equality, prefix, and substring queries answered
// directly over the code streams, without decoding rows back to bytes.
//
// All operations require a conformant dictionary — sorted, unique, complete —
// which this package's encoder produces and (*Archive).Searcher verifies:
//
//   - Sorted: tokens in strict bytewise-lexicographic order, so a needle can
//     be tokenized and prefix-ranged by binary search.
//   - Complete: all 256 single-byte tokens present, so any query string is
//     encodable into codes.
//   - Unique: no two tokens equal, so distinct code sequences denote distinct
//     strings; equality reduces to comparing code slices.

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
)

// ErrDictionaryNotSearchable reports an archive whose dictionary does not meet
// the search preconditions: strictly sorted bytewise-lexicographic order with
// all 256 single-byte tokens present. Archives encoded by this package
// qualify; archives serialized by versions that predate dictionary sorting do
// not.
var ErrDictionaryNotSearchable = errors.New("dictionary is not sorted for search")

// Searcher answers equality, prefix, and substring queries over an archive's
// code streams without decoding rows. Construction validates the archive once
// — dictionary conformance plus structural bounds on the code stream — so the
// query methods cannot fail on malformed data. A Searcher reads the Archive it
// was built from; do not mutate the archive while using it.
type Searcher struct {
	archive     *Archive
	numTokens   int
	maxTokenLen int
}

// Searcher validates the archive for compressed-domain search.
func (a *Archive) Searcher() (*Searcher, error) {
	numTokens := len(a.TokenBoundaries) - 1
	if numTokens < singleByteTokens || numTokens > maxTokenID+1 {
		return nil, fmt.Errorf("%w: %d tokens", ErrDictionaryNotSearchable, numTokens)
	}
	bounds := a.TokenBoundaries
	if uint64(bounds[numTokens]) > uint64(len(a.Dictionary)) {
		return nil, fmt.Errorf("%w: token boundaries exceed dictionary", ErrDictionaryNotSearchable)
	}
	singles, maxLen := 0, 0
	var prev []byte
	for t := range numTokens {
		if bounds[t] > bounds[t+1] {
			return nil, fmt.Errorf("%w: token boundaries not monotonic at %d", ErrDictionaryNotSearchable, t)
		}
		tok := a.Dictionary[bounds[t]:bounds[t+1]]
		if len(tok) == 0 || (t > 0 && bytes.Compare(prev, tok) >= 0) {
			return nil, fmt.Errorf("%w: token %d out of order", ErrDictionaryNotSearchable, t)
		}
		if len(tok) == 1 {
			singles++
		}
		maxLen = max(maxLen, len(tok))
		prev = tok
	}
	if singles != singleByteTokens {
		return nil, fmt.Errorf("%w: %d single-byte tokens, need %d", ErrDictionaryNotSearchable, singles, singleByteTokens)
	}

	sb := a.StringBoundaries
	if len(sb) > 0 {
		if sb[0] != 0 {
			return nil, fmt.Errorf("corrupted string boundaries: first is %d, want 0", sb[0])
		}
		for i := 1; i < len(sb); i++ {
			if sb[i] < sb[i-1] {
				return nil, fmt.Errorf("corrupted string boundaries: not monotonic at %d", i)
			}
		}
		if sb[len(sb)-1] > len(a.CompressedData) {
			return nil, fmt.Errorf("corrupted string boundaries: exceed code stream")
		}
	}
	for i, code := range a.CompressedData {
		if int(code) >= numTokens {
			return nil, fmt.Errorf("invalid token ID at code %d: %d", i, code)
		}
	}
	return &Searcher{archive: a, numTokens: numTokens, maxTokenLen: maxLen}, nil
}

func (s *Searcher) token(t int) []byte {
	b := s.archive.TokenBoundaries
	return s.archive.Dictionary[b[t]:b[t+1]]
}

func (s *Searcher) tokenLen(t int) int {
	b := s.archive.TokenBoundaries
	return int(b[t+1] - b[t])
}

// searchRowCodes is the unchecked row-codes accessor for validated archives.
func (s *Searcher) searchRowCodes(k int) []uint16 {
	sb := s.archive.StringBoundaries
	return s.archive.CompressedData[sb[k]:sb[k+1]]
}

// tokenRange is an inclusive [begin, last] run of token ids, empty when
// begin > last. In a sorted dictionary the tokens sharing any byte-prefix form
// one such contiguous run.
type tokenRange struct{ begin, last int }

var emptyTokenRange = tokenRange{begin: 1, last: 0}

func (r tokenRange) empty() bool            { return r.begin > r.last }
func (r tokenRange) contains(t uint16) bool { return r.begin <= int(t) && int(t) <= r.last }

// narrow restricts r to the tokens whose byte at index k equals target (and
// are therefore longer than k), assuming every token in r already agrees on
// bytes 0..k. Two binary searches; tokens of length <= k sort before any token
// that has byte k, so the length guard both excludes them and keeps the byte
// access in bounds.
func (s *Searcher) narrow(r tokenRange, k int, target byte) tokenRange {
	// Lower bound: first token in range with byte k >= target.
	lo, hi := r.begin, r.last
	for lo < hi {
		mid := lo + (hi-lo)/2
		if s.tokenLen(mid) <= k || s.token(mid)[k] < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if s.tokenLen(lo) <= k || s.token(lo)[k] != target {
		return emptyTokenRange
	}
	first := lo

	// Upper bound: first token with byte k > target, stepped back to the last
	// one equal to it. lo already holds first.
	hi = r.last
	for lo < hi {
		mid := lo + (hi-lo)/2
		if s.tokenLen(mid) <= k || s.token(mid)[k] <= target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	last := lo
	if s.tokenLen(lo) > k && s.token(lo)[k] > target {
		last = lo - 1
	}
	return tokenRange{begin: first, last: last}
}

// prefixRange returns the contiguous run of token ids that have needle as a
// byte-prefix, or the empty range. An empty needle spans the whole dictionary.
func (s *Searcher) prefixRange(needle []byte) tokenRange {
	r := tokenRange{begin: 0, last: s.numTokens - 1}
	for k, target := range needle {
		r = s.narrow(r, k, target)
		if r.empty() {
			return emptyTokenRange
		}
	}
	return r
}

// Tokenize segments text into its canonical code sequence — the same greedy
// longest-match segmentation the encoder produces — by narrowing a candidate
// window over the sorted dictionary one byte at a time. Completeness makes
// every byte encodable, so the result always decodes back to text.
func (s *Searcher) Tokenize(text []byte) []uint16 {
	tokens := make([]uint16, 0, len(text))
	for pos := 0; pos < len(text); {
		maxLen := min(len(text)-pos, s.maxTokenLen)
		best := 0
		r := tokenRange{begin: 0, last: s.numTokens - 1}
		for k := range maxLen {
			next := s.narrow(r, k, text[pos+k])
			if next.empty() {
				break
			}
			// The shortest token in range sorts first; length exactly k+1
			// means it matches the bytes consumed so far in full.
			if s.tokenLen(next.begin) == k+1 {
				best = next.begin
			}
			r = next
		}
		tokens = append(tokens, uint16(best))
		pos += s.tokenLen(best)
	}
	return tokens
}

// RowsEqualTo returns the indices of rows exactly equal to needle. Uniqueness
// and completeness make the encoding canonical, so equality is a code-slice
// compare against the tokenized needle — no decoding.
func (s *Searcher) RowsEqualTo(needle []byte) []int {
	query := s.Tokenize(needle)
	var hits []int
	for k := 0; k < s.archive.Rows(); k++ {
		if slices.Equal(s.searchRowCodes(k), query) {
			hits = append(hits, k)
		}
	}
	return hits
}

// prefixQuery is a needle prepared for prefix search: its token sequence plus,
// per token position, the range of dictionary tokens that extend the rest of
// the needle — the valid-divergence interval.
type prefixQuery struct {
	tokens    []uint16
	intervals []tokenRange
}

func (s *Searcher) newPrefixQuery(prefix []byte) prefixQuery {
	tokens := s.Tokenize(prefix)
	intervals := make([]tokenRange, len(tokens))
	byteOff := 0
	for i, t := range tokens {
		intervals[i] = s.prefixRange(prefix[byteOff:])
		byteOff += s.tokenLen(int(t))
	}
	return prefixQuery{tokens: tokens, intervals: intervals}
}

// startsWith reports whether the row codes begin with the query's needle: the
// codes match the needle's tokens up to a single divergence point, where the
// row's token must extend the whole remaining needle (greedy segmentation
// makes a partial extension impossible).
func startsWith(codes []uint16, q *prefixQuery) bool {
	minLen := min(len(codes), len(q.tokens))
	diff := 0
	for diff < minLen && codes[diff] == q.tokens[diff] {
		diff++
	}
	if diff == len(q.tokens) {
		return true // every needle token matched; also the empty needle
	}
	if diff < len(codes) {
		return q.intervals[diff].contains(codes[diff])
	}
	return false // the row ended before the needle was satisfied
}

// RowsStartingWith returns the indices of rows that begin with prefix.
func (s *Searcher) RowsStartingWith(prefix []byte) []int {
	q := s.newPrefixQuery(prefix)
	var hits []int
	for k := 0; k < s.archive.Rows(); k++ {
		if startsWith(s.searchRowCodes(k), &q) {
			hits = append(hits, k)
		}
	}
	return hits
}

// RowsContaining returns the indices of rows containing pattern as a
// substring, even one straddling token boundaries, via a token-level KMP
// transition table built once per call. Patterns longer than 65535 bytes are
// rejected (KMP states are uint16).
func (s *Searcher) RowsContaining(pattern []byte) ([]int, error) {
	if len(pattern) > math.MaxUint16 {
		return nil, fmt.Errorf("pattern length %d exceeds %d", len(pattern), math.MaxUint16)
	}
	table := s.newContainsTable(pattern)
	var hits []int
	for k := 0; k < s.archive.Rows(); k++ {
		if table.matches(s.searchRowCodes(k)) {
			hits = append(hits, k)
		}
	}
	return hits, nil
}

// containsTable is a token-level KMP transition table for one pattern: one
// transition per token rather than per byte. base holds the transition from
// state 0 densely; for an entry state > 0 only tokens whose first byte lies on
// the pattern's failure chain transition differently, and the sorted
// dictionary makes those tokens contiguous ranges, stored sparsely per state.
// A state counts the leading pattern bytes matched; accept == len(pattern).
type containsTable struct {
	accept  uint16
	base    []uint16
	sparse  []sparseTransition
	offsets []uint32 // offsets[s]..offsets[s+1] bounds state s's exceptions
}

// sparseTransition overrides base: tokens in rng transition to target.
type sparseTransition struct {
	rng    tokenRange
	target uint16
}

// next is the KMP transition from state on code. Precondition: state < accept
// (matches stops at accept), so offsets[state+1] is in bounds.
func (t *containsTable) next(state, code uint16) uint16 {
	if state > 0 {
		// Ranges are ascending and disjoint: stop once past the code.
		for _, tr := range t.sparse[t.offsets[state]:t.offsets[state+1]] {
			if int(code) < tr.rng.begin {
				break
			}
			if int(code) <= tr.rng.last {
				return tr.target
			}
		}
	}
	return t.base[code]
}

// matches reports whether codes contain the table's pattern as a substring.
func (t *containsTable) matches(codes []uint16) bool {
	if t.accept == 0 {
		return true // the empty pattern is a substring of everything
	}
	state := uint16(0)
	for _, code := range codes {
		state = t.next(state, code)
		if state == t.accept {
			return true
		}
	}
	return false
}

func (s *Searcher) newContainsTable(pattern []byte) containsTable {
	m := len(pattern)
	if m == 0 {
		return containsTable{}
	}
	b := containsBuilder{
		s:       s,
		p:       pattern,
		fail:    kmpFailure(pattern),
		base:    make([]uint16, s.numTokens),
		offsets: make([]uint32, m+1),
	}
	if m <= dfaMaxPattern {
		b.buildDFA()
	}
	b.basePass()
	b.sparsePass()
	return containsTable{accept: uint16(m), base: b.base, sparse: b.sparse, offsets: b.offsets}
}

// containsBuilder is the scratch state for building a containsTable; the
// dictionary traversal threads KMP states through it.
type containsBuilder struct {
	s          *Searcher
	p          []byte
	fail       []uint16
	dfa        [][256]uint16 // dense byte transitions per state; nil for long patterns
	base       []uint16
	sparse     []sparseTransition
	offsets    []uint32
	rangeStart int // sparse index where the current entry state's ranges begin
}

// dfaMaxPattern caps the dense DFA at a 132 KiB table. Longer patterns fall
// back to failure-chain stepping; building base dominates RowsContaining, so
// the dense table is worth it for every realistic pattern length.
const dfaMaxPattern = 256

// buildDFA precomputes dfa[s][c] = the state after byte c from state s, with
// the accept state absorbing — replacing the per-byte failure-chain walk with
// one load. Rows are [256]uint16 arrays so byte indexing needs no bounds check.
func (b *containsBuilder) buildDFA() {
	m := len(b.p)
	dfa := make([][256]uint16, m+1)
	dfa[0][b.p[0]] = 1
	x := 0 // the state the failure link mirrors
	for s := 1; s <= m; s++ {
		dfa[s] = dfa[x]
		if s < m {
			dfa[s][b.p[s]] = uint16(s + 1)
			x = int(dfa[x][b.p[s]])
		}
	}
	for c := range 256 {
		dfa[m][c] = uint16(m)
	}
	b.dfa = dfa
}

// kmpFailure is the byte-level KMP failure table: fail[i] is the length of the
// longest proper prefix of p[:i+1] that is also a suffix.
func kmpFailure(p []byte) []uint16 {
	fail := make([]uint16, len(p))
	length := 0
	for i := 1; i < len(p); {
		switch {
		case p[i] == p[length]:
			length++
			fail[i] = uint16(length)
			i++
		case length > 0:
			length = int(fail[length-1])
		default:
			i++
		}
	}
	return fail
}

// stepByte advances the byte-level KMP one byte from state st; the accept
// state is absorbing.
func (b *containsBuilder) stepByte(st uint16, c byte) uint16 {
	if b.dfa != nil {
		return b.dfa[st][c]
	}
	if int(st) == len(b.p) {
		return st
	}
	for st > 0 && b.p[st] != c {
		st = b.fail[st-1]
	}
	if b.p[st] == c {
		st++
	}
	return st
}

func (b *containsBuilder) stepBytes(st uint16, data []byte) uint16 {
	if dfa := b.dfa; dfa != nil {
		for _, c := range data {
			st = dfa[st][c]
		}
		return st
	}
	for _, c := range data {
		st = b.stepByte(st, c)
	}
	return st
}

// basePass fills base[t] with the state reached by running token t's bytes
// from state 0. Cost is one serial DFA step per dictionary byte past each
// token's first pattern-byte occurrence.
// ponytail: dependent-load bound; interleave several tokens' walks if a
// profile ever shows table build dominating a real workload.
func (b *containsBuilder) basePass() {
	p0 := b.p[0]
	for t := range b.base {
		// State 0 only advances on the pattern's first byte, so jump straight
		// past its first occurrence; without one the token cannot leave state 0.
		tok := b.s.token(t)
		if i := bytes.IndexByte(tok, p0); i >= 0 {
			b.base[t] = b.stepBytes(1, tok[i+1:])
		}
	}
}

// emit appends (rng, target), extending the previous range when adjacent with
// the same target; merging never crosses into the previous entry state.
func (b *containsBuilder) emit(rng tokenRange, target uint16) {
	if len(b.sparse) > b.rangeStart {
		last := &b.sparse[len(b.sparse)-1]
		if last.target == target && last.rng.last+1 == rng.begin {
			last.rng.last = rng.last
			return
		}
	}
	b.sparse = append(b.sparse, sparseTransition{rng: rng, target: target})
}

// sparsePass fills the sparse exceptions for every entry state j in 1..m.
func (b *containsBuilder) sparsePass() {
	m := len(b.p)
	var relevant []byte
	for j := 1; j < m; j++ {
		b.rangeStart = len(b.sparse)
		b.offsets[j] = uint32(b.rangeStart)

		// Only bytes p[s] along the failure chain j → fail[j-1] → … → 0 can
		// move state j differently from state 0; skip every other first byte.
		relevant = relevant[:0]
		for st := uint16(j); st > 0; st = b.fail[st-1] {
			relevant = append(relevant, b.p[st])
		}
		slices.Sort(relevant)
		relevant = slices.Compact(relevant)

		for _, c := range relevant {
			rng := b.s.prefixRange([]byte{c})
			if rng.empty() {
				continue
			}
			b.traverse(rng, 1, b.stepByte(uint16(j), c), b.stepByte(0, c))
		}
	}
	b.offsets[m] = uint32(len(b.sparse))
}

// traverse walks the sorted-dictionary subtree rng at byte depth, tracking the
// KMP state evolved from the entry state (kmpJ) against the one evolved from
// state 0 (kmp0 — what base records), emitting an exception for every token
// where they differ. Subtrees where the two states coincide are pruned.
func (b *containsBuilder) traverse(rng tokenRange, depth int, kmpJ, kmp0 uint16) {
	if kmpJ == kmp0 || rng.empty() {
		return
	}
	m := uint16(len(b.p))

	// Full match from the entry state: accept is absorbing, so it holds
	// through the whole subtree; override any token whose base differs.
	if kmpJ == m {
		for i := rng.begin; i <= rng.last; {
			if b.base[i] != m {
				start := i
				for i <= rng.last && b.base[i] != m {
					i++
				}
				b.emit(tokenRange{begin: start, last: i - 1}, m)
			} else {
				i++
			}
		}
		return
	}

	// Tokens of length == depth end here; they all exit at kmpJ.
	cur := rng.begin
	for cur <= rng.last && b.s.tokenLen(cur) == depth {
		cur++
	}
	if cur > rng.begin {
		b.emit(tokenRange{begin: rng.begin, last: cur - 1}, kmpJ)
	}

	// Partition the longer tokens by their byte at depth and recurse.
	for cur <= rng.last {
		c := b.s.token(cur)[depth]
		subHi := cur
		for subHi < rng.last && b.s.token(subHi + 1)[depth] == c {
			subHi++
		}
		b.traverse(tokenRange{begin: cur, last: subHi}, depth+1, b.stepByte(kmpJ, c), b.stepByte(kmp0, c))
		cur = subHi + 1
	}
}
