package onpair

// decodeFastGeneric is the portable implementation of decodeFast, compiled on
// every platform: non-assembly builds use it as decodeFast, and the assembly
// kernels are differential-tested against it.
// Starting at codes[i] and dst[w], it copies each token as one 16-byte
// over-copy and stops at the first token it cannot prove safe — an
// out-of-range ID, a length over decodeSlack (including wrapped corrupt
// boundaries), a token within decodeSlack of the dictionary end, or fewer
// than decodeSlack bytes left before len(dst). It returns the next code
// index and write offset; the caller settles the stopped token.
func decodeFastGeneric(dst []byte, w int, codes []uint16, i int, bounds []uint32, dict []byte) (int, int) {
	dictLen := len(dict)
	for ; i < len(codes); i++ {
		tokenID := codes[i]
		if int(tokenID)+1 >= len(bounds) {
			return i, w
		}
		tokenStart := bounds[tokenID]
		tokenLen := bounds[int(tokenID)+1] - tokenStart
		if tokenLen > decodeSlack || uint64(tokenStart)+decodeSlack > uint64(dictLen) || w+decodeSlack > len(dst) {
			return i, w
		}
		ts := int(tokenStart)
		copy(dst[w:w+decodeSlack], dict[ts:ts+decodeSlack])
		w += int(tokenLen)
	}
	return i, w
}
