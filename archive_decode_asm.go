//go:build arm64 || amd64

package onpair

// decodeFast is the assembly fast path of the gather-copy decode loops
// (archive_decode_arm64.s, archive_decode_amd64.s). Starting at codes[i] and dst[w], it copies each
// token as one 16-byte over-copy and stops at the first token it cannot
// prove safe — an out-of-range ID, a length over decodeSlack (including
// wrapped corrupt boundaries), a token within decodeSlack of the dictionary
// end, or fewer than decodeSlack bytes left before len(dst). It returns the
// next code index and write offset; the caller settles the stopped token.
//
//go:noescape
func decodeFast(dst []byte, w int, codes []uint16, i int, bounds []uint32, dict []byte) (int, int)
