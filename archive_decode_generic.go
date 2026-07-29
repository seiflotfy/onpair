//go:build !arm64 && !amd64

package onpair

// decodeFast is the portable fast path of the gather-copy decode loops; the
// arm64 and amd64 builds replace it with an assembly kernel. See
// decodeFastGeneric for the semantics.
func decodeFast(dst []byte, w int, codes []uint16, i int, bounds []uint32, dict []byte) (int, int) {
	return decodeFastGeneric(dst, w, codes, i, bounds, dict)
}
