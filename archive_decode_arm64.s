//go:build arm64

#include "textflag.h"

// func decodeFast(dst []byte, w int, codes []uint16, i int, bounds []uint32, dict []byte) (int, int)
//
// Register plan:
//   R0 dst base    R1 len(dst)     R2 w        R3 codes base  R4 len(codes)
//   R5 i           R6 bounds base  R7 len(bounds)              R8 dict base
//   R9 len(dict)   R10/R12/R13/R17 scratch     R11 code       R14 start  R15 end  R16 tokenLen
//
// Every store is preceded by the same guards as the Go fast path: the code
// indexes bounds, tokenLen <= 16 (a wrapped 32-bit subtraction fails this),
// start+16 <= len(dict) covers the 16-byte source read, and w+16 <= len(dst)
// covers the 16-byte destination write.
TEXT ·decodeFast(SB), NOSPLIT, $0-128
	MOVD dst_base+0(FP), R0
	MOVD dst_len+8(FP), R1
	MOVD w+24(FP), R2
	MOVD codes_base+32(FP), R3
	MOVD codes_len+40(FP), R4
	MOVD i+56(FP), R5
	MOVD bounds_base+64(FP), R6
	MOVD bounds_len+72(FP), R7
	MOVD dict_base+88(FP), R8
	MOVD dict_len+96(FP), R9

loop:
	CMP  R4, R5
	BGE  done                    // i >= len(codes)
	LSL  $1, R5, R10
	MOVHU (R3)(R10), R11         // code := codes[i]
	ADD  $1, R11, R12
	CMP  R7, R12
	BGE  done                    // code+1 >= len(bounds)
	LSL  $2, R11, R12
	MOVD (R6)(R12), R13          // one 8-byte load: low32 = bounds[code], high32 = bounds[code+1]
	AND  $0xffffffff, R13, R14   // start
	LSR  $32, R13, R15           // end
	SUBW R14, R15, R16           // tokenLen = end - start (wraps huge if corrupt)
	CMPW $16, R16
	BHI  done                    // tokenLen > decodeSlack
	ADD  $16, R14, R17
	CMP  R9, R17
	BHI  done                    // start+16 > len(dict)
	ADD  $16, R2, R17
	CMP  R1, R17
	BHI  done                    // w+16 > len(dst)
	ADD  R14, R8, R17
	LDP  (R17), (R12, R13)       // 16-byte over-copy
	ADD  R2, R0, R17
	STP  (R12, R13), (R17)
	ADD  R16, R2, R2             // w += tokenLen
	ADD  $1, R5, R5              // i++
	B    loop

done:
	MOVD R5, ret+112(FP)
	MOVD R2, ret1+120(FP)
	RET
