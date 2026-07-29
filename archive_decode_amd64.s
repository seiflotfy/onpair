//go:build amd64

#include "textflag.h"

// func decodeFast(dst []byte, w int, codes []uint16, i int, bounds []uint32, dict []byte) (int, int)
//
// Register plan:
//   DI dst base    SI len(dst)     DX w        R8 codes base  R9 len(codes)
//   R10 i          R11 bounds base R12 len(bounds)            R13 dict base
//   R14 len(dict)  AX code         CX start    BX tokenLen    R15 scratch
//
// Every store is preceded by the same guards as the Go fast path: the code
// indexes bounds, tokenLen <= 16 (a wrapped 32-bit subtraction fails this),
// start+16 <= len(dict) covers the 16-byte source read, and w+16 <= len(dst)
// covers the 16-byte destination write.
TEXT ·decodeFast(SB), NOSPLIT, $0-128
	MOVQ dst_base+0(FP), DI
	MOVQ dst_len+8(FP), SI
	MOVQ w+24(FP), DX
	MOVQ codes_base+32(FP), R8
	MOVQ codes_len+40(FP), R9
	MOVQ i+56(FP), R10
	MOVQ bounds_base+64(FP), R11
	MOVQ bounds_len+72(FP), R12
	MOVQ dict_base+88(FP), R13
	MOVQ dict_len+96(FP), R14

loop:
	CMPQ R10, R9
	JGE  done                    // i >= len(codes)
	MOVWQZX (R8)(R10*2), AX      // code := codes[i]
	LEAQ 1(AX), BX
	CMPQ BX, R12
	JGE  done                    // code+1 >= len(bounds)
	MOVQ (R11)(AX*4), BX         // one 8-byte load: low32 = bounds[code], high32 = bounds[code+1]
	MOVL BX, CX                  // start (zero-extended)
	SHRQ $32, BX                 // end
	SUBL CX, BX                  // tokenLen = end - start (wraps huge if corrupt)
	CMPL BX, $16
	JA   done                    // tokenLen > decodeSlack
	LEAQ 16(CX), R15
	CMPQ R15, R14
	JA   done                    // start+16 > len(dict)
	LEAQ 16(DX), R15
	CMPQ R15, SI
	JA   done                    // w+16 > len(dst)
	MOVOU (R13)(CX*1), X0        // 16-byte over-copy
	MOVOU X0, (DI)(DX*1)
	ADDQ BX, DX                  // w += tokenLen
	INCQ R10                     // i++
	JMP  loop

done:
	MOVQ R10, ret+112(FP)
	MOVQ DX, ret1+120(FP)
	RET
