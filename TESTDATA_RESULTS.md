# Testdata Compression Results

## Summary

✅ **All 7 testdata files compress and decompress perfectly with the new API**
❌ **Old API fails on continuous files (not its intended use case)**

## New Dictionary API Results

| File | Original | Compressed | Ratio | Status |
|------|----------|------------|-------|--------|
| art_of_war.txt | 10,356 B | 6,050 B | **1.71x** | ✅ PASS |
| en_bible_kjv.txt | 4,455,996 B | 1,355,260 B | **3.29x** | ✅ PASS |
| en_mobydick.txt | 1,276,306 B | 467,816 B | **2.73x** | ✅ PASS |
| en_shakespeare.txt | 5,638,525 B | 2,235,348 B | **2.52x** | ✅ PASS |
| logs_apache_2k.log | 171,239 B | 14,124 B | **12.12x** 🔥 | ✅ PASS |
| logs_hdfs_2k.log | 287,848 B | 60,032 B | **4.79x** | ✅ PASS |
| zh_tao_te_ching_en.txt | 80,595 B | 38,558 B | **2.09x** | ✅ PASS |

### Highlights

- **Apache logs**: 12.12x compression! (highly repetitive log format)
- **Bible**: 3.29x compression (King James Version)
- **All files decompress byte-perfect** ✅

## Dictionary16 (16-byte constraint) Results

| File | Original | Compressed | Ratio |
|------|----------|------------|-------|
| art_of_war.txt | 10,356 B | 6,044 B | **1.71x** |
| en_bible_kjv.txt | 4,455,996 B | 1,395,800 B | **3.19x** |
| en_mobydick.txt | 1,276,306 B | 469,356 B | **2.72x** |
| en_shakespeare.txt | 5,638,525 B | 2,256,408 B | **2.50x** |
| logs_apache_2k.log | 171,239 B | 32,860 B | **5.21x** |
| logs_hdfs_2k.log | 287,848 B | 81,010 B | **3.55x** |
| zh_tao_te_ching_en.txt | 80,595 B | 38,798 B | **2.08x** |

**All Dictionary16 tests also pass perfectly** ✅

## Why Old API Failed

The old `OnPair` API was designed for **collections of separate strings** (like database columns), not continuous byte streams:

```go
// ✅ What old API was designed for:
strings := []string{
    "user_000001@example.com",
    "user_000002@example.com",
    "user_000003@example.com",
}
onpair := New()
onpair.CompressStrings(strings)  // Each string preserved separately
```

When we split files by newlines, the `\n` characters are lost, so decompression produces different output.

## New API Advantages

1. **Works for continuous byte streams** (files, network data, etc.)
2. **Works for string collections** (compress each individually)
3. **Better compression ratios** (especially on structured data like logs)
4. **Byte-perfect decompression** on all test files

## Test Coverage

```bash
go test -v ./compressor -run TestRoundtripAllTestdata
```

All 14 subtests (7 files × 2 dictionary types) pass ✅
