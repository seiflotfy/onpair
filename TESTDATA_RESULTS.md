# Testdata Compression Results

Compression results for the files in `testdata/`, produced by:

```bash
go test -run TestSerializationSizes -v .
```

Regenerate this table after any change to training or serialization; the
numbers below are from the current code.

**Column definitions:**

- **SpaceUsed**: core compression data (compressed tokens + dictionary + token boundaries)
- **Full In-Mem**: total with absolute string boundaries (8 bytes × count)
- **Serialized**: `WriteTo` output — delta-encoded boundaries, auto-selected token-stream encoding, metadata
- **Ratio**: original size / serialized size

## OnPair (default configuration)

| File | Original | SpaceUsed | Full In-Mem | Serialized | Ratio |
|------|----------|-----------|-------------|------------|-------|
| art_of_war.txt | 10,312 B | 13,754 B | 14,122 B | 9,104 B | **1.13x** |
| en_bible_kjv.txt | 4,256,060 B | 1,800,915 B | 2,600,667 B | 1,648,043 B | **2.58x** |
| en_mobydick.txt | 1,231,686 B | 826,859 B | 1,005,347 B | 720,168 B | **1.71x** |
| en_shakespeare.txt | 5,245,735 B | 2,512,917 B | 4,084,085 B | 2,388,575 B | **2.20x** |
| logs_apache_2k.log | 167,241 B | 69,541 B | 85,549 B | 62,136 B | **2.69x** |
| logs_hdfs_2k.log | 283,848 B | 156,377 B | 172,385 B | 129,479 B | **2.19x** |
| zh_tao_te_ching_en.txt | 76,909 B | 75,292 B | 90,044 B | 57,366 B | **1.34x** |

## OnPair16 (`WithMaxTokenLength(16)`)

| File | Original | SpaceUsed | Full In-Mem | Serialized | Ratio |
|------|----------|-----------|-------------|------------|-------|
| art_of_war.txt | 10,312 B | 13,738 B | 14,106 B | 9,087 B | **1.13x** |
| en_bible_kjv.txt | 4,256,060 B | 1,787,192 B | 2,586,944 B | 1,629,263 B | **2.61x** |
| en_mobydick.txt | 1,231,686 B | 824,170 B | 1,002,658 B | 717,805 B | **1.72x** |
| en_shakespeare.txt | 5,245,735 B | 2,512,793 B | 4,083,961 B | 2,386,757 B | **2.20x** |
| logs_apache_2k.log | 167,241 B | 42,570 B | 58,578 B | 18,932 B | **8.83x** |
| logs_hdfs_2k.log | 283,848 B | 145,347 B | 161,355 B | 101,302 B | **2.80x** |
| zh_tao_te_ching_en.txt | 76,909 B | 74,760 B | 89,512 B | 56,771 B | **1.35x** |

## Other configurations

- `WithThreshold(10)`: fewer, higher-frequency merges; best on repetitive logs
  (apache 10.03x, hdfs 4.70x serialized).
- `WithMaxTokenID(4095)`: 4,096-token dictionary paced by the adaptive
  threshold controller; smaller dictionaries with competitive ratios (bible
  2.75x, mobydick 2.21x).
- All configurations round-trip byte-perfect; the full matrix is printed by
  `TestSerializationSizes` and `TestAllTestdataFiles`.

## Serialization notes

String boundaries are serialized as a first absolute value plus varint deltas
(~87% smaller than absolute 8-byte boundaries on the log datasets, e.g.
16,012 B → 2,016 B for apache's 2,001 boundaries); they are reconstructed to
absolute positions on load, so random access stays O(1). The compressed token
stream picks the smallest of raw, flate(raw), byte-codebook+escape, and
flate(codebook) for both 12-bit and 16-bit widths.
