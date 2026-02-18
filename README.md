# OnPair: Short Strings Compression for Fast Random Access

[![Paper](https://img.shields.io/badge/Paper-arXiv:2508.02280-blue)](https://arxiv.org/abs/2508.02280)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Go implementation of **OnPair**, a compression algorithm designed for efficient random access on sequences of short strings.

## Overview

OnPair is a field-level compression algorithm designed for workloads requiring fast random access to individual strings in large collections. The compression process consists of two distinct phases:

- **Training Phase**: A longest prefix matching strategy is used to parse the input and identify frequent adjacent token pairs. When the frequency of a pair exceeds a predefined threshold, a new token is created to represent the merged pair. This continues until the dictionary is full or the input data is exhausted. The dictionary supports up to 65,536 tokens.
- **Parsing Phase**: Once the dictionary is constructed, each string is compressed independently into a sequence of token IDs by greedily applying longest prefix matching.

OnPair16 is a variant that limits dictionary entries to a maximum length of 16 bytes. This constraint enables further optimizations in both longest prefix matching and decoding.

## Installation

```bash
go get github.com/seiflotfy/onpair
```

## Quick Start

### Reusable model (`TrainModel` -> `Encode`)

```go
package main

import (
    "fmt"
    "github.com/seiflotfy/onpair"
)

func main() {
    trainRows := []string{
        "user_000001",
        "user_000002",
        "user_000003",
        "admin_001",
        "user_000004",
    }

    model, err := onpair.TrainModel(trainRows, onpair.WithMaxTokenLength(16))
    if err != nil {
        panic(err)
    }

    archive, err := model.Encode(trainRows)
    if err != nil {
        panic(err)
    }

    for i := 0; i < archive.Rows(); i++ {
        row, err := archive.AppendRow(nil, i)
        if err != nil {
            panic(err)
        }
        fmt.Printf("row %d: %s\n", i, string(row))
    }
}
```

### Single-shot encode (`Encoder`)

```go
enc := onpair.NewEncoder(
    onpair.WithMaxTokenLength(16), // optional
    onpair.WithMaxTokenID(4095),   // optional smaller dictionary cap
    onpair.WithTokenBitWidth(12),  // optional packed 12-bit token stream
    onpair.WithTrainingSampleBytes(8*1024*1024), // optional larger training sample
    onpair.WithDrainStratifiedSampling(2048),    // optional Drain-like stratified sampling
)
archive, err := enc.Encode([]string{"user_001", "user_002", "admin_001"})
if err != nil {
    panic(err)
}
```

`WithTokenBitWidth(12)` uses packed 12-bit token IDs in archive storage and
automatically limits dictionary IDs to `4095`.
The serialized `compressed_data` stage now auto-selects flate encoding when it
is smaller than the raw token stream (for both 12-bit and 16-bit payloads).

## Advanced Features

### Random access (decode one string at a time)

```go
row, err := archive.AppendRow(nil, 42) // row index 42 only
if err != nil {
    panic(err)
}
fmt.Println(string(row))
```

### Strict decoding into caller buffers

```go
buf := make([]byte, 256)
n, err := archive.DecompressString(0, buf) // returns ErrShortBuffer if too small
if err != nil {
    panic(err)
}
fmt.Println(string(buf[:n]))
```

### Bulk decode with error handling

```go
all := make([]byte, 4096)
n, err := archive.DecompressAllChecked(all)
if err != nil {
    panic(err)
}
_ = all[:n]
```

### Serialization

```go
file, err := os.Create("archive.bin")
if err != nil {
    panic(err)
}
defer file.Close()

if _, err := archive.WriteTo(file); err != nil {
    panic(err)
}

loaded := &onpair.Archive{}
if _, err := file.Seek(0, io.SeekStart); err != nil {
    panic(err)
}
if _, err := loaded.ReadFrom(file); err != nil {
    panic(err)
}
```

## API Reference

### Recommended lifecycle (`Model` + `Archive`)

```go
model, err := onpair.TrainModel(trainRows, onpair.WithMaxTokenLength(16))
if err != nil { /* ... */ }

archive, err := model.Encode(queryRows)
if err != nil { /* ... */ }

out, err := archive.AppendRow(nil, 0)
if err != nil { /* ... */ }
_ = out
```

### Constructors and options

- `NewEncoder(opts ...Option) *Encoder`
- `NewModel(opts ...Option) *Model`
- `TrainModel(strings []string, opts ...Option) (*Model, error)`
- `WithThreshold(t uint16) Option`
- `WithMaxTokenLength(n int) Option`
- `WithMaxTokenID(maxID uint16) Option`
- `WithTokenBitWidth(bits uint8) Option` (`12` or `16`, default `16`)
- `WithTrainingSampleBytes(n int) Option` (default `1 MiB`)
- `WithDrainStratifiedSampling(maxClusters int) Option`

### Encode/decode

- `(*Encoder).Encode(strings []string) (*Archive, error)` (single-shot train+encode)
- `(*Model).Train(strings []string) error`
- `(*Model).Encode(strings []string) (*Archive, error)`
- `(*Model).Trained() bool`
- `(*Archive).Rows() int`
- `(*Archive).DecodedLen(index int) (int, error)`
- `(*Archive).AppendRow(dst []byte, index int) ([]byte, error)`
- `(*Archive).AppendAll(dst []byte) ([]byte, error)`
- `(*Archive).DecompressString(index int, buffer []byte) (int, error)`
- `(*Archive).DecompressAllChecked(buffer []byte) (int, error)`

### Serialization

- `(*Archive).WriteTo(w io.Writer) (int64, error)`
- `(*Archive).ReadFrom(r io.Reader) (int64, error)`

## Building from Source

```bash
git clone https://github.com/seiflotfy/onpair
cd onpair

# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./...
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Authors

- **Francesco Gargiulo** - [francesco.gargiulo@phd.unipi.it](mailto:francesco.gargiulo@phd.unipi.it)
- **Rossano Venturini** - [rossano.venturini@unipi.it](mailto:rossano.venturini@unipi.it)

*University of Pisa, Italy*
