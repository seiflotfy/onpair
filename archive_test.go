package onpair

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

// ============================================================================
// Gather-copy decode edge cases
// ============================================================================

// gatherEdgeArchive builds an archive by hand whose dictionary is shorter than
// the 16-byte fast-copy width and which contains a zero-length token, so every
// decode path must take the exact-copy fallback and skip nothing.
func gatherEdgeArchive() *Archive {
	return &Archive{
		// Tokens: 0 -> "ab", 1 -> "" (zero-length), 2 -> "cde"
		CompressedData:   []uint16{0, 1, 2, 0},
		StringBoundaries: []int{0, 2, 4},
		Dictionary:       []byte("abcde"),
		TokenBoundaries:  []uint32{0, 2, 2, 5},
	}
}

func TestDecodeZeroLengthTokenAndShortDictionary(t *testing.T) {
	archive := gatherEdgeArchive()
	wantRows := []string{"ab", "cdeab"}
	wantAll := "abcdeab"

	all, err := archive.AppendAll(nil)
	if err != nil {
		t.Fatalf("AppendAll: %v", err)
	}
	if string(all) != wantAll {
		t.Fatalf("AppendAll = %q, want %q", all, wantAll)
	}

	for i, want := range wantRows {
		row, err := archive.AppendRow(nil, i)
		if err != nil {
			t.Fatalf("AppendRow(%d): %v", i, err)
		}
		if string(row) != want {
			t.Fatalf("AppendRow(%d) = %q, want %q", i, row, want)
		}

		n, err := archive.DecodedLen(i)
		if err != nil {
			t.Fatalf("DecodedLen(%d): %v", i, err)
		}
		if n != len(want) {
			t.Fatalf("DecodedLen(%d) = %d, want %d", i, n, len(want))
		}

		buf := make([]byte, len(want)) // exact fit: over-copy must stay inside
		if n, err = archive.DecompressString(i, buf); err != nil {
			t.Fatalf("DecompressString(%d): %v", i, err)
		}
		if string(buf[:n]) != want {
			t.Fatalf("DecompressString(%d) = %q, want %q", i, buf[:n], want)
		}

		roomy := make([]byte, 64) // row-sized fast path: dictionary-end tokens exact-copied
		if n, err = archive.DecompressString(i, roomy); err != nil {
			t.Fatalf("DecompressString(%d, roomy): %v", i, err)
		}
		if string(roomy[:n]) != want {
			t.Fatalf("DecompressString(%d, roomy) = %q, want %q", i, roomy[:n], want)
		}
	}

	buf := make([]byte, len(wantAll))
	n, err := archive.DecompressAllChecked(buf)
	if err != nil {
		t.Fatalf("DecompressAllChecked: %v", err)
	}
	if string(buf[:n]) != wantAll {
		t.Fatalf("DecompressAllChecked = %q, want %q", buf[:n], wantAll)
	}
}

func TestAppendRowReusesDstAcrossRows(t *testing.T) {
	input := []string{"user_000001", "user_000002", "admin_001", "user_000003"}
	archive := mustEncode(NewEncoder(), input)

	var dst []byte
	for i, want := range input {
		prev := len(dst)
		next, err := archive.AppendRow(dst, i)
		if err != nil {
			t.Fatalf("AppendRow(%d): %v", i, err)
		}
		if string(next[prev:]) != want {
			t.Fatalf("AppendRow(%d) appended %q, want %q", i, next[prev:], want)
		}
		dst = next
	}
	if string(dst) != strings.Join(input, "") {
		t.Fatalf("accumulated rows = %q", dst)
	}
}

func TestDecodeWrappedBoundariesError(t *testing.T) {
	// Adjacent token boundaries that wrap mod 2^32 with a distance of at most
	// decodeSlack must error, never panic, on every decode path (regression:
	// the row-sized dispatch once sliced dict[4294967286:5]).
	archive := &Archive{
		CompressedData:   []uint16{0},
		StringBoundaries: []int{0, 1},
		Dictionary:       make([]byte, 16),
		TokenBoundaries:  []uint32{0xFFFFFFF6, 5},
	}
	if _, err := archive.DecompressString(0, make([]byte, 64)); err == nil {
		t.Fatal("DecompressString(roomy): expected corruption error")
	}
	if _, err := archive.DecompressString(0, make([]byte, 8)); err == nil {
		t.Fatal("DecompressString(tight): expected corruption error")
	}
	if _, err := archive.DecompressAllChecked(make([]byte, 64)); err == nil {
		t.Fatal("DecompressAllChecked: expected corruption error")
	}
	if _, err := archive.AppendRow(nil, 0); err == nil {
		t.Fatal("AppendRow: expected corruption error")
	}
	if _, err := archive.AppendAll(nil); err == nil {
		t.Fatal("AppendAll: expected corruption error")
	}
	if _, err := archive.DecodedLen(0); err == nil {
		t.Fatal("DecodedLen: expected corruption error")
	}
}

func TestCodebookDecodeRejectsOversizedCount(t *testing.T) {
	// A codebook payload declaring far more tokens than the stream holds must
	// be rejected before the token slice is allocated (regression: a 6-byte
	// payload once drove a ~1 GB allocation through ReadFrom).
	payload := make([]byte, 6)
	binary.LittleEndian.PutUint32(payload[:4], uint32(maxCompressedTokenRead))
	binary.LittleEndian.PutUint16(payload[4:6], 0)
	if _, err := decodeCompressedDataStageCodebook(payload, tokenBitWidth16); err == nil {
		t.Fatal("expected underrun error for oversized declared count")
	}
}

func TestSerialization(t *testing.T) {
	strings := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}

	// Create and compress with OnPair
	enc := NewEncoder()
	archive := mustEncode(enc, strings)

	// Serialize
	var buf bytes.Buffer
	n, err := archive.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	if n == 0 {
		t.Fatalf("WriteTo wrote 0 bytes")
	}

	// Deserialize
	archive2 := &Archive{}
	n2, err := archive2.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if n2 != n {
		t.Errorf("ReadFrom read %d bytes, WriteTo wrote %d bytes", n2, n)
	}

	// Verify all fields match
	// MaxTokenLen is not stored in Archive anymore, so we skip this check
	// if archive2.MaxTokenLen != archive.MaxTokenLen { ... }
	if len(archive2.CompressedData) != len(archive.CompressedData) {
		t.Errorf("CompressedData length mismatch: got %d, want %d", len(archive2.CompressedData), len(archive.CompressedData))
	}
	if len(archive2.StringBoundaries) != len(archive.StringBoundaries) {
		t.Errorf("StringBoundaries length mismatch: got %d, want %d", len(archive2.StringBoundaries), len(archive.StringBoundaries))
	}
	// MaxTokenLen is not stored in Archive anymore, so we skip this check
	// if archive2.MaxTokenLen != archive.MaxTokenLen { ... }
	if len(archive2.TokenBoundaries) != len(archive.TokenBoundaries) {
		t.Errorf("TokenBoundaries length mismatch: got %d, want %d", len(archive2.TokenBoundaries), len(archive.TokenBoundaries))
	}

	// Verify decompression works correctly
	buffer := make([]byte, 256)
	for i := 0; i < len(strings); i++ {
		size, err := archive2.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		decompressed := string(buffer[:size])
		if decompressed != strings[i] {
			t.Errorf("Decompression mismatch at index %d: got %q, want %q", i, decompressed, strings[i])
		}
	}
}

func TestSerialization16(t *testing.T) {
	strings := []string{
		"user_001",
		"user_002",
		"user_003",
		"admin_01",
	}

	// Create and compress with OnPair16
	enc := NewEncoder(WithMaxTokenLength(16))
	archive := mustEncode(enc, strings)

	// Serialize
	var buf bytes.Buffer
	n, err := archive.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Deserialize
	archive2 := &Archive{}
	n2, err := archive2.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if n2 != n {
		t.Errorf("ReadFrom read %d bytes, WriteTo wrote %d bytes", n2, n)
	}

	// Verify MaxTokenLen is preserved (16 for OnPair16)
	// MaxTokenLen is not stored in Archive anymore
	// if archive2.MaxTokenLen != 16 { ... }

	// Verify decompression works correctly
	buffer := make([]byte, 256)
	for i := 0; i < len(strings); i++ {
		size, err := archive2.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		decompressed := string(buffer[:size])
		if decompressed != strings[i] {
			t.Errorf("Decompression mismatch at index %d: got %q, want %q", i, decompressed, strings[i])
		}
	}
}

func TestSerializationPacked12Bit(t *testing.T) {
	input := []string{
		"user_000001",
		"user_000002",
		"user_000003",
		"admin_001",
		"user_000004",
	}

	archive := mustEncode(NewEncoder(WithTokenBitWidth(12)), input)
	if archive.tokenBitWidth() != tokenBitWidth12 {
		t.Fatalf("token bit-width mismatch: got %d want %d", archive.tokenBitWidth(), tokenBitWidth12)
	}

	expectedCompressedBytes := packed12ByteSize(len(archive.CompressedData))
	expectedSpaceUsed := expectedCompressedBytes + len(archive.Dictionary) + len(archive.TokenBoundaries)*4
	if got := archive.SpaceUsed(); got != expectedSpaceUsed {
		t.Fatalf("SpaceUsed mismatch: got %d want %d", got, expectedSpaceUsed)
	}

	var buf bytes.Buffer
	if _, err := archive.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		t.Fatalf("read magic failed: %v", err)
	}
	if string(magic[:]) != archiveMagic {
		t.Fatalf("magic mismatch: got %q want %q", string(magic[:]), archiveMagic)
	}

	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		t.Fatalf("read version failed: %v", err)
	}
	if version != archiveVersion {
		t.Fatalf("version mismatch: got %d want %d", version, archiveVersion)
	}

	var stageCount uint16
	if err := binary.Read(r, binary.LittleEndian, &stageCount); err != nil {
		t.Fatalf("read stage count failed: %v", err)
	}

	foundCompressed := false
	for i := 0; i < int(stageCount); i++ {
		header, _, err := readStageHeader(r)
		if err != nil {
			t.Fatalf("readStageHeader(%d) failed: %v", i, err)
		}
		params := make([]byte, header.paramLen)
		if _, err := io.ReadFull(r, params); err != nil {
			t.Fatalf("read params(%d) failed: %v", i, err)
		}

		if header.name == stageCompressedData {
			foundCompressed = true
			if len(params) != 1 {
				t.Fatalf("compressed_data params length mismatch: got %d want 1", len(params))
			}
			if params[0] != stageCompressedDataParamWidth12 &&
				params[0] != stageCompressedDataParamWidth12Flate &&
				params[0] != stageCompressedDataParamWidth12Codebook &&
				params[0] != stageCompressedDataParamWidth12CodebookFlate {
				t.Fatalf("compressed_data params mismatch: got %v want [%d/%d/%d/%d]",
					params,
					stageCompressedDataParamWidth12,
					stageCompressedDataParamWidth12Flate,
					stageCompressedDataParamWidth12Codebook,
					stageCompressedDataParamWidth12CodebookFlate,
				)
			}
			if params[0] == stageCompressedDataParamWidth12 {
				expectedPayloadLen := uint32(4 + expectedCompressedBytes)
				if header.dataLen != expectedPayloadLen {
					t.Fatalf("compressed_data payload length mismatch: got %d want %d", header.dataLen, expectedPayloadLen)
				}
			}
		}

		if _, err := io.CopyN(io.Discard, r, int64(header.dataLen)); err != nil {
			t.Fatalf("skip payload(%d) failed: %v", i, err)
		}
	}
	if !foundCompressed {
		t.Fatalf("compressed_data stage not found")
	}

	loaded := &Archive{}
	if _, err := loaded.ReadFrom(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if loaded.tokenBitWidth() != tokenBitWidth12 {
		t.Fatalf("loaded token bit-width mismatch: got %d want %d", loaded.tokenBitWidth(), tokenBitWidth12)
	}
	if !slices.Equal(loaded.CompressedData, archive.CompressedData) {
		t.Fatalf("compressed data mismatch after round-trip")
	}
}

func TestSerializationCompressedDataUsesFlateWhenSmaller16Bit(t *testing.T) {
	rows := make([]string, 30000)
	for i := range rows {
		rows[i] = "GET /api/v1/users/42 HTTP/1.1"
	}

	archive := mustEncode(NewEncoder(), rows)
	if archive.tokenBitWidth() != tokenBitWidth16 {
		t.Fatalf("token bit-width mismatch: got %d want %d", archive.tokenBitWidth(), tokenBitWidth16)
	}

	var blob bytes.Buffer
	if _, err := archive.WriteTo(&blob); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	r := bytes.NewReader(blob.Bytes())
	if _, err := io.CopyN(io.Discard, r, 8); err != nil {
		t.Fatalf("skip archive header failed: %v", err)
	}

	var stageCount uint16
	if err := binary.Read(bytes.NewReader(blob.Bytes()[6:8]), binary.LittleEndian, &stageCount); err != nil {
		t.Fatalf("read stage count failed: %v", err)
	}

	foundCompressed := false
	for i := 0; i < int(stageCount); i++ {
		header, _, err := readStageHeader(r)
		if err != nil {
			t.Fatalf("readStageHeader(%d) failed: %v", i, err)
		}
		params := make([]byte, header.paramLen)
		if _, err := io.ReadFull(r, params); err != nil {
			t.Fatalf("read params(%d) failed: %v", i, err)
		}
		payload := make([]byte, header.dataLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			t.Fatalf("read payload(%d) failed: %v", i, err)
		}

		if header.name != stageCompressedData {
			continue
		}
		foundCompressed = true
		if len(params) != 1 {
			t.Fatalf("compressed_data params length mismatch: got %d want 1", len(params))
		}
		if params[0] != stageCompressedDataParamWidth16Flate &&
			params[0] != stageCompressedDataParamWidth16Codebook &&
			params[0] != stageCompressedDataParamWidth16CodebookFlate {
			t.Fatalf("compressed_data params mismatch: got %v want one of [%d/%d/%d]",
				params,
				stageCompressedDataParamWidth16Flate,
				stageCompressedDataParamWidth16Codebook,
				stageCompressedDataParamWidth16CodebookFlate,
			)
		}

		raw, err := encodeCompressedDataStage16(archive.CompressedData)
		if err != nil {
			t.Fatalf("encodeCompressedDataStage16 failed: %v", err)
		}
		if len(payload) >= len(raw) {
			t.Fatalf("flate compressed payload not smaller: got %d want < %d", len(payload), len(raw))
		}
	}
	if !foundCompressed {
		t.Fatalf("compressed_data stage not found")
	}

	loaded := &Archive{}
	if _, err := loaded.ReadFrom(bytes.NewReader(blob.Bytes())); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if !slices.Equal(loaded.CompressedData, archive.CompressedData) {
		t.Fatalf("compressed data mismatch after round-trip")
	}
}

func TestSerializationCompressedDataUsesFlateWhenSmaller12Bit(t *testing.T) {
	rows := make([]string, 30000)
	for i := range rows {
		rows[i] = "GET /api/v1/users/42 HTTP/1.1"
	}

	archive := mustEncode(NewEncoder(WithTokenBitWidth(12)), rows)
	if archive.tokenBitWidth() != tokenBitWidth12 {
		t.Fatalf("token bit-width mismatch: got %d want %d", archive.tokenBitWidth(), tokenBitWidth12)
	}

	var blob bytes.Buffer
	if _, err := archive.WriteTo(&blob); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	r := bytes.NewReader(blob.Bytes())
	if _, err := io.CopyN(io.Discard, r, 8); err != nil {
		t.Fatalf("skip archive header failed: %v", err)
	}

	var stageCount uint16
	if err := binary.Read(bytes.NewReader(blob.Bytes()[6:8]), binary.LittleEndian, &stageCount); err != nil {
		t.Fatalf("read stage count failed: %v", err)
	}

	foundCompressed := false
	for i := 0; i < int(stageCount); i++ {
		header, _, err := readStageHeader(r)
		if err != nil {
			t.Fatalf("readStageHeader(%d) failed: %v", i, err)
		}
		params := make([]byte, header.paramLen)
		if _, err := io.ReadFull(r, params); err != nil {
			t.Fatalf("read params(%d) failed: %v", i, err)
		}
		payload := make([]byte, header.dataLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			t.Fatalf("read payload(%d) failed: %v", i, err)
		}

		if header.name != stageCompressedData {
			continue
		}
		foundCompressed = true
		if len(params) != 1 {
			t.Fatalf("compressed_data params length mismatch: got %d want 1", len(params))
		}
		if params[0] != stageCompressedDataParamWidth12Flate &&
			params[0] != stageCompressedDataParamWidth12Codebook &&
			params[0] != stageCompressedDataParamWidth12CodebookFlate {
			t.Fatalf("compressed_data params mismatch: got %v want one of [%d/%d/%d]",
				params,
				stageCompressedDataParamWidth12Flate,
				stageCompressedDataParamWidth12Codebook,
				stageCompressedDataParamWidth12CodebookFlate,
			)
		}

		raw, err := encodeCompressedDataStage12(archive.CompressedData)
		if err != nil {
			t.Fatalf("encodeCompressedDataStage12 failed: %v", err)
		}
		if len(payload) >= len(raw) {
			t.Fatalf("flate compressed payload not smaller: got %d want < %d", len(payload), len(raw))
		}
	}
	if !foundCompressed {
		t.Fatalf("compressed_data stage not found")
	}

	loaded := &Archive{}
	if _, err := loaded.ReadFrom(bytes.NewReader(blob.Bytes())); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if !slices.Equal(loaded.CompressedData, archive.CompressedData) {
		t.Fatalf("compressed data mismatch after round-trip")
	}
}

func TestDecodeCompressedDataStageRejectsInvalidFlatePayload(t *testing.T) {
	dst := &Archive{}
	err := decodeCompressedDataStage(dst, []byte{stageCompressedDataParamWidth16Flate}, []byte{0x00, 0x01, 0x02, 0x03})
	if err == nil {
		t.Fatalf("expected decodeCompressedDataStage to fail on invalid flate payload")
	}
}

func TestCompressedDataCodebookRoundTrip16Bit(t *testing.T) {
	compressed := []uint16{
		10, 10, 10, 10, 11, 10, 12, 10, 13, 10, 14, 10, 10, 10, 15, 16, 10, 17, 18, 10,
		500, 501, 500, 500, 502, 503, 500, 504, 505, 500,
	}

	payload, err := encodeCompressedDataStageCodebook(compressed, tokenBitWidth16)
	if err != nil {
		t.Fatalf("encodeCompressedDataStageCodebook failed: %v", err)
	}

	decoded, err := decodeCompressedDataStageCodebook(payload, tokenBitWidth16)
	if err != nil {
		t.Fatalf("decodeCompressedDataStageCodebook failed: %v", err)
	}

	if !slices.Equal(decoded, compressed) {
		t.Fatalf("codebook round-trip mismatch")
	}
}

func TestCompressedDataCodebookRoundTrip12Bit(t *testing.T) {
	compressed := []uint16{
		1, 1, 1, 2, 3, 4, 1, 5, 1, 100, 101, 100, 100, 102, 100, 103, 1, 1, 1, 104,
	}

	payload, err := encodeCompressedDataStageCodebook(compressed, tokenBitWidth12)
	if err != nil {
		t.Fatalf("encodeCompressedDataStageCodebook failed: %v", err)
	}

	decoded, err := decodeCompressedDataStageCodebook(payload, tokenBitWidth12)
	if err != nil {
		t.Fatalf("decodeCompressedDataStageCodebook failed: %v", err)
	}

	if !slices.Equal(decoded, compressed) {
		t.Fatalf("codebook round-trip mismatch")
	}
}

func TestDecodeCompressedDataStageRejectsInvalidCodebookPayload(t *testing.T) {
	// compressedLen=1, codebookLen=1, codebook[0]=7, stream code=1 (out of range)
	payload := []byte{
		0x01, 0x00, 0x00, 0x00,
		0x01, 0x00,
		0x07, 0x00,
		0x01,
	}
	dst := &Archive{}
	err := decodeCompressedDataStage(dst, []byte{stageCompressedDataParamWidth16Codebook}, payload)
	if err == nil {
		t.Fatalf("expected decodeCompressedDataStage to fail on invalid codebook payload")
	}
}

func TestSerializationPacked12BitRejectsOutOfRangeToken(t *testing.T) {
	tokenID := uint16(maxTokenID12Bit + 1)
	tokenBoundaries := make([]uint32, int(tokenID)+2)
	dictionary := make([]byte, int(tokenID)+1)
	for i := range tokenBoundaries {
		tokenBoundaries[i] = uint32(i)
	}

	archive := &Archive{
		CompressedData:          []uint16{tokenID},
		StringBoundaries:        []int{0, 1},
		Dictionary:              dictionary,
		TokenBoundaries:         tokenBoundaries,
		compressedTokenBitWidth: tokenBitWidth12,
	}

	if _, err := archive.WriteTo(io.Discard); err == nil {
		t.Fatalf("expected WriteTo to fail for out-of-range 12-bit token")
	}
}

func TestReadFromSkipsUnknownStage(t *testing.T) {
	input := []string{"user_001", "user_002", "admin_001"}
	archive := mustEncode(NewEncoder(), input)

	var buf bytes.Buffer
	if _, err := archive.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	serialized := append([]byte(nil), buf.Bytes()...)
	if len(serialized) < 8 {
		t.Fatalf("serialized archive too small: %d", len(serialized))
	}

	stageCount := binary.LittleEndian.Uint16(serialized[6:8])
	binary.LittleEndian.PutUint16(serialized[6:8], stageCount+1)

	var extra bytes.Buffer
	if _, err := writeStage(&extra, "unknown.stage", []byte{0x42}, []byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("writeStage failed: %v", err)
	}
	serialized = append(serialized, extra.Bytes()...)

	decoded := &Archive{}
	if _, err := decoded.ReadFrom(bytes.NewReader(serialized)); err != nil {
		t.Fatalf("ReadFrom failed with unknown stage: %v", err)
	}

	buffer := make([]byte, 256)
	for i, expected := range input {
		n, err := decoded.DecompressString(i, buffer)
		if err != nil {
			t.Fatalf("DecompressString(%d) failed: %v", i, err)
		}
		if got := string(buffer[:n]); got != expected {
			t.Fatalf("row %d mismatch: got %q want %q", i, got, expected)
		}
	}
}

func TestReadFromRejectsOversizedStagePayload(t *testing.T) {
	var buf bytes.Buffer
	if _, err := buf.Write([]byte(archiveMagic)); err != nil {
		t.Fatalf("write magic failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, archiveVersion); err != nil {
		t.Fatalf("write version failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint16(1)); err != nil {
		t.Fatalf("write stage count failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint8(1)); err != nil {
		t.Fatalf("write stage name len failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint16(0)); err != nil {
		t.Fatalf("write stage param len failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(maxStagePayloadBytes+1)); err != nil {
		t.Fatalf("write stage payload len failed: %v", err)
	}

	decoded := &Archive{}
	_, err := decoded.ReadFrom(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatalf("expected oversized stage payload error")
	}
}

func TestReadFromRejectsMissingRequiredStages(t *testing.T) {
	var buf bytes.Buffer
	if _, err := buf.Write([]byte(archiveMagic)); err != nil {
		t.Fatalf("write magic failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, archiveVersion); err != nil {
		t.Fatalf("write version failed: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint16(1)); err != nil {
		t.Fatalf("write stage count failed: %v", err)
	}
	if _, err := writeStage(&buf, "unknown.only", nil, []byte{9, 9, 9}); err != nil {
		t.Fatalf("writeStage failed: %v", err)
	}

	decoded := &Archive{}
	_, err := decoded.ReadFrom(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatalf("expected missing stage error")
	}
}

func TestDecompressAllCheckedReturnsErrorOnFailure(t *testing.T) {
	archive := mustEncode(NewEncoder(), []string{"hello", "world"})
	if len(archive.CompressedData) == 0 {
		t.Fatalf("expected compressed data")
	}

	corrupt := &Archive{
		CompressedData:   append([]uint16(nil), archive.CompressedData...),
		StringBoundaries: append([]int(nil), archive.StringBoundaries...),
		Dictionary:       append([]byte(nil), archive.Dictionary...),
		TokenBoundaries:  append([]uint32(nil), archive.TokenBoundaries...),
	}
	corrupt.CompressedData[0] = uint16(len(corrupt.TokenBoundaries) + 1)

	if _, err := corrupt.DecompressAllChecked(make([]byte, 32)); err == nil {
		t.Fatalf("DecompressAllChecked should return an error on decode failure")
	}
}

func TestReadFromErrorIncludesOffsetAndStageIndex(t *testing.T) {
	archive := mustEncode(NewEncoder(), []string{"user_001", "admin_001"})

	var buf bytes.Buffer
	if _, err := archive.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	serialized := buf.Bytes()
	if len(serialized) < 2 {
		t.Fatalf("serialized archive too small: %d", len(serialized))
	}

	truncated := serialized[:len(serialized)-1]
	decoded := &Archive{}
	_, err := decoded.ReadFrom(bytes.NewReader(truncated))
	if err == nil {
		t.Fatalf("expected read error from truncated archive")
	}
	msg := err.Error()
	if !strings.Contains(msg, "offset") {
		t.Fatalf("expected error to include offset, got: %v", err)
	}
	if !strings.Contains(msg, "stage index") {
		t.Fatalf("expected error to include stage index, got: %v", err)
	}
}

func TestSerializationLargeFile(t *testing.T) {
	testFile := "testdata/art_of_war.txt"
	lines, err := loadTestDataLines(testFile)
	if err != nil {
		t.Skipf("Failed to load %s: %v", testFile, err)
	}

	// Calculate original size
	originalSize := 0
	for _, line := range lines {
		originalSize += len(line)
	}

	// Compress
	enc := NewEncoder()
	archive := mustEncode(enc, lines)

	// Serialize
	var buf bytes.Buffer
	_, err = archive.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	serializedSize := buf.Len()
	ratio := float64(originalSize) / float64(serializedSize)
	t.Logf("Original: %d bytes, Serialized: %d bytes, Ratio: %.2fx", originalSize, serializedSize, ratio)

	// Deserialize
	archive2 := &Archive{}
	_, err = archive2.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify all strings decompress correctly
	buffer := make([]byte, 4096)
	for i := 0; i < len(lines); i++ {
		size, err := archive2.DecompressString(i, buffer)
		if err != nil {
			t.Errorf("DecompressString failed: %v", err)
			continue
		}
		decompressed := string(buffer[:size])
		if decompressed != lines[i] {
			t.Errorf("Line %d mismatch after serialization/deserialization", i)
			break
		}
	}
}

func TestDeltaEncodingSavings(t *testing.T) {
	testFile := "testdata/logs_apache_2k.log"
	lines, err := loadTestDataLines(testFile)
	if err != nil {
		t.Skipf("Failed to load %s: %v", testFile, err)
	}

	// Compress
	enc := NewEncoder()
	archive := mustEncode(enc, lines)

	// Serialize with delta encoding
	var buf bytes.Buffer
	_, err = archive.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	actualSerializedSize := buf.Len()
	numBoundaries := len(archive.StringBoundaries)

	// Calculate what the size would have been with absolute encoding
	// Original format: 4 bytes (length) + numBoundaries * 8 bytes
	absoluteBoundariesSize := 4 + numBoundaries*8

	// Calculate current delta encoding size by manually encoding
	deltaOverhead := 4 + 8 + 4 // length + first boundary + delta buf length
	deltaBufSize := 0
	varintBuf := make([]byte, 10)
	for i := 1; i < len(archive.StringBoundaries); i++ {
		delta := archive.StringBoundaries[i] - archive.StringBoundaries[i-1]
		n := binary.PutUvarint(varintBuf, uint64(delta))
		deltaBufSize += n
	}
	deltaBoundariesSize := deltaOverhead + deltaBufSize

	t.Logf("StringBoundaries encoding comparison:")
	t.Logf("  Number of boundaries: %d", numBoundaries)
	t.Logf("  Absolute encoding: 4 (len) + %d × 8 = %d bytes", numBoundaries, absoluteBoundariesSize)
	t.Logf("  Delta encoding: 4 (len) + 8 (first) + 4 (delta len) + %d (deltas) = %d bytes",
		deltaBufSize, deltaBoundariesSize)

	savings := absoluteBoundariesSize - deltaBoundariesSize
	savingsPercent := float64(savings) / float64(absoluteBoundariesSize) * 100

	t.Logf("  Savings: %d bytes (%.1f%% reduction)", savings, savingsPercent)
	t.Logf("  Total serialized size: %d bytes", actualSerializedSize)

	if savingsPercent < 50 {
		t.Errorf("Expected at least 50%% savings from delta encoding, got %.1f%%", savingsPercent)
	}

	// Verify deserialization still works
	archive2 := &Archive{}
	_, err = archive2.ReadFrom(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify boundaries match
	if len(archive2.StringBoundaries) != len(archive.StringBoundaries) {
		t.Fatalf("Boundary count mismatch: got %d, want %d", len(archive2.StringBoundaries), len(archive.StringBoundaries))
	}

	for i := range archive.StringBoundaries {
		if archive2.StringBoundaries[i] != archive.StringBoundaries[i] {
			t.Errorf("Boundary %d mismatch: got %d, want %d", i, archive2.StringBoundaries[i], archive.StringBoundaries[i])
		}
	}
}

func TestEncodeTokenBoundariesStageSelectsDelta(t *testing.T) {
	bounds := []uint32{0, 1, 2, 3, 5, 8, 13, 21}
	archive := &Archive{TokenBoundaries: bounds}

	payload, param, err := encodeTokenBoundariesStage(archive)
	if err != nil {
		t.Fatalf("encodeTokenBoundariesStage failed: %v", err)
	}
	if param != stageTokenBoundariesParamDelta {
		t.Fatalf("param mismatch: got %d want %d", param, stageTokenBoundariesParamDelta)
	}

	expectedPayload, err := encodeTokenBoundariesStageDelta(bounds)
	if err != nil {
		t.Fatalf("encodeTokenBoundariesStageDelta failed: %v", err)
	}
	if !bytes.Equal(payload, expectedPayload) {
		t.Fatalf("delta payload mismatch")
	}

	var decoded Archive
	if err := decodeTokenBoundariesStage(&decoded, []byte{param}, payload); err != nil {
		t.Fatalf("decodeTokenBoundariesStage failed: %v", err)
	}
	if !slices.Equal(decoded.TokenBoundaries, bounds) {
		t.Fatalf("decoded boundaries mismatch: got %v want %v", decoded.TokenBoundaries, bounds)
	}
}

func TestEncodeTokenBoundariesStageSelectsRawWhenSmaller(t *testing.T) {
	bounds := []uint32{0, ^uint32(0)}
	archive := &Archive{TokenBoundaries: bounds}

	payload, param, err := encodeTokenBoundariesStage(archive)
	if err != nil {
		t.Fatalf("encodeTokenBoundariesStage failed: %v", err)
	}
	if param != stageTokenBoundariesParamWidth {
		t.Fatalf("param mismatch: got %d want %d", param, stageTokenBoundariesParamWidth)
	}

	expectedPayload, err := encodeTokenBoundariesStageRaw(bounds)
	if err != nil {
		t.Fatalf("encodeTokenBoundariesStageRaw failed: %v", err)
	}
	if !bytes.Equal(payload, expectedPayload) {
		t.Fatalf("raw payload mismatch")
	}
}

func TestWriteToUsesSelectedTokenBoundariesParam(t *testing.T) {
	dictionary := make([]byte, 10)
	for i := range dictionary {
		dictionary[i] = byte(i)
	}

	archive := &Archive{
		CompressedData:   []uint16{0},
		StringBoundaries: []int{0, 1},
		Dictionary:       dictionary,
		TokenBoundaries:  []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	var blob bytes.Buffer
	if _, err := archive.WriteTo(&blob); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	r := bytes.NewReader(blob.Bytes())
	if _, err := io.CopyN(io.Discard, r, 6); err != nil { // magic + version
		t.Fatalf("skip archive header failed: %v", err)
	}
	var stageCount uint16
	if err := binary.Read(r, binary.LittleEndian, &stageCount); err != nil {
		t.Fatalf("read stage count failed: %v", err)
	}

	found := false
	for i := 0; i < int(stageCount); i++ {
		header, _, err := readStageHeader(r)
		if err != nil {
			t.Fatalf("readStageHeader(%d) failed: %v", i, err)
		}
		params := make([]byte, header.paramLen)
		if _, err := io.ReadFull(r, params); err != nil {
			t.Fatalf("read params(%d) failed: %v", i, err)
		}
		if header.name == stageTokenBoundaries {
			found = true
			if len(params) != 1 {
				t.Fatalf("token_boundaries params length mismatch: got %d want 1", len(params))
			}
			if params[0] != stageTokenBoundariesParamDelta {
				t.Fatalf("token_boundaries param mismatch: got %d want %d", params[0], stageTokenBoundariesParamDelta)
			}
		}
		if _, err := io.CopyN(io.Discard, r, int64(header.dataLen)); err != nil {
			t.Fatalf("skip payload(%d) failed: %v", i, err)
		}
	}
	if !found {
		t.Fatalf("token_boundaries stage not found")
	}
}

// TestSerializationSizes verifies the serialize→deserialize→decode round-trip
// for every configuration and logs the size table recorded in
// TESTDATA_RESULTS.md.

func TestSerializationSizes(t *testing.T) {
	testFiles := []string{
		"testdata/art_of_war.txt",
		"testdata/en_bible_kjv.txt",
		"testdata/en_mobydick.txt",
		"testdata/en_shakespeare.txt",
		"testdata/logs_apache_2k.log",
		"testdata/logs_hdfs_2k.log",
		"testdata/zh_tao_te_ching_en.txt",
	}

	configs := []struct {
		name string
		opts []Option
	}{
		{"Default", nil},
		{"Threshold=10", []Option{WithThreshold(10)}},
		{"MaxTokenID=4095", []Option{WithMaxTokenID(4095)}},
		{"Threshold=10 + MaxTokenID=4095", []Option{WithThreshold(10), WithMaxTokenID(4095)}},
		{"OnPair16", []Option{WithMaxTokenLength(16)}},
	}

	for _, cfg := range configs {
		t.Logf("\n=== OnPair Serialization Sizes (%s) ===", cfg.name)
		t.Logf("%-25s | %12s | %12s | %12s | %12s | %s", "File", "Original", "SpaceUsed", "Full In-Mem", "Serialized", "Ratio")
		t.Log("--------------------------|--------------|--------------|--------------|--------------|-------")

		for _, testFile := range testFiles {
			lines, err := loadTestDataLines(testFile)
			if err != nil {
				t.Logf("Skipping %s: %v", testFile, err)
				continue
			}

			// Calculate original size
			originalSize := 0
			for _, line := range lines {
				originalSize += len(line)
			}

			// Compress with OnPair
			enc := NewEncoder(cfg.opts...)
			archive := mustEncode(enc, lines)
			spaceUsed := archive.SpaceUsed()

			// Calculate full in-memory size (including StringBoundaries)
			// StringBoundaries stored as []int (8 bytes each on 64-bit systems)
			fullInMemory := spaceUsed + len(archive.StringBoundaries)*8

			// Serialize
			var buf bytes.Buffer
			_, err = archive.WriteTo(&buf)
			if err != nil {
				t.Errorf("WriteTo failed for %s: %v", testFile, err)
				continue
			}

			serializedSize := buf.Len()
			ratio := float64(originalSize) / float64(serializedSize)

			loaded := &Archive{}
			if _, err := loaded.ReadFrom(bytes.NewReader(buf.Bytes())); err != nil {
				t.Errorf("ReadFrom failed for %s (%s): %v", testFile, cfg.name, err)
				continue
			}
			decoded, err := loaded.AppendAll(nil)
			if err != nil {
				t.Errorf("AppendAll failed for %s (%s): %v", testFile, cfg.name, err)
				continue
			}
			if string(decoded) != strings.Join(lines, "") {
				t.Errorf("round-trip mismatch for %s (%s)", testFile, cfg.name)
				continue
			}

			// Extract filename
			name := testFile[9:] // Remove "testdata/"
			t.Logf("%-25s | %12d | %12d | %12d | %12d | %.2fx", name, originalSize, spaceUsed, fullInMemory, serializedSize, ratio)
		}
	}
}

// ============================================================================
// Round-trip and fuzz helpers
// ============================================================================

func verifyArchiveRoundTrip(t *testing.T, archive *Archive, rows []string) {
	t.Helper()

	if archive.Rows() != len(rows) {
		t.Fatalf("Rows mismatch: got %d want %d", archive.Rows(), len(rows))
	}

	expectedAll := strings.Join(rows, "")

	for i, want := range rows {
		gotLen, err := archive.DecodedLen(i)
		if err != nil {
			t.Fatalf("DecodedLen(%d) failed: %v", i, err)
		}
		if gotLen != len(want) {
			t.Fatalf("DecodedLen(%d): got %d want %d", i, gotLen, len(want))
		}

		gotAppend, err := archive.AppendRow(nil, i)
		if err != nil {
			t.Fatalf("AppendRow(%d) failed: %v", i, err)
		}
		if string(gotAppend) != want {
			t.Fatalf("AppendRow(%d) mismatch: got %q want %q", i, string(gotAppend), want)
		}

		buf := make([]byte, len(want))
		n, err := archive.DecompressString(i, buf)
		if err != nil {
			t.Fatalf("DecompressString(%d) failed: %v", i, err)
		}
		if n != len(want) {
			t.Fatalf("DecompressString(%d) size mismatch: got %d want %d", i, n, len(want))
		}
		if string(buf[:n]) != want {
			t.Fatalf("DecompressString(%d) mismatch: got %q want %q", i, string(buf[:n]), want)
		}

		if len(want) > 0 {
			_, err = archive.DecompressString(i, make([]byte, len(want)-1))
			if !errors.Is(err, ErrShortBuffer) {
				t.Fatalf("DecompressString(%d) expected ErrShortBuffer, got %v", i, err)
			}
		}
	}

	all, err := archive.AppendAll(nil)
	if err != nil {
		t.Fatalf("AppendAll failed: %v", err)
	}
	if string(all) != expectedAll {
		t.Fatalf("AppendAll mismatch: got %q want %q", string(all), expectedAll)
	}

	allBuf := make([]byte, len(expectedAll))
	n, err := archive.DecompressAllChecked(allBuf)
	if err != nil {
		t.Fatalf("DecompressAllChecked failed: %v", err)
	}
	if n != len(expectedAll) {
		t.Fatalf("DecompressAllChecked size mismatch: got %d want %d", n, len(expectedAll))
	}
	if string(allBuf[:n]) != expectedAll {
		t.Fatalf("DecompressAllChecked mismatch: got %q want %q", string(allBuf[:n]), expectedAll)
	}

	if len(expectedAll) > 0 {
		_, err = archive.DecompressAllChecked(make([]byte, len(expectedAll)-1))
		if !errors.Is(err, ErrShortBuffer) {
			t.Fatalf("DecompressAllChecked expected ErrShortBuffer, got %v", err)
		}
	}

	var blob bytes.Buffer
	if _, err := archive.WriteTo(&blob); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	var loaded Archive
	if _, err := loaded.ReadFrom(bytes.NewReader(blob.Bytes())); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	loadedAll, err := loaded.AppendAll(nil)
	if err != nil {
		t.Fatalf("AppendAll after ReadFrom failed: %v", err)
	}
	if string(loadedAll) != expectedAll {
		t.Fatalf("round-trip mismatch: got %q want %q", string(loadedAll), expectedAll)
	}
}

func FuzzArchiveRoundTrip(f *testing.F) {
	f.Add([]byte("hello"), []byte("world"), []byte("user_000001"))
	f.Add([]byte(""), []byte(""), []byte(""))
	f.Add([]byte("null\x00byte"), []byte("tab\there"), []byte("🚀rocket"))
	f.Add([]byte("aaaaaaaaaaaa"), []byte("bbbbbbbbbbbb"), []byte("cccccccccccc"))

	f.Fuzz(func(t *testing.T, a, b, c []byte) {
		total := len(a) + len(b) + len(c)
		if limitFuzzSize(total) {
			t.Skip()
		}

		rows := []string{
			string(a),
			string(b),
			string(c),
			string(a) + string(b),
			string(b) + string(c),
			string(c) + string(a),
		}

		cases := []struct {
			name string
			opts []Option
		}{
			{name: "default"},
			{name: "maxlen16", opts: []Option{WithMaxTokenLength(16)}},
			{name: "maxid4095", opts: []Option{WithMaxTokenID(4095)}},
			{name: "bitwidth12", opts: []Option{WithTokenBitWidth(12)}},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				archive := mustEncode(NewEncoder(tc.opts...), rows)
				verifyArchiveRoundTrip(t, archive, rows)
			})
		}
	})
}

func FuzzArchiveCorruptionPaths(f *testing.F) {
	f.Add(uint8(0), uint16(0), uint32(0))
	f.Add(uint8(1), uint16(4), uint32(1024))
	f.Add(uint8(2), uint16(8), uint32(1<<31))

	f.Fuzz(func(t *testing.T, op uint8, idx uint16, value uint32) {
		base := mustEncode(NewEncoder(), []string{
			"alpha",
			"beta",
			"gamma",
			"delta",
		})

		archive := &Archive{
			CompressedData:   append([]uint16(nil), base.CompressedData...),
			StringBoundaries: append([]int(nil), base.StringBoundaries...),
			Dictionary:       append([]byte(nil), base.Dictionary...),
			TokenBoundaries:  append([]uint32(nil), base.TokenBoundaries...),
		}

		switch op % 6 {
		case 0:
			if len(archive.CompressedData) > 0 {
				archive.CompressedData[int(idx)%len(archive.CompressedData)] = uint16(value)
			}
		case 1:
			if len(archive.TokenBoundaries) > 0 {
				archive.TokenBoundaries[int(idx)%len(archive.TokenBoundaries)] = value
			}
		case 2:
			if len(archive.StringBoundaries) > 0 {
				archive.StringBoundaries[int(idx)%len(archive.StringBoundaries)] = int(value)
			}
		case 3:
			if len(archive.Dictionary) > 0 {
				archive.Dictionary[int(idx)%len(archive.Dictionary)] = byte(value)
			}
		case 4:
			if len(archive.StringBoundaries) > 2 {
				i := int(idx)%(len(archive.StringBoundaries)-1) + 1
				archive.StringBoundaries[i-1], archive.StringBoundaries[i] = archive.StringBoundaries[i], archive.StringBoundaries[i-1]
			}
		case 5:
			if len(archive.TokenBoundaries) > 1 {
				archive.TokenBoundaries = archive.TokenBoundaries[:len(archive.TokenBoundaries)-1]
			}
		}

		_, _ = archive.DecodedLen(0)
		_, _ = archive.AppendRow(nil, 0)
		_, _ = archive.AppendAll(nil)
		_, _ = archive.DecompressString(0, make([]byte, 64))
		_, _ = archive.DecompressAllChecked(make([]byte, 64))
	})
}

func BenchmarkSerialization(b *testing.B) {
	lines, err := loadTestDataLines("testdata/logs_apache_2k.log")
	if err != nil {
		b.Skipf("testdata unavailable: %v", err)
	}
	// Repeat the corpus so the archive is large enough to be representative.
	rows := make([]string, 0, len(lines)*10)
	for range 10 {
		rows = append(rows, lines...)
	}
	archive := mustEncode(NewEncoder(), rows)
	var buf bytes.Buffer
	if _, err := archive.WriteTo(&buf); err != nil {
		b.Fatal(err)
	}
	wire := buf.Bytes()

	b.Run("WriteTo", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf.Reset()
			if _, err := archive.WriteTo(&buf); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ReadFrom", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decoded := &Archive{}
			if _, err := decoded.ReadFrom(bytes.NewReader(wire)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
