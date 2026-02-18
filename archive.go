package onpair

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

const (
	archiveMagic   = "OPAR"
	archiveVersion = uint16(2)

	stageCompressedData   = "compressed_data"
	stageStringBoundaries = "string_boundaries"
	stageDictionary       = "dictionary"
	stageTokenBoundaries  = "token_boundaries"

	stageCompressedDataParamWidth   = uint8(2)
	stageStringBoundariesParamDelta = uint8(1)
	stageTokenBoundariesParamWidth  = uint8(4)

	maxArchiveStages       = 64
	maxStagePayloadBytes   = 1 << 30 // 1 GiB
	maxCompressedTokenRead = maxStagePayloadBytes / 2
	maxBoundaryCountRead   = maxStagePayloadBytes / (strconv.IntSize / 8)
	maxTokenBoundsRead     = maxStagePayloadBytes / 4
)

// Wire format (version 2):
//
//	magic[4] = "OPAR"
//	version  = uint16 little-endian
//	stageCnt = uint16 little-endian
//	repeat stageCnt times:
//	  nameLen  = uint8
//	  paramLen = uint16 little-endian
//	  dataLen  = uint32 little-endian
//	  name     = nameLen bytes
//	  params   = paramLen bytes
//	  payload  = dataLen bytes
//
// Required stage names:
//
//	compressed_data, string_boundaries, dictionary, token_boundaries
//
// Unknown stages are skipped via dataLen framing.
type wireStageHeader struct {
	name     string
	paramLen uint16
	dataLen  uint32
}

func writeBytes(w io.Writer, b []byte) (int64, error) {
	n, err := w.Write(b)
	if err != nil {
		return int64(n), err
	}
	if n != len(b) {
		return int64(n), io.ErrShortWrite
	}
	return int64(n), nil
}

func writeStage(w io.Writer, name string, params []byte, payload []byte) (int64, error) {
	if len(name) == 0 || len(name) > 255 {
		return 0, fmt.Errorf("invalid stage name length: %d", len(name))
	}
	if len(params) > int(^uint16(0)) {
		return 0, fmt.Errorf("stage params too large for %q: %d", name, len(params))
	}
	if len(payload) > maxStagePayloadBytes {
		return 0, fmt.Errorf("stage payload too large for %q: %d", name, len(payload))
	}

	var total int64
	add := func(n int64) {
		total += n
	}

	nameLen := uint8(len(name))
	if err := binary.Write(w, binary.LittleEndian, nameLen); err != nil {
		return total, err
	}
	add(1)

	paramLen := uint16(len(params))
	if err := binary.Write(w, binary.LittleEndian, paramLen); err != nil {
		return total, err
	}
	add(2)

	dataLen := uint32(len(payload))
	if err := binary.Write(w, binary.LittleEndian, dataLen); err != nil {
		return total, err
	}
	add(4)

	n, err := writeBytes(w, []byte(name))
	add(n)
	if err != nil {
		return total, err
	}

	n, err = writeBytes(w, params)
	add(n)
	if err != nil {
		return total, err
	}

	n, err = writeBytes(w, payload)
	add(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

func readStageHeader(r io.Reader) (wireStageHeader, int64, error) {
	var total int64
	var nameLen uint8
	if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
		return wireStageHeader{}, total, err
	}
	total += 1
	if nameLen == 0 {
		return wireStageHeader{}, total, fmt.Errorf("stage name length must be > 0")
	}

	var paramLen uint16
	if err := binary.Read(r, binary.LittleEndian, &paramLen); err != nil {
		return wireStageHeader{}, total, err
	}
	total += 2

	var dataLen uint32
	if err := binary.Read(r, binary.LittleEndian, &dataLen); err != nil {
		return wireStageHeader{}, total, err
	}
	total += 4
	if dataLen > uint32(maxStagePayloadBytes) {
		return wireStageHeader{}, total, fmt.Errorf("stage payload too large: %d", dataLen)
	}

	nameBytes := make([]byte, int(nameLen))
	n, err := io.ReadFull(r, nameBytes)
	total += int64(n)
	if err != nil {
		return wireStageHeader{}, total, err
	}

	return wireStageHeader{
		name:     string(nameBytes),
		paramLen: paramLen,
		dataLen:  dataLen,
	}, total, nil
}

// Archive holds the compressed data and the dictionary needed to decompress it.
// It replaces the old Dictionary struct.
type Archive struct {
	// Compressed data storage
	CompressedData   []uint16 // Sequence of token IDs
	StringBoundaries []int    // End positions for each string

	// Dictionary storage
	Dictionary      []byte   // Raw token data
	TokenBoundaries []uint32 // Token end positions in dictionary
}

// Rows returns the number of strings encoded in this archive.
func (a *Archive) Rows() int {
	if len(a.StringBoundaries) == 0 {
		return 0
	}
	return len(a.StringBoundaries) - 1
}

// DecodedLen reports the decoded length in bytes for one string.
func (a *Archive) DecodedLen(index int) (int, error) {
	if index < 0 || index >= a.Rows() {
		return 0, fmt.Errorf("index out of bounds: %d", index)
	}

	start := a.StringBoundaries[index]
	end := a.StringBoundaries[index+1]
	if start < 0 || end < start || end > len(a.CompressedData) {
		return 0, fmt.Errorf("corrupted string boundaries for index %d", index)
	}

	tokenBounds := a.TokenBoundaries
	dictionary := a.Dictionary
	dictLen := uint32(len(dictionary))
	boundsLen := len(tokenBounds)

	n := 0
	for tokenPos, tokenID := range a.CompressedData[start:end] {
		absPos := start + tokenPos
		tokenIdx := int(tokenID)
		if tokenIdx+1 >= boundsLen {
			return 0, fmt.Errorf("invalid token ID at row %d token %d (abs %d): %d", index, tokenPos, absPos, tokenID)
		}
		tokenStart := tokenBounds[tokenIdx]
		tokenEnd := tokenBounds[tokenIdx+1]
		if tokenEnd > dictLen || tokenStart > tokenEnd {
			return 0, fmt.Errorf("corrupted token boundaries at row %d token %d (abs %d) for ID %d", index, tokenPos, absPos, tokenID)
		}
		tokenBytes := dictionary[tokenStart:tokenEnd]
		n += len(tokenBytes)
	}
	return n, nil
}

// AppendRow appends the decoded string at index to dst.
func (a *Archive) AppendRow(dst []byte, index int) ([]byte, error) {
	if index < 0 || index >= a.Rows() {
		return dst, fmt.Errorf("index out of bounds: %d", index)
	}

	start := a.StringBoundaries[index]
	end := a.StringBoundaries[index+1]
	if start < 0 || end < start || end > len(a.CompressedData) {
		return dst, fmt.Errorf("corrupted string boundaries for index %d", index)
	}

	tokenBounds := a.TokenBoundaries
	dictionary := a.Dictionary
	dictLen := uint32(len(dictionary))
	boundsLen := len(tokenBounds)

	for tokenPos, tokenID := range a.CompressedData[start:end] {
		absPos := start + tokenPos
		tokenIdx := int(tokenID)
		if tokenIdx+1 >= boundsLen {
			return dst, fmt.Errorf("invalid token ID at row %d token %d (abs %d): %d", index, tokenPos, absPos, tokenID)
		}
		tokenStart := tokenBounds[tokenIdx]
		tokenEnd := tokenBounds[tokenIdx+1]
		if tokenEnd > dictLen || tokenStart > tokenEnd {
			return dst, fmt.Errorf("corrupted token boundaries at row %d token %d (abs %d) for ID %d", index, tokenPos, absPos, tokenID)
		}
		tokenBytes := dictionary[tokenStart:tokenEnd]
		dst = append(dst, tokenBytes...)
	}
	return dst, nil
}

// AppendAll appends all decoded strings to dst.
func (a *Archive) AppendAll(dst []byte) ([]byte, error) {
	tokenBounds := a.TokenBoundaries
	dictionary := a.Dictionary
	dictLen := uint32(len(dictionary))
	boundsLen := len(tokenBounds)

	for tokenPos, tokenID := range a.CompressedData {
		tokenIdx := int(tokenID)
		if tokenIdx+1 >= boundsLen {
			return dst, fmt.Errorf("invalid token ID at token %d: %d", tokenPos, tokenID)
		}
		tokenStart := tokenBounds[tokenIdx]
		tokenEnd := tokenBounds[tokenIdx+1]
		if tokenEnd > dictLen || tokenStart > tokenEnd {
			return dst, fmt.Errorf("corrupted token boundaries at token %d for ID %d", tokenPos, tokenID)
		}
		tokenBytes := dictionary[tokenStart:tokenEnd]
		dst = append(dst, tokenBytes...)
	}
	return dst, nil
}

// DecompressString decompresses a specific string into buffer.
func (a *Archive) DecompressString(index int, buffer []byte) (int, error) {
	if index < 0 || index >= a.Rows() {
		return 0, fmt.Errorf("index out of bounds: %d", index)
	}
	start := a.StringBoundaries[index]
	end := a.StringBoundaries[index+1]
	if start < 0 || end < start || end > len(a.CompressedData) {
		return 0, fmt.Errorf("corrupted string boundaries for index %d", index)
	}

	tokenBounds := a.TokenBoundaries
	dictionary := a.Dictionary
	dictLen := uint32(len(dictionary))
	boundsLen := len(tokenBounds)

	offset := 0
	for tokenPos, tokenID := range a.CompressedData[start:end] {
		absPos := start + tokenPos
		tokenIdx := int(tokenID)
		if tokenIdx+1 >= boundsLen {
			return 0, fmt.Errorf("invalid token ID at row %d token %d (abs %d): %d", index, tokenPos, absPos, tokenID)
		}
		tokenStart := tokenBounds[tokenIdx]
		tokenEnd := tokenBounds[tokenIdx+1]
		if tokenEnd > dictLen || tokenStart > tokenEnd {
			return 0, fmt.Errorf("corrupted token boundaries at row %d token %d (abs %d) for ID %d", index, tokenPos, absPos, tokenID)
		}
		tokenBytes := dictionary[tokenStart:tokenEnd]
		if offset+len(tokenBytes) > len(buffer) {
			return 0, fmt.Errorf("%w at row %d token %d (abs %d): need %d bytes, have %d", ErrShortBuffer, index, tokenPos, absPos, offset+len(tokenBytes), len(buffer))
		}
		copy(buffer[offset:offset+len(tokenBytes)], tokenBytes)
		offset += len(tokenBytes)
	}
	return offset, nil
}

// DecompressAllChecked decompresses all strings into a single buffer.
func (a *Archive) DecompressAllChecked(buffer []byte) (int, error) {
	tokenBounds := a.TokenBoundaries
	dictionary := a.Dictionary
	dictLen := uint32(len(dictionary))
	boundsLen := len(tokenBounds)

	offset := 0
	for tokenPos, tokenID := range a.CompressedData {
		tokenIdx := int(tokenID)
		if tokenIdx+1 >= boundsLen {
			return 0, fmt.Errorf("invalid token ID at token %d: %d", tokenPos, tokenID)
		}
		tokenStart := tokenBounds[tokenIdx]
		tokenEnd := tokenBounds[tokenIdx+1]
		if tokenEnd > dictLen || tokenStart > tokenEnd {
			return 0, fmt.Errorf("corrupted token boundaries at token %d for ID %d", tokenPos, tokenID)
		}
		tokenBytes := dictionary[tokenStart:tokenEnd]
		if offset+len(tokenBytes) > len(buffer) {
			return 0, fmt.Errorf("%w at token %d: need %d bytes, have %d", ErrShortBuffer, tokenPos, offset+len(tokenBytes), len(buffer))
		}
		copy(buffer[offset:offset+len(tokenBytes)], tokenBytes)
		offset += len(tokenBytes)
	}
	return offset, nil
}

// SpaceUsed returns the total space (in bytes) used by the archive.
func (a *Archive) SpaceUsed() int {
	return len(a.CompressedData)*2 +
		len(a.Dictionary) +
		len(a.TokenBoundaries)*4
}

func encodeCompressedDataStage(a *Archive) ([]byte, error) {
	if len(a.CompressedData) > maxCompressedTokenRead {
		return nil, fmt.Errorf("compressed token count too large: %d", len(a.CompressedData))
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(a.CompressedData))); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, a.CompressedData); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeStringBoundariesStage(a *Archive) ([]byte, error) {
	if len(a.StringBoundaries) > maxBoundaryCountRead {
		return nil, fmt.Errorf("string boundary count too large: %d", len(a.StringBoundaries))
	}

	const maxIntValue = int(^uint(0) >> 1)
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(a.StringBoundaries))); err != nil {
		return nil, err
	}

	if len(a.StringBoundaries) == 0 {
		return buf.Bytes(), nil
	}
	if a.StringBoundaries[0] < 0 {
		return nil, fmt.Errorf("first string boundary is negative: %d", a.StringBoundaries[0])
	}
	if a.StringBoundaries[0] > maxIntValue {
		return nil, fmt.Errorf("first string boundary overflows int: %d", a.StringBoundaries[0])
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint64(a.StringBoundaries[0])); err != nil {
		return nil, err
	}

	deltaBuf := make([]byte, 0, len(a.StringBoundaries)*2)
	varintBuf := make([]byte, binary.MaxVarintLen64)
	for i := 1; i < len(a.StringBoundaries); i++ {
		delta := a.StringBoundaries[i] - a.StringBoundaries[i-1]
		if delta < 0 {
			return nil, fmt.Errorf("string boundaries not monotonic at index %d", i)
		}
		n := binary.PutUvarint(varintBuf, uint64(delta))
		deltaBuf = append(deltaBuf, varintBuf[:n]...)
	}

	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(deltaBuf))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(deltaBuf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeDictionaryStage(a *Archive) ([]byte, error) {
	if len(a.Dictionary) > maxStagePayloadBytes {
		return nil, fmt.Errorf("dictionary too large: %d", len(a.Dictionary))
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(a.Dictionary))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(a.Dictionary); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeTokenBoundariesStage(a *Archive) ([]byte, error) {
	if len(a.TokenBoundaries) > maxTokenBoundsRead {
		return nil, fmt.Errorf("token boundary count too large: %d", len(a.TokenBoundaries))
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(a.TokenBoundaries))); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, a.TokenBoundaries); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCompressedDataStage(dst *Archive, params []byte, payload []byte) error {
	if len(params) != 1 || params[0] != stageCompressedDataParamWidth {
		return fmt.Errorf("invalid compressed_data params: %v", params)
	}
	if len(payload) < 4 {
		return fmt.Errorf("compressed_data payload too short: %d", len(payload))
	}

	r := bytes.NewReader(payload)
	var compressedLen uint32
	if err := binary.Read(r, binary.LittleEndian, &compressedLen); err != nil {
		return err
	}
	if compressedLen > uint32(maxCompressedTokenRead) {
		return fmt.Errorf("compressed token count too large: %d", compressedLen)
	}
	expectedBytes := int(compressedLen) * 2
	if r.Len() != expectedBytes {
		return fmt.Errorf("compressed_data length mismatch: payload=%d expected=%d", r.Len(), expectedBytes)
	}

	compressedData := make([]uint16, compressedLen)
	if err := binary.Read(r, binary.LittleEndian, compressedData); err != nil {
		return err
	}
	if r.Len() != 0 {
		return fmt.Errorf("compressed_data trailing bytes: %d", r.Len())
	}
	dst.CompressedData = compressedData
	return nil
}

func decodeStringBoundariesStage(dst *Archive, params []byte, payload []byte) error {
	if len(params) != 1 || params[0] != stageStringBoundariesParamDelta {
		return fmt.Errorf("invalid string_boundaries params: %v", params)
	}
	if len(payload) < 4 {
		return fmt.Errorf("string_boundaries payload too short: %d", len(payload))
	}

	const maxIntValue = uint64(^uint(0) >> 1)
	r := bytes.NewReader(payload)
	var boundariesLen uint32
	if err := binary.Read(r, binary.LittleEndian, &boundariesLen); err != nil {
		return err
	}
	if boundariesLen > uint32(maxBoundaryCountRead) {
		return fmt.Errorf("string boundary count too large: %d", boundariesLen)
	}

	if boundariesLen == 0 {
		if r.Len() != 0 {
			return fmt.Errorf("string_boundaries trailing bytes: %d", r.Len())
		}
		dst.StringBoundaries = nil
		return nil
	}
	if r.Len() < 12 {
		return fmt.Errorf("string_boundaries missing first boundary or delta length")
	}

	var firstBoundary uint64
	if err := binary.Read(r, binary.LittleEndian, &firstBoundary); err != nil {
		return err
	}
	if firstBoundary > maxIntValue {
		return fmt.Errorf("first string boundary overflows int: %d", firstBoundary)
	}

	var deltaBufLen uint32
	if err := binary.Read(r, binary.LittleEndian, &deltaBufLen); err != nil {
		return err
	}
	if deltaBufLen > uint32(r.Len()) {
		return fmt.Errorf("delta buffer length %d exceeds remaining payload %d", deltaBufLen, r.Len())
	}
	if boundariesLen-1 > deltaBufLen {
		return fmt.Errorf("delta buffer too short for %d boundaries: %d", boundariesLen, deltaBufLen)
	}

	deltaBuf := make([]byte, int(deltaBufLen))
	if _, err := io.ReadFull(r, deltaBuf); err != nil {
		return err
	}
	if r.Len() != 0 {
		return fmt.Errorf("string_boundaries trailing bytes: %d", r.Len())
	}

	boundaries := make([]int, boundariesLen)
	boundaries[0] = int(firstBoundary)
	offset := 0
	prev := boundaries[0]
	for i := 1; i < len(boundaries); i++ {
		delta, n := binary.Uvarint(deltaBuf[offset:])
		if n <= 0 {
			return fmt.Errorf("failed to decode boundary delta at index %d", i)
		}
		offset += n
		if delta > maxIntValue-uint64(prev) {
			return fmt.Errorf("boundary delta overflows int at index %d", i)
		}
		prev += int(delta)
		boundaries[i] = prev
	}
	if offset != len(deltaBuf) {
		return fmt.Errorf("unused bytes in boundary delta buffer: %d", len(deltaBuf)-offset)
	}

	dst.StringBoundaries = boundaries
	return nil
}

func decodeDictionaryStage(dst *Archive, params []byte, payload []byte) error {
	if len(params) != 0 {
		return fmt.Errorf("invalid dictionary params: %v", params)
	}
	if len(payload) < 4 {
		return fmt.Errorf("dictionary payload too short: %d", len(payload))
	}

	r := bytes.NewReader(payload)
	var dictLen uint32
	if err := binary.Read(r, binary.LittleEndian, &dictLen); err != nil {
		return err
	}
	if dictLen > uint32(maxStagePayloadBytes) {
		return fmt.Errorf("dictionary length too large: %d", dictLen)
	}
	if r.Len() != int(dictLen) {
		return fmt.Errorf("dictionary length mismatch: payload=%d expected=%d", r.Len(), dictLen)
	}

	dictionary := make([]byte, dictLen)
	if _, err := io.ReadFull(r, dictionary); err != nil {
		return err
	}
	if r.Len() != 0 {
		return fmt.Errorf("dictionary trailing bytes: %d", r.Len())
	}
	dst.Dictionary = dictionary
	return nil
}

func decodeTokenBoundariesStage(dst *Archive, params []byte, payload []byte) error {
	if len(params) != 1 || params[0] != stageTokenBoundariesParamWidth {
		return fmt.Errorf("invalid token_boundaries params: %v", params)
	}
	if len(payload) < 4 {
		return fmt.Errorf("token_boundaries payload too short: %d", len(payload))
	}

	r := bytes.NewReader(payload)
	var tokenLen uint32
	if err := binary.Read(r, binary.LittleEndian, &tokenLen); err != nil {
		return err
	}
	if tokenLen > uint32(maxTokenBoundsRead) {
		return fmt.Errorf("token boundary count too large: %d", tokenLen)
	}
	expectedBytes := int(tokenLen) * 4
	if r.Len() != expectedBytes {
		return fmt.Errorf("token_boundaries length mismatch: payload=%d expected=%d", r.Len(), expectedBytes)
	}

	tokenBoundaries := make([]uint32, tokenLen)
	if err := binary.Read(r, binary.LittleEndian, tokenBoundaries); err != nil {
		return err
	}
	if r.Len() != 0 {
		return fmt.Errorf("token_boundaries trailing bytes: %d", r.Len())
	}
	dst.TokenBoundaries = tokenBoundaries
	return nil
}

func validateArchiveStructure(a *Archive) error {
	if len(a.StringBoundaries) == 0 {
		return fmt.Errorf("string boundaries must contain at least one entry")
	}
	if a.StringBoundaries[0] != 0 {
		return fmt.Errorf("first string boundary must be 0: %d", a.StringBoundaries[0])
	}
	for i := 1; i < len(a.StringBoundaries); i++ {
		if a.StringBoundaries[i] < a.StringBoundaries[i-1] {
			return fmt.Errorf("string boundaries not monotonic at index %d", i)
		}
	}
	if last := a.StringBoundaries[len(a.StringBoundaries)-1]; last > len(a.CompressedData) {
		return fmt.Errorf("string boundary %d out of range for %d tokens", last, len(a.CompressedData))
	}

	if len(a.TokenBoundaries) == 0 {
		return fmt.Errorf("token boundaries must contain at least one entry")
	}
	if a.TokenBoundaries[0] != 0 {
		return fmt.Errorf("first token boundary must be 0: %d", a.TokenBoundaries[0])
	}
	for i := 1; i < len(a.TokenBoundaries); i++ {
		if a.TokenBoundaries[i] < a.TokenBoundaries[i-1] {
			return fmt.Errorf("token boundaries not monotonic at index %d", i)
		}
	}
	if last := a.TokenBoundaries[len(a.TokenBoundaries)-1]; int(last) > len(a.Dictionary) {
		return fmt.Errorf("token boundary %d out of range for dictionary size %d", last, len(a.Dictionary))
	}
	for i, tokenID := range a.CompressedData {
		if int(tokenID)+1 >= len(a.TokenBoundaries) {
			return fmt.Errorf("compressed token out of range at index %d: %d", i, tokenID)
		}
	}
	return nil
}

// WriteTo serializes the Archive to an io.Writer.
func (a *Archive) WriteTo(w io.Writer) (int64, error) {
	if err := validateArchiveStructure(a); err != nil {
		return 0, fmt.Errorf("invalid archive: %w", err)
	}

	compressedPayload, err := encodeCompressedDataStage(a)
	if err != nil {
		return 0, err
	}
	stringBoundariesPayload, err := encodeStringBoundariesStage(a)
	if err != nil {
		return 0, err
	}
	dictionaryPayload, err := encodeDictionaryStage(a)
	if err != nil {
		return 0, err
	}
	tokenBoundariesPayload, err := encodeTokenBoundariesStage(a)
	if err != nil {
		return 0, err
	}

	stages := []struct {
		name    string
		params  []byte
		payload []byte
	}{
		{
			name:    stageCompressedData,
			params:  []byte{stageCompressedDataParamWidth},
			payload: compressedPayload,
		},
		{
			name:    stageStringBoundaries,
			params:  []byte{stageStringBoundariesParamDelta},
			payload: stringBoundariesPayload,
		},
		{
			name:    stageDictionary,
			params:  nil,
			payload: dictionaryPayload,
		},
		{
			name:    stageTokenBoundaries,
			params:  []byte{stageTokenBoundariesParamWidth},
			payload: tokenBoundariesPayload,
		},
	}

	var total int64
	n, err := writeBytes(w, []byte(archiveMagic))
	total += n
	if err != nil {
		return total, err
	}

	if err := binary.Write(w, binary.LittleEndian, archiveVersion); err != nil {
		return total, err
	}
	total += 2

	if err := binary.Write(w, binary.LittleEndian, uint16(len(stages))); err != nil {
		return total, err
	}
	total += 2

	for _, stage := range stages {
		n, err := writeStage(w, stage.name, stage.params, stage.payload)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// ReadFrom deserializes an Archive from an io.Reader.
func (a *Archive) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	var magic [4]byte
	magicOffset := total
	n, err := io.ReadFull(r, magic[:])
	total += int64(n)
	if err != nil {
		return total, fmt.Errorf("read archive magic at offset %d: %w", magicOffset, err)
	}
	if string(magic[:]) != archiveMagic {
		return total, fmt.Errorf("invalid archive magic at offset %d: %q", magicOffset, string(magic[:]))
	}

	var version uint16
	versionOffset := total
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return total, fmt.Errorf("read archive version at offset %d: %w", versionOffset, err)
	}
	total += 2
	if version != archiveVersion {
		return total, fmt.Errorf("unsupported archive version at offset %d: %d", versionOffset, version)
	}

	var stageCount uint16
	stageCountOffset := total
	if err := binary.Read(r, binary.LittleEndian, &stageCount); err != nil {
		return total, fmt.Errorf("read stage count at offset %d: %w", stageCountOffset, err)
	}
	total += 2
	if stageCount == 0 || stageCount > maxArchiveStages {
		return total, fmt.Errorf("invalid stage count at offset %d: %d", stageCountOffset, stageCount)
	}

	tmp := Archive{}
	seenStages := make(map[string]bool, stageCount)
	var paramsScratch []byte
	var payloadScratch []byte

	for i := 0; i < int(stageCount); i++ {
		headerOffset := total
		header, n, err := readStageHeader(r)
		total += n
		if err != nil {
			return total, fmt.Errorf("read stage header at offset %d (stage index %d): %w", headerOffset, i, err)
		}
		if seenStages[header.name] {
			return total, fmt.Errorf("duplicate stage %q at stage index %d", header.name, i)
		}

		paramsLen := int(header.paramLen)
		if cap(paramsScratch) < paramsLen {
			paramsScratch = make([]byte, paramsLen)
		}
		params := paramsScratch[:paramsLen]
		paramsOffset := total
		nParams, err := io.ReadFull(r, params)
		total += int64(nParams)
		if err != nil {
			return total, fmt.Errorf("read stage %q params at offset %d (stage index %d): %w", header.name, paramsOffset, i, err)
		}

		switch header.name {
		case stageCompressedData, stageStringBoundaries, stageDictionary, stageTokenBoundaries:
			payloadLen := int(header.dataLen)
			if cap(payloadScratch) < payloadLen {
				payloadScratch = make([]byte, payloadLen)
			}
			payload := payloadScratch[:payloadLen]
			payloadOffset := total
			nPayload, err := io.ReadFull(r, payload)
			total += int64(nPayload)
			if err != nil {
				return total, fmt.Errorf("read stage %q payload at offset %d (stage index %d): %w", header.name, payloadOffset, i, err)
			}

			switch header.name {
			case stageCompressedData:
				if err := decodeCompressedDataStage(&tmp, params, payload); err != nil {
					return total, fmt.Errorf("decode stage %q at offset %d (stage index %d): %w", header.name, payloadOffset, i, err)
				}
			case stageStringBoundaries:
				if err := decodeStringBoundariesStage(&tmp, params, payload); err != nil {
					return total, fmt.Errorf("decode stage %q at offset %d (stage index %d): %w", header.name, payloadOffset, i, err)
				}
			case stageDictionary:
				if err := decodeDictionaryStage(&tmp, params, payload); err != nil {
					return total, fmt.Errorf("decode stage %q at offset %d (stage index %d): %w", header.name, payloadOffset, i, err)
				}
			case stageTokenBoundaries:
				if err := decodeTokenBoundariesStage(&tmp, params, payload); err != nil {
					return total, fmt.Errorf("decode stage %q at offset %d (stage index %d): %w", header.name, payloadOffset, i, err)
				}
			}
			seenStages[header.name] = true

		default:
			skipOffset := total
			skipped, err := io.CopyN(io.Discard, r, int64(header.dataLen))
			total += skipped
			if err != nil {
				return total, fmt.Errorf("skip unknown stage %q at offset %d (stage index %d): %w", header.name, skipOffset, i, err)
			}
		}
	}

	requiredStages := []string{
		stageCompressedData,
		stageStringBoundaries,
		stageDictionary,
		stageTokenBoundaries,
	}
	for _, stageName := range requiredStages {
		if !seenStages[stageName] {
			return total, fmt.Errorf("missing required stage %q", stageName)
		}
	}
	if err := validateArchiveStructure(&tmp); err != nil {
		return total, fmt.Errorf("invalid archive structure: %w", err)
	}

	*a = tmp
	return total, nil
}
