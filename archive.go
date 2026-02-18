package onpair

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const (
	archiveMagic   = "OPAR"
	archiveVersion = uint16(2)

	stageCompressedData   = "compressed_data"
	stageStringBoundaries = "string_boundaries"
	stageDictionary       = "dictionary"
	stageTokenBoundaries  = "token_boundaries"

	stageCompressedDataParamWidth16              = uint8(2)  // raw legacy 16-bit (2-byte) token IDs
	stageCompressedDataParamWidth16Flate         = uint8(3)  // flate(raw 16-bit payload)
	stageCompressedDataParamWidth16Codebook      = uint8(4)  // byte codebook + escape stream (16-bit token IDs)
	stageCompressedDataParamWidth16CodebookFlate = uint8(5)  // flate(codebook stream for 16-bit token IDs)
	stageCompressedDataParamWidth12              = uint8(12) // raw packed 12-bit token IDs
	stageCompressedDataParamWidth12Flate         = uint8(13) // flate(raw 12-bit payload)
	stageCompressedDataParamWidth12Codebook      = uint8(14) // byte codebook + escape stream (12-bit token IDs)
	stageCompressedDataParamWidth12CodebookFlate = uint8(15) // flate(codebook stream for 12-bit token IDs)
	stageStringBoundariesParamDelta              = uint8(1)
	stageTokenBoundariesParamWidth               = uint8(4) // raw uint32 boundaries
	stageTokenBoundariesParamDelta               = uint8(5) // first boundary + varint deltas

	maxArchiveStages       = 64
	maxStagePayloadBytes   = 1 << 30 // 1 GiB
	maxCompressedTokenRead = maxStagePayloadBytes / 2
	maxBoundaryCountRead   = maxStagePayloadBytes / 8
	maxTokenBoundsRead     = maxStagePayloadBytes / 4
	maxStringBoundaryValue = uint64(maxCompressedTokenRead)

	compressedDataCodebookEscapeByte = uint8(0xFF)
	compressedDataCodebookMaxEntries = int(compressedDataCodebookEscapeByte)
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

	// Internal encoding metadata for compressed token stream.
	compressedTokenBitWidth uint8
}

func (a *Archive) tokenBitWidth() uint8 {
	switch a.compressedTokenBitWidth {
	case tokenBitWidth12:
		return tokenBitWidth12
	case tokenBitWidth16:
		return tokenBitWidth16
	default:
		return tokenBitWidth16
	}
}

func packed12ByteSize(tokenCount int) int {
	return (tokenCount*int(tokenBitWidth12) + 7) / 8
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
	compressedBytes := len(a.CompressedData) * 2
	if a.tokenBitWidth() == tokenBitWidth12 {
		compressedBytes = packed12ByteSize(len(a.CompressedData))
	}

	return compressedBytes +
		len(a.Dictionary) +
		len(a.TokenBoundaries)*4
}

func encodeCompressedDataStage(a *Archive) ([]byte, uint8, error) {
	if len(a.CompressedData) > maxCompressedTokenRead {
		return nil, 0, fmt.Errorf("compressed token count too large: %d", len(a.CompressedData))
	}

	var (
		rawPayload        []byte
		rawParam          uint8
		flateParam        uint8
		codebookParam     uint8
		codebookFlateParm uint8
		encodeRawErr      error
	)
	switch a.tokenBitWidth() {
	case tokenBitWidth12:
		rawPayload, encodeRawErr = encodeCompressedDataStage12(a.CompressedData)
		rawParam = stageCompressedDataParamWidth12
		flateParam = stageCompressedDataParamWidth12Flate
		codebookParam = stageCompressedDataParamWidth12Codebook
		codebookFlateParm = stageCompressedDataParamWidth12CodebookFlate
	case tokenBitWidth16:
		rawPayload, encodeRawErr = encodeCompressedDataStage16(a.CompressedData)
		rawParam = stageCompressedDataParamWidth16
		flateParam = stageCompressedDataParamWidth16Flate
		codebookParam = stageCompressedDataParamWidth16Codebook
		codebookFlateParm = stageCompressedDataParamWidth16CodebookFlate
	default:
		return nil, 0, fmt.Errorf("unsupported token bit-width: %d", a.compressedTokenBitWidth)
	}
	if encodeRawErr != nil {
		return nil, 0, encodeRawErr
	}

	type candidate struct {
		payload []byte
		param   uint8
	}
	candidates := []candidate{
		{payload: rawPayload, param: rawParam},
	}

	flatePayload, err := encodeFlatePayload(rawPayload)
	if err != nil {
		return nil, 0, err
	}
	candidates = append(candidates, candidate{payload: flatePayload, param: flateParam})

	codebookPayload, codebookErr := encodeCompressedDataStageCodebook(a.CompressedData, a.tokenBitWidth())
	if codebookErr == nil {
		candidates = append(candidates, candidate{payload: codebookPayload, param: codebookParam})

		flateCodebook, err := encodeFlatePayload(codebookPayload)
		if err != nil {
			return nil, 0, err
		}
		candidates = append(candidates, candidate{payload: flateCodebook, param: codebookFlateParm})
	}

	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if len(candidate.payload) < len(best.payload) {
			best = candidate
		}
	}
	return best.payload, best.param, nil
}

func encodeFlatePayload(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(raw); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeFlatePayload(payload []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(payload))
	defer r.Close()

	limited := io.LimitReader(r, maxStagePayloadBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxStagePayloadBytes {
		return nil, fmt.Errorf("flate payload expands beyond limit")
	}
	return raw, nil
}

type tokenFrequency struct {
	tokenID uint16
	count   uint32
}

func encodeCompressedDataStageCodebook(compressed []uint16, tokenBitWidth uint8) ([]byte, error) {
	if len(compressed) > maxCompressedTokenRead {
		return nil, fmt.Errorf("compressed token count too large: %d", len(compressed))
	}

	var maxTokenID uint16
	for i, tokenID := range compressed {
		if tokenBitWidth == tokenBitWidth12 && tokenID > maxTokenID12Bit {
			return nil, fmt.Errorf("compressed token out of 12-bit range at index %d: %d", i, tokenID)
		}
		if tokenID > maxTokenID {
			maxTokenID = tokenID
		}
	}

	counts := make([]uint32, int(maxTokenID)+1)
	for _, tokenID := range compressed {
		counts[tokenID]++
	}

	frequencies := make([]tokenFrequency, 0, len(counts))
	for tokenID, count := range counts {
		if count == 0 {
			continue
		}
		frequencies = append(frequencies, tokenFrequency{
			tokenID: uint16(tokenID),
			count:   count,
		})
	}
	sort.Slice(frequencies, func(i, j int) bool {
		if frequencies[i].count != frequencies[j].count {
			return frequencies[i].count > frequencies[j].count
		}
		return frequencies[i].tokenID < frequencies[j].tokenID
	})

	codebookLen := len(frequencies)
	if codebookLen > compressedDataCodebookMaxEntries {
		codebookLen = compressedDataCodebookMaxEntries
	}

	codeByToken := make([]uint8, len(counts))
	for i := range codeByToken {
		codeByToken[i] = compressedDataCodebookEscapeByte
	}
	for i := 0; i < codebookLen; i++ {
		codeByToken[frequencies[i].tokenID] = uint8(i)
	}

	payloadLen := uint64(4 + 2 + codebookLen*2)
	for _, tokenID := range compressed {
		if codeByToken[tokenID] == compressedDataCodebookEscapeByte {
			payloadLen += 3
			continue
		}
		payloadLen++
	}
	if payloadLen > uint64(maxStagePayloadBytes) {
		return nil, fmt.Errorf("compressed_data codebook payload too large: %d", payloadLen)
	}

	payload := make([]byte, int(payloadLen))
	binary.LittleEndian.PutUint32(payload[:4], uint32(len(compressed)))
	binary.LittleEndian.PutUint16(payload[4:6], uint16(codebookLen))

	outIdx := 6
	for i := 0; i < codebookLen; i++ {
		binary.LittleEndian.PutUint16(payload[outIdx:outIdx+2], frequencies[i].tokenID)
		outIdx += 2
	}

	for _, tokenID := range compressed {
		code := codeByToken[tokenID]
		if code != compressedDataCodebookEscapeByte {
			payload[outIdx] = code
			outIdx++
			continue
		}
		payload[outIdx] = compressedDataCodebookEscapeByte
		outIdx++
		binary.LittleEndian.PutUint16(payload[outIdx:outIdx+2], tokenID)
		outIdx += 2
	}

	if outIdx != len(payload) {
		return nil, fmt.Errorf("compressed_data codebook payload mismatch: wrote %d bytes, expected %d", outIdx, len(payload))
	}
	return payload, nil
}

func decodeCompressedDataStageCodebook(payload []byte, tokenBitWidth uint8) ([]uint16, error) {
	if len(payload) < 6 {
		return nil, fmt.Errorf("compressed_data codebook payload too short: %d", len(payload))
	}

	compressedLen := binary.LittleEndian.Uint32(payload[:4])
	if compressedLen > uint32(maxCompressedTokenRead) {
		return nil, fmt.Errorf("compressed token count too large: %d", compressedLen)
	}

	codebookLen := int(binary.LittleEndian.Uint16(payload[4:6]))
	if codebookLen > compressedDataCodebookMaxEntries {
		return nil, fmt.Errorf("compressed_data codebook length too large: %d", codebookLen)
	}

	headerLen := 6 + codebookLen*2
	if len(payload) < headerLen {
		return nil, fmt.Errorf("compressed_data codebook payload too short for dictionary: %d", len(payload))
	}

	codebook := make([]uint16, codebookLen)
	inIdx := 6
	for i := 0; i < codebookLen; i++ {
		tokenID := binary.LittleEndian.Uint16(payload[inIdx : inIdx+2])
		inIdx += 2
		if tokenBitWidth == tokenBitWidth12 && tokenID > maxTokenID12Bit {
			return nil, fmt.Errorf("compressed_data codebook token out of 12-bit range at code %d: %d", i, tokenID)
		}
		codebook[i] = tokenID
	}

	stream := payload[headerLen:]
	compressedData := make([]uint16, compressedLen)
	inIdx = 0
	for i := 0; i < len(compressedData); i++ {
		if inIdx >= len(stream) {
			return nil, fmt.Errorf("compressed_data codebook payload underrun at token %d", i)
		}

		code := stream[inIdx]
		inIdx++
		if code != compressedDataCodebookEscapeByte {
			codeIdx := int(code)
			if codeIdx >= len(codebook) {
				return nil, fmt.Errorf("compressed_data codebook index out of range at token %d: %d", i, codeIdx)
			}
			compressedData[i] = codebook[codeIdx]
			continue
		}

		if inIdx+2 > len(stream) {
			return nil, fmt.Errorf("compressed_data codebook escape underrun at token %d", i)
		}
		tokenID := binary.LittleEndian.Uint16(stream[inIdx : inIdx+2])
		inIdx += 2
		if tokenBitWidth == tokenBitWidth12 && tokenID > maxTokenID12Bit {
			return nil, fmt.Errorf("compressed_data codebook escape token out of 12-bit range at token %d: %d", i, tokenID)
		}
		compressedData[i] = tokenID
	}

	if inIdx != len(stream) {
		return nil, fmt.Errorf("compressed_data codebook trailing bytes: %d", len(stream)-inIdx)
	}
	return compressedData, nil
}

func encodeCompressedDataStage16(compressed []uint16) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(compressed))); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, compressed); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeCompressedDataStage12(compressed []uint16) ([]byte, error) {
	for i, tokenID := range compressed {
		if tokenID > maxTokenID12Bit {
			return nil, fmt.Errorf("compressed token out of 12-bit range at index %d: %d", i, tokenID)
		}
	}

	packedLen := packed12ByteSize(len(compressed))
	payload := make([]byte, 4+packedLen)
	binary.LittleEndian.PutUint32(payload[:4], uint32(len(compressed)))
	packed := payload[4:]

	outIdx := 0
	var bitBuf uint32
	bitsInBuf := 0
	for _, tokenID := range compressed {
		bitBuf |= uint32(tokenID) << bitsInBuf
		bitsInBuf += int(tokenBitWidth12)
		for bitsInBuf >= 8 {
			packed[outIdx] = byte(bitBuf)
			outIdx++
			bitBuf >>= 8
			bitsInBuf -= 8
		}
	}
	if bitsInBuf > 0 {
		packed[outIdx] = byte(bitBuf)
		outIdx++
	}
	if outIdx != len(packed) {
		return nil, fmt.Errorf("packed 12-bit payload mismatch: wrote %d bytes, expected %d", outIdx, len(packed))
	}

	return payload, nil
}

func encodeStringBoundariesStage(a *Archive) ([]byte, error) {
	if len(a.StringBoundaries) > maxBoundaryCountRead {
		return nil, fmt.Errorf("string boundary count too large: %d", len(a.StringBoundaries))
	}

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
	if uint64(a.StringBoundaries[0]) > maxStringBoundaryValue {
		return nil, fmt.Errorf("first string boundary exceeds max supported value: %d", a.StringBoundaries[0])
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

func encodeTokenBoundariesStage(a *Archive) ([]byte, uint8, error) {
	if len(a.TokenBoundaries) > maxTokenBoundsRead {
		return nil, 0, fmt.Errorf("token boundary count too large: %d", len(a.TokenBoundaries))
	}

	rawPayload, err := encodeTokenBoundariesStageRaw(a.TokenBoundaries)
	if err != nil {
		return nil, 0, err
	}
	deltaPayload, err := encodeTokenBoundariesStageDelta(a.TokenBoundaries)
	if err != nil {
		return nil, 0, err
	}
	if len(deltaPayload) < len(rawPayload) {
		return deltaPayload, stageTokenBoundariesParamDelta, nil
	}
	return rawPayload, stageTokenBoundariesParamWidth, nil
}

func encodeTokenBoundariesStageRaw(bounds []uint32) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(bounds))); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, bounds); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeTokenBoundariesStageDelta(bounds []uint32) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(bounds))); err != nil {
		return nil, err
	}

	if len(bounds) == 0 {
		return buf.Bytes(), nil
	}
	if err := binary.Write(&buf, binary.LittleEndian, bounds[0]); err != nil {
		return nil, err
	}

	deltaBuf := make([]byte, 0, len(bounds)*2)
	varintBuf := make([]byte, binary.MaxVarintLen64)
	for i := 1; i < len(bounds); i++ {
		if bounds[i] < bounds[i-1] {
			return nil, fmt.Errorf("token boundaries not monotonic at index %d", i)
		}
		delta := bounds[i] - bounds[i-1]
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

func decodeCompressedDataStage(dst *Archive, params []byte, payload []byte) error {
	if len(params) != 1 {
		return fmt.Errorf("invalid compressed_data params: %v", params)
	}

	switch params[0] {
	case stageCompressedDataParamWidth16:
		compressedData, err := decodeCompressedDataStage16(payload)
		if err != nil {
			return err
		}
		dst.CompressedData = compressedData
		dst.compressedTokenBitWidth = tokenBitWidth16
		return nil
	case stageCompressedDataParamWidth12:
		compressedData, err := decodeCompressedDataStage12(payload)
		if err != nil {
			return err
		}
		dst.CompressedData = compressedData
		dst.compressedTokenBitWidth = tokenBitWidth12
		return nil
	case stageCompressedDataParamWidth16Flate:
		rawPayload, err := decodeFlatePayload(payload)
		if err != nil {
			return err
		}
		compressedData, err := decodeCompressedDataStage16(rawPayload)
		if err != nil {
			return err
		}
		dst.CompressedData = compressedData
		dst.compressedTokenBitWidth = tokenBitWidth16
		return nil
	case stageCompressedDataParamWidth12Flate:
		rawPayload, err := decodeFlatePayload(payload)
		if err != nil {
			return err
		}
		compressedData, err := decodeCompressedDataStage12(rawPayload)
		if err != nil {
			return err
		}
		dst.CompressedData = compressedData
		dst.compressedTokenBitWidth = tokenBitWidth12
		return nil
	case stageCompressedDataParamWidth16Codebook:
		compressedData, err := decodeCompressedDataStageCodebook(payload, tokenBitWidth16)
		if err != nil {
			return err
		}
		dst.CompressedData = compressedData
		dst.compressedTokenBitWidth = tokenBitWidth16
		return nil
	case stageCompressedDataParamWidth16CodebookFlate:
		rawPayload, err := decodeFlatePayload(payload)
		if err != nil {
			return err
		}
		compressedData, err := decodeCompressedDataStageCodebook(rawPayload, tokenBitWidth16)
		if err != nil {
			return err
		}
		dst.CompressedData = compressedData
		dst.compressedTokenBitWidth = tokenBitWidth16
		return nil
	case stageCompressedDataParamWidth12Codebook:
		compressedData, err := decodeCompressedDataStageCodebook(payload, tokenBitWidth12)
		if err != nil {
			return err
		}
		dst.CompressedData = compressedData
		dst.compressedTokenBitWidth = tokenBitWidth12
		return nil
	case stageCompressedDataParamWidth12CodebookFlate:
		rawPayload, err := decodeFlatePayload(payload)
		if err != nil {
			return err
		}
		compressedData, err := decodeCompressedDataStageCodebook(rawPayload, tokenBitWidth12)
		if err != nil {
			return err
		}
		dst.CompressedData = compressedData
		dst.compressedTokenBitWidth = tokenBitWidth12
		return nil
	default:
		return fmt.Errorf("invalid compressed_data params: %v", params)
	}
}

func decodeCompressedDataStage16(payload []byte) ([]uint16, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("compressed_data payload too short: %d", len(payload))
	}

	r := bytes.NewReader(payload)
	var compressedLen uint32
	if err := binary.Read(r, binary.LittleEndian, &compressedLen); err != nil {
		return nil, err
	}
	if compressedLen > uint32(maxCompressedTokenRead) {
		return nil, fmt.Errorf("compressed token count too large: %d", compressedLen)
	}
	expectedBytes := int(compressedLen) * 2
	if r.Len() != expectedBytes {
		return nil, fmt.Errorf("compressed_data length mismatch: payload=%d expected=%d", r.Len(), expectedBytes)
	}

	compressedData := make([]uint16, compressedLen)
	if err := binary.Read(r, binary.LittleEndian, compressedData); err != nil {
		return nil, err
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("compressed_data trailing bytes: %d", r.Len())
	}
	return compressedData, nil
}

func decodeCompressedDataStage12(payload []byte) ([]uint16, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("compressed_data payload too short: %d", len(payload))
	}

	r := bytes.NewReader(payload)
	var compressedLen uint32
	if err := binary.Read(r, binary.LittleEndian, &compressedLen); err != nil {
		return nil, err
	}
	if compressedLen > uint32(maxCompressedTokenRead) {
		return nil, fmt.Errorf("compressed token count too large: %d", compressedLen)
	}

	expectedBytes := packed12ByteSize(int(compressedLen))
	if r.Len() != expectedBytes {
		return nil, fmt.Errorf("compressed_data length mismatch: payload=%d expected=%d", r.Len(), expectedBytes)
	}

	packed := make([]byte, expectedBytes)
	if _, err := io.ReadFull(r, packed); err != nil {
		return nil, err
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("compressed_data trailing bytes: %d", r.Len())
	}

	compressedData := make([]uint16, compressedLen)
	inIdx := 0
	var bitBuf uint32
	bitsInBuf := 0
	for i := 0; i < len(compressedData); i++ {
		for bitsInBuf < int(tokenBitWidth12) {
			if inIdx >= len(packed) {
				return nil, fmt.Errorf("compressed_data 12-bit payload underrun at token %d", i)
			}
			bitBuf |= uint32(packed[inIdx]) << bitsInBuf
			inIdx++
			bitsInBuf += 8
		}
		compressedData[i] = uint16(bitBuf & uint32(maxTokenID12Bit))
		bitBuf >>= tokenBitWidth12
		bitsInBuf -= int(tokenBitWidth12)
	}
	if inIdx != len(packed) {
		return nil, fmt.Errorf("compressed_data 12-bit payload overrun: used %d bytes, have %d", inIdx, len(packed))
	}
	if bitBuf != 0 {
		return nil, fmt.Errorf("compressed_data 12-bit payload has non-zero padding")
	}
	return compressedData, nil
}

func decodeStringBoundariesStage(dst *Archive, params []byte, payload []byte) error {
	if len(params) != 1 || params[0] != stageStringBoundariesParamDelta {
		return fmt.Errorf("invalid string_boundaries params: %v", params)
	}
	if len(payload) < 4 {
		return fmt.Errorf("string_boundaries payload too short: %d", len(payload))
	}

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
	if firstBoundary > maxStringBoundaryValue {
		return fmt.Errorf("first string boundary exceeds max supported value: %d", firstBoundary)
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
	current := firstBoundary
	for i := 1; i < len(boundaries); i++ {
		delta, n := binary.Uvarint(deltaBuf[offset:])
		if n <= 0 {
			return fmt.Errorf("failed to decode boundary delta at index %d", i)
		}
		offset += n
		if delta > maxStringBoundaryValue-current {
			return fmt.Errorf("boundary delta exceeds max supported value at index %d", i)
		}
		current += delta
		boundaries[i] = int(current)
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
	if len(params) != 1 {
		return fmt.Errorf("invalid token_boundaries params: %v", params)
	}

	switch params[0] {
	case stageTokenBoundariesParamWidth:
		tokenBoundaries, err := decodeTokenBoundariesStageRaw(payload)
		if err != nil {
			return err
		}
		dst.TokenBoundaries = tokenBoundaries
		return nil
	case stageTokenBoundariesParamDelta:
		tokenBoundaries, err := decodeTokenBoundariesStageDelta(payload)
		if err != nil {
			return err
		}
		dst.TokenBoundaries = tokenBoundaries
		return nil
	default:
		return fmt.Errorf("invalid token_boundaries params: %v", params)
	}
}

func decodeTokenBoundariesStageRaw(payload []byte) ([]uint32, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("token_boundaries payload too short: %d", len(payload))
	}

	r := bytes.NewReader(payload)
	var tokenLen uint32
	if err := binary.Read(r, binary.LittleEndian, &tokenLen); err != nil {
		return nil, err
	}
	if tokenLen > uint32(maxTokenBoundsRead) {
		return nil, fmt.Errorf("token boundary count too large: %d", tokenLen)
	}
	expectedBytes := int(tokenLen) * 4
	if r.Len() != expectedBytes {
		return nil, fmt.Errorf("token_boundaries length mismatch: payload=%d expected=%d", r.Len(), expectedBytes)
	}

	tokenBoundaries := make([]uint32, tokenLen)
	if err := binary.Read(r, binary.LittleEndian, tokenBoundaries); err != nil {
		return nil, err
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("token_boundaries trailing bytes: %d", r.Len())
	}
	return tokenBoundaries, nil
}

func decodeTokenBoundariesStageDelta(payload []byte) ([]uint32, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("token_boundaries payload too short: %d", len(payload))
	}

	r := bytes.NewReader(payload)
	var tokenLen uint32
	if err := binary.Read(r, binary.LittleEndian, &tokenLen); err != nil {
		return nil, err
	}
	if tokenLen > uint32(maxTokenBoundsRead) {
		return nil, fmt.Errorf("token boundary count too large: %d", tokenLen)
	}
	if tokenLen == 0 {
		if r.Len() != 0 {
			return nil, fmt.Errorf("token_boundaries trailing bytes: %d", r.Len())
		}
		return nil, nil
	}
	if r.Len() < 8 {
		return nil, fmt.Errorf("token_boundaries missing first boundary or delta length")
	}

	var firstBoundary uint32
	if err := binary.Read(r, binary.LittleEndian, &firstBoundary); err != nil {
		return nil, err
	}

	var deltaBufLen uint32
	if err := binary.Read(r, binary.LittleEndian, &deltaBufLen); err != nil {
		return nil, err
	}
	if deltaBufLen > uint32(r.Len()) {
		return nil, fmt.Errorf("delta buffer length %d exceeds remaining payload %d", deltaBufLen, r.Len())
	}
	if tokenLen-1 > deltaBufLen {
		return nil, fmt.Errorf("delta buffer too short for %d boundaries: %d", tokenLen, deltaBufLen)
	}

	deltaBuf := make([]byte, deltaBufLen)
	if _, err := io.ReadFull(r, deltaBuf); err != nil {
		return nil, err
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("token_boundaries trailing bytes: %d", r.Len())
	}

	tokenBoundaries := make([]uint32, tokenLen)
	tokenBoundaries[0] = firstBoundary
	deltaOffset := 0
	for i := 1; i < len(tokenBoundaries); i++ {
		delta, n := binary.Uvarint(deltaBuf[deltaOffset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid token boundary delta at index %d", i)
		}
		next := uint64(tokenBoundaries[i-1]) + delta
		if next > uint64(^uint32(0)) {
			return nil, fmt.Errorf("token boundary overflow at index %d", i)
		}
		tokenBoundaries[i] = uint32(next)
		deltaOffset += n
	}
	if deltaOffset != len(deltaBuf) {
		return nil, fmt.Errorf("token_boundaries delta trailing bytes: %d", len(deltaBuf)-deltaOffset)
	}
	return tokenBoundaries, nil
}

func validateArchiveStructure(a *Archive) error {
	if a.compressedTokenBitWidth != 0 &&
		a.compressedTokenBitWidth != tokenBitWidth12 &&
		a.compressedTokenBitWidth != tokenBitWidth16 {
		return fmt.Errorf("invalid token bit-width: %d", a.compressedTokenBitWidth)
	}

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
	if a.tokenBitWidth() == tokenBitWidth12 {
		for i, tokenID := range a.CompressedData {
			if tokenID > maxTokenID12Bit {
				return fmt.Errorf("compressed token out of 12-bit range at index %d: %d", i, tokenID)
			}
		}
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

	compressedPayload, compressedParam, err := encodeCompressedDataStage(a)
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
	tokenBoundariesPayload, tokenBoundariesParam, err := encodeTokenBoundariesStage(a)
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
			params:  []byte{compressedParam},
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
			params:  []byte{tokenBoundariesParam},
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

	tmp := Archive{compressedTokenBitWidth: tokenBitWidth16}
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
