package onpair

// Model is a reusable trained dictionary.
type Model struct {
	config          Config
	matcher         *matcher
	codeRemap       []uint16 // creation-order token id → sorted dictionary id
	dictionary      []byte
	tokenBoundaries []uint32
}

// NewModel creates an empty model with the provided options.
func NewModel(opts ...Option) *Model {
	var cfg Config
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Model{config: cfg}
}

// TrainModel trains a reusable model from sample strings.
func TrainModel(strings []string, opts ...Option) (*Model, error) {
	m := NewModel(opts...)
	if err := m.Train(strings); err != nil {
		return nil, err
	}
	return m, nil
}

// Train builds the dictionary and matcher for subsequent Encode calls.
func (m *Model) Train(strings []string) error {
	enc := &Encoder{config: m.config}
	data, endPositions := flattenStrings(strings)
	matcher, dict, tokenBoundaries, remap := enc.train(data, endPositions)
	m.matcher = matcher
	m.codeRemap = remap
	m.dictionary = dict
	m.tokenBoundaries = tokenBoundaries
	return nil
}

// Encode compresses strings using a previously trained model.
func (m *Model) Encode(strings []string) (*Archive, error) {
	if m.matcher == nil {
		return nil, ErrUntrainedModel
	}
	enc := &Encoder{config: m.config}
	data, endPositions := flattenStrings(strings)
	compressedData, stringBoundaries := enc.compress(data, endPositions, m.matcher, m.codeRemap)

	tokenBoundaries := append([]uint32(nil), m.tokenBoundaries...)
	return &Archive{
		CompressedData:          compressedData,
		StringBoundaries:        stringBoundaries,
		Dictionary:              padDictionary(m.dictionary),
		TokenBoundaries:         tokenBoundaries,
		compressedTokenBitWidth: resolveTokenBitWidth(enc.config),
	}, nil
}

// Trained reports whether the model is ready for Encode.
func (m *Model) Trained() bool {
	return m.matcher != nil
}

// Encode compresses a collection of strings into an Archive.
func (e *Encoder) Encode(strings []string) (*Archive, error) {
	data, endPositions := flattenStrings(strings)

	// Train the dictionary
	matcher, dict, tokenBoundaries, remap := e.train(data, endPositions)

	// Compress the data
	compressedData, stringBoundaries := e.compress(data, endPositions, matcher, remap)

	return &Archive{
		CompressedData:          compressedData,
		StringBoundaries:        stringBoundaries,
		Dictionary:              padDictionary(dict),
		TokenBoundaries:         tokenBoundaries,
		compressedTokenBitWidth: resolveTokenBitWidth(e.config),
	}, nil
}
