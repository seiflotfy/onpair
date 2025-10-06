package compressor

// SimplePRNG is a simple Linear Congruential Generator for cross-platform deterministic shuffling.
// Uses the same constants as Rust's StdRng for compatibility.
type SimplePRNG struct {
	state uint64
}

// NewSimplePRNG creates a new PRNG with the given seed
func NewSimplePRNG(seed uint64) *SimplePRNG {
	return &SimplePRNG{state: seed}
}

// Next generates the next random number using LCG
// Uses multiplier and increment from Numerical Recipes
func (p *SimplePRNG) Next() uint64 {
	p.state = p.state*6364136223846793005 + 1442695040888963407
	return p.state
}

// Uint64N returns a random number in [0, n)
func (p *SimplePRNG) Uint64N(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	return p.Next() % n
}

// Shuffle performs an in-place Fisher-Yates shuffle
func (p *SimplePRNG) Shuffle(slice []int) {
	for i := len(slice) - 1; i > 0; i-- {
		j := int(p.Uint64N(uint64(i + 1)))
		slice[i], slice[j] = slice[j], slice[i]
	}
}
