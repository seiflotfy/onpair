package onpair

// thresholdController adapts the merge threshold while training scans the
// sample. Every
// checkInterval created tokens it compares the recent tokens-per-byte creation
// rate against the rate needed to fill the remaining dictionary capacity over
// the remaining sample bytes, nudging the threshold up when merges are being
// minted too eagerly and down when the dictionary would end up underfull.
// The sample is size-capped before scanning starts, so the controller paces
// against the full sample instead of stopping the scan early on a byte budget.
type thresholdController struct {
	capacity       int    // merge-token capacity (dictionary slots past the single-byte tokens)
	sampleBytes    int    // total bytes the training scan will cover
	checkInterval  int    // created tokens between rebalances
	threshold      uint16 // current merge threshold, kept in [2, 255]
	entriesCreated int
	bytesScanned   int
	entriesAtCheck int
	bytesAtCheck   int
	nextCheckpoint int
}

func newThresholdController(capacity, sampleBytes int) *thresholdController {
	checkInterval := capacity / 128
	if checkInterval < 64 {
		checkInterval = 64
	}
	return &thresholdController{
		capacity:       capacity,
		sampleBytes:    sampleBytes,
		checkInterval:  checkInterval,
		threshold:      2,
		nextCheckpoint: checkInterval,
	}
}

func (c *thresholdController) onBytesScanned(n int) {
	c.bytesScanned += n
}

func (c *thresholdController) onEntryCreated() {
	c.entriesCreated++
	if c.entriesCreated >= c.nextCheckpoint {
		c.rebalance()
	}
}

func (c *thresholdController) rebalance() {
	deltaEntries := c.entriesCreated - c.entriesAtCheck
	deltaBytes := c.bytesScanned - c.bytesAtCheck

	recentRate := 1e9
	if deltaBytes > 0 {
		recentRate = float64(deltaEntries) / float64(deltaBytes)
	}
	entriesLeft := c.capacity - c.entriesCreated
	if entriesLeft < 1 {
		entriesLeft = 1
	}
	bytesLeft := c.sampleBytes - c.bytesScanned
	if bytesLeft < 1 {
		bytesLeft = 1
	}

	ratio := recentRate * float64(bytesLeft) / float64(entriesLeft)
	if ratio > 2.0 && c.threshold < 255 {
		c.threshold++
	} else if ratio < 0.5 && c.threshold > 2 {
		c.threshold--
	}

	c.entriesAtCheck = c.entriesCreated
	c.bytesAtCheck = c.bytesScanned
	c.nextCheckpoint = c.entriesCreated + c.checkInterval
}
