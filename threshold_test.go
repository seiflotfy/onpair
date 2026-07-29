package onpair

import "testing"

func TestThresholdController_StartsAtTwo(t *testing.T) {
	c := newThresholdController(65280, 1<<20)
	if c.threshold != 2 {
		t.Fatalf("initial threshold = %d, want 2", c.threshold)
	}
}

func TestThresholdController_RisesWhenCreationOutpacesBudget(t *testing.T) {
	c := newThresholdController(1000, 1<<20)
	// One token per scanned byte is far above the pace needed to fill 1000
	// slots over 1 MiB, so every checkpoint should raise the threshold.
	for i := 0; i < 3*c.checkInterval; i++ {
		c.onBytesScanned(1)
		c.onEntryCreated()
	}
	if c.threshold != 5 {
		t.Fatalf("threshold = %d after 3 checkpoints of overshoot, want 5", c.threshold)
	}
}

func TestThresholdController_FallsBackToFloorWhenUnderfilled(t *testing.T) {
	c := newThresholdController(1000, 1<<20)
	for i := 0; i < 3*c.checkInterval; i++ {
		c.onBytesScanned(1)
		c.onEntryCreated()
	}
	// Now creation nearly stalls relative to the remaining budget: the
	// threshold should step back down, but never below the floor of 2.
	for i := 0; i < 20*c.checkInterval; i++ {
		c.onBytesScanned(1 << 14)
		c.onEntryCreated()
	}
	if c.threshold != 2 {
		t.Fatalf("threshold = %d after sustained undershoot, want floor 2", c.threshold)
	}
}

func TestThresholdController_ClampsAt255(t *testing.T) {
	c := newThresholdController(64, 1<<40)
	for i := 0; i < 300*c.checkInterval; i++ {
		c.onBytesScanned(1)
		c.onEntryCreated()
	}
	if c.threshold != 255 {
		t.Fatalf("threshold = %d, want ceiling 255", c.threshold)
	}
}

func TestTrainDynamicThresholdProducesMergedTokens(t *testing.T) {
	rows := make([]string, 200)
	for i := range rows {
		rows[i] = "user_session_0001"
	}
	m, err := TrainModel(rows)
	if err != nil {
		t.Fatalf("TrainModel: %v", err)
	}
	// tokenBoundaries has one entry per token plus the leading zero; more than
	// 257 entries means merges happened beyond the 256 single-byte tokens.
	if len(m.tokenBoundaries) <= singleByteTokens+1 {
		t.Fatalf("no merged tokens created: %d boundaries", len(m.tokenBoundaries))
	}
}
