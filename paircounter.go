package onpair

// pairCounter is an open-addressed uint32 → uint16 counter for the BPE-style
// pair frequency table in buildTokens. Linear probing, tombstone-based delete.
// Faster than map[uint32]uint16 for this workload because the counter is hit
// once per adjacent token pair and Go's map has per-op bookkeeping we don't
// need for a bounded-range integer key.
type pairCounter struct {
	entries   []pairEntry
	mask      uint32
	count     int // live (non-tombstone) entries
	tombstone int
}

type pairEntry struct {
	key   uint32
	count uint16
	state uint8 // 0 = empty, 1 = occupied, 2 = tombstone
}

func newPairCounter(hint int) *pairCounter {
	size := uint32(64)
	target := uint32(hint * 4 / 3)
	for size < target {
		size <<= 1
	}
	return &pairCounter{
		entries: make([]pairEntry, size),
		mask:    size - 1,
	}
}

// splitmix32 finalizer — fast high-quality mix for uint32 keys.
func hashPairKey(k uint32) uint32 {
	k ^= k >> 16
	k *= 0x85ebca6b
	k ^= k >> 13
	k *= 0xc2b2ae35
	k ^= k >> 16
	return k
}

// incr increments the count for key, inserting if absent. Returns the new count.
func (t *pairCounter) incr(key uint32) uint16 {
	if (t.count+t.tombstone+1)*4 >= len(t.entries)*3 {
		t.grow()
	}
	h := hashPairKey(key) & t.mask
	firstTomb := -1
	for {
		e := &t.entries[h]
		switch e.state {
		case 0:
			if firstTomb >= 0 {
				slot := &t.entries[firstTomb]
				slot.key = key
				slot.count = 1
				slot.state = 1
				t.tombstone--
			} else {
				e.key = key
				e.count = 1
				e.state = 1
			}
			t.count++
			return 1
		case 1:
			if e.key == key {
				e.count++
				return e.count
			}
		case 2:
			if firstTomb < 0 {
				firstTomb = int(h)
			}
		}
		h = (h + 1) & t.mask
	}
}

// remove marks the slot for key as tombstone, if present.
func (t *pairCounter) remove(key uint32) {
	h := hashPairKey(key) & t.mask
	for {
		e := &t.entries[h]
		if e.state == 0 {
			return
		}
		if e.state == 1 && e.key == key {
			e.state = 2
			t.count--
			t.tombstone++
			return
		}
		h = (h + 1) & t.mask
	}
}

func (t *pairCounter) grow() {
	old := t.entries
	newSize := uint32(len(old)) * 2
	// If tombstone pressure is the real cause, same-size rehash suffices.
	if t.count*2 < len(old) {
		newSize = uint32(len(old))
	}
	t.entries = make([]pairEntry, newSize)
	t.mask = newSize - 1
	t.count = 0
	t.tombstone = 0
	for i := range old {
		e := &old[i]
		if e.state != 1 {
			continue
		}
		t.insertForGrow(e.key, e.count)
	}
}

func (t *pairCounter) insertForGrow(key uint32, count uint16) {
	h := hashPairKey(key) & t.mask
	for {
		e := &t.entries[h]
		if e.state == 0 {
			e.key = key
			e.count = count
			e.state = 1
			t.count++
			return
		}
		h = (h + 1) & t.mask
	}
}
