package onpair

// Open-addressed tables that replace the Go maps in Matcher. The key spaces are
// bounded integers and the lookup is the dominant cost of find(), so we avoid
// the generic-map bookkeeping (hash seeding, overflow buckets, range checks).

// longBucketTable maps an 8-byte token prefix to its candidate longBucket.
// Empty slots are identified by a nil bucket pointer, so writers must never
// insert a nil value.
type longBucketTable struct {
	entries []longBucketEntry
	mask    uint64
	count   int
}

type longBucketEntry struct {
	key    uint64
	bucket *longBucket
}

func (t *longBucketTable) reserve() {
	const initialSize = 64
	t.entries = make([]longBucketEntry, initialSize)
	t.mask = initialSize - 1
}

func (t *longBucketTable) get(key uint64) *longBucket {
	if t.entries == nil {
		return nil
	}
	h := hashU64(key) & t.mask
	for {
		e := &t.entries[h]
		if e.bucket == nil {
			return nil
		}
		if e.key == key {
			return e.bucket
		}
		h = (h + 1) & t.mask
	}
}

func (t *longBucketTable) set(key uint64, bucket *longBucket) {
	if t.entries == nil {
		t.reserve()
	}
	if (t.count+1)*2 >= len(t.entries) {
		t.grow()
	}
	h := hashU64(key) & t.mask
	for {
		e := &t.entries[h]
		if e.bucket == nil {
			e.key = key
			e.bucket = bucket
			t.count++
			return
		}
		if e.key == key {
			e.bucket = bucket
			return
		}
		h = (h + 1) & t.mask
	}
}

func (t *longBucketTable) grow() {
	old := t.entries
	size := uint64(len(old)) * 2
	t.entries = make([]longBucketEntry, size)
	t.mask = size - 1
	t.count = 0
	for i := range old {
		if old[i].bucket == nil {
			continue
		}
		t.reinsert(old[i].key, old[i].bucket)
	}
}

func (t *longBucketTable) reinsert(key uint64, bucket *longBucket) {
	h := hashU64(key) & t.mask
	for {
		e := &t.entries[h]
		if e.bucket == nil {
			e.key = key
			e.bucket = bucket
			t.count++
			return
		}
		h = (h + 1) & t.mask
	}
}

// u64U16Table maps a uint64 key to a uint16 value. Value 0 marks an empty slot,
// which is safe here because the only caller (shortMatchLookup) never stores
// token ID 0: single-byte tokens (IDs 0–255) bypass the short-lookup path in
// matcher.insert, and merged tokens start at ID 256.
type u64U16Table struct {
	entries []u64U16Entry
	mask    uint64
	count   int
}

type u64U16Entry struct {
	key   uint64
	value uint16
	_     [6]byte // pad to 16 bytes for four entries per 64-byte cache line
}

func (t *u64U16Table) reserve() {
	const initialSize = 16
	t.entries = make([]u64U16Entry, initialSize)
	t.mask = initialSize - 1
}

func (t *u64U16Table) get(key uint64) (uint16, bool) {
	if t.entries == nil {
		return 0, false
	}
	h := hashU64(key) & t.mask
	for {
		e := &t.entries[h]
		if e.value == 0 {
			return 0, false
		}
		if e.key == key {
			return e.value, true
		}
		h = (h + 1) & t.mask
	}
}

func (t *u64U16Table) set(key uint64, value uint16) {
	if t.entries == nil {
		t.reserve()
	}
	if (t.count+1)*2 >= len(t.entries) {
		t.grow()
	}
	h := hashU64(key) & t.mask
	for {
		e := &t.entries[h]
		if e.value == 0 {
			e.key = key
			e.value = value
			t.count++
			return
		}
		if e.key == key {
			e.value = value
			return
		}
		h = (h + 1) & t.mask
	}
}

func (t *u64U16Table) grow() {
	old := t.entries
	size := uint64(len(old)) * 2
	t.entries = make([]u64U16Entry, size)
	t.mask = size - 1
	t.count = 0
	for i := range old {
		if old[i].value == 0 {
			continue
		}
		t.reinsert(old[i].key, old[i].value)
	}
}

func (t *u64U16Table) reinsert(key uint64, value uint16) {
	h := hashU64(key) & t.mask
	for {
		e := &t.entries[h]
		if e.value == 0 {
			e.key = key
			e.value = value
			t.count++
			return
		}
		h = (h + 1) & t.mask
	}
}

// hashU64 is Fibonacci multiplicative hashing with the high bits folded back
// down, since callers mask the low bits. Two ops on the probe's critical path;
// the tables stay at most half full, so the weaker avalanche is enough.
func hashU64(k uint64) uint64 {
	k *= 0x9e3779b97f4a7c15
	return k ^ (k >> 29)
}
