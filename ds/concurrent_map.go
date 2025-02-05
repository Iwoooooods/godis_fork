package ds

import (
	"log"
	"math"
	"sync"
	"sync/atomic"
)

const prime32 = uint32(16777619)

type ShardedMap struct {
	table  []*Shard
	size   int32
	shards int
}

// compute the hashcode of a key with FNV
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

type Shard struct {
	m  map[string]any
	mu sync.RWMutex
}

func computeShards(param int) int {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	} else {
		return int(n + 1)
	}
}

func NewShardedMap(shards int) *ShardedMap {
	numOfS := computeShards(shards)
	log.Printf("making a ShardedMap with %d shards...", numOfS)
	table := make([]*Shard, numOfS)
	for i := 0; i < numOfS; i++ {
		table[i] = &Shard{
			m: make(map[string]any),
		}
	}
	return &ShardedMap{
		table:  table,
		size:   0,
		shards: shards,
	}
}

// determines which shard a given hash code should map to
func (m *ShardedMap) spread(hashCode uint32) uint32 {
	if m == nil {
		panic("map is nil")
	}
	tableSize := uint32(len(m.table))
	return (tableSize - 1) & uint32(hashCode)
}

// locates the shard from the Shard slice
func (m *ShardedMap) locate(index uint32) *Shard {
	if m == nil {
		panic("map is nil")
	}
	return m.table[index]
}

func (m *ShardedMap) Get(key string) (any, bool) {
	if m == nil {
		panic("map is nil")
	}

	hashCode := fnv32(key)
	index := m.spread(hashCode)
	shard := m.locate(index)

	shard.mu.RLock()
	defer shard.mu.RUnlock()
	val, exists := shard.m[key]
	return val, exists
}

func (m *ShardedMap) Len() int {
	if m == nil {
		panic("map is nil")
	}
	return int(atomic.LoadInt32(&m.size))
}

// Put inserts or updates a key/value pair, returns false if key exists
func (m *ShardedMap) Put(key string, val any) bool {
	if m == nil {
		panic("map is nil")
	}
	hashCode := fnv32(key)
	index := m.spread(hashCode)
	shard := m.locate(index)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return false
	}
	shard.m[key] = val
	m.increCount()
	return true
}

func (m *ShardedMap) Del(key string) bool {
	if m == nil {
		panic("map is nil")
	}

	// locate the key in shards
	shard := m.locate(m.spread(fnv32(key)))

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// delete the k/v pair if exists
	// decre the counter
	if _, ok := shard.m[key]; ok {
		m.decreCount()
		delete(shard.m, key)
		return true
	}

	return false
}

func (m *ShardedMap) Clear() *ShardedMap {
	if m == nil {
		panic("map is nil")
	}

	for _, shard := range m.table {
		shard.mu.Lock()
		shard.m = make(map[string]any)
		shard.mu.Unlock()
	}
	atomic.StoreInt32(&m.size, 0)

	return m
}

// ForEach traveses the map
// if func parameter returns false
// the ForEach will break
func (m *ShardedMap) ForEach(consumer func(key string, val any) bool) {
	if m == nil {
		panic("map is nil")
	}

	for _, s := range m.table {
		s.mu.Lock()
		f := func() bool {
			defer s.mu.Unlock()
			for k, v := range s.m {
				continues := consumer(k, v)
				if !continues {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}

func (m *ShardedMap) Keys() []string {
	results := make([]string, 0, m.Len())
	m.ForEach(func(key string, val any) bool {
		results = append(results, key)
		return true
	})
	return results
}

func (m *ShardedMap) increCount() {
	atomic.AddInt32(&m.size, 1)
}

func (m *ShardedMap) decreCount() {
	atomic.AddInt32(&m.size, -1)
}
