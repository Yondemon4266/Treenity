// Package hashmap is a string-keyed hash map written by hand.
// Collisions are handled by keeping several pairs in the same bucket.
package hashmap

// pair is one key/value entry stored in a bucket.
type pair[V any] struct {
	key   string
	value V
}

// Map associates string keys to values of type V.
type Map[V any] struct {
	buckets [][]pair[V]
	count   int
}

const (
	startBuckets = 8
	maxPerBucket = 2 // average pairs per bucket tolerated before growing
)

// New returns an empty map.
func New[V any]() *Map[V] {
	return &Map[V]{buckets: make([][]pair[V], startBuckets)}
}

// hash is the djb2 hash of key.
func hash(key string) uint32 {
	h := uint32(5381)
	for i := 0; i < len(key); i++ {
		h = h*33 + uint32(key[i])
	}
	return h
}

// slot gives the bucket number of key.
func (m *Map[V]) slot(key string) int {
	return int(hash(key) % uint32(len(m.buckets)))
}

// Get returns the value stored under key and whether it exists.
func (m *Map[V]) Get(key string) (V, bool) {
	for _, p := range m.buckets[m.slot(key)] {
		if p.key == key {
			return p.value, true
		}
	}
	var zero V
	return zero, false
}

// Set stores value under key, replacing any previous value.
func (m *Map[V]) Set(key string, value V) {
	b := m.slot(key)
	for i := range m.buckets[b] {
		if m.buckets[b][i].key == key {
			m.buckets[b][i].value = value
			return
		}
	}
	m.buckets[b] = append(m.buckets[b], pair[V]{key, value})
	m.count++
	if m.count > maxPerBucket*len(m.buckets) {
		m.grow()
	}
}

// Remove deletes key and reports whether it was present.
func (m *Map[V]) Remove(key string) bool {
	b := m.slot(key)
	for i, p := range m.buckets[b] {
		if p.key == key {
			m.buckets[b] = append(m.buckets[b][:i], m.buckets[b][i+1:]...)
			m.count--
			return true
		}
	}
	return false
}

// Len returns the number of stored keys.
func (m *Map[V]) Len() int { return m.count }

// Each calls f for every pair, in no particular order.
func (m *Map[V]) Each(f func(key string, value V)) {
	for _, bucket := range m.buckets {
		for _, p := range bucket {
			f(p.key, p.value)
		}
	}
}

// grow doubles the number of buckets and files every pair again.
func (m *Map[V]) grow() {
	old := m.buckets
	m.buckets = make([][]pair[V], len(old)*2)
	for _, bucket := range old {
		for _, p := range bucket {
			b := m.slot(p.key)
			m.buckets[b] = append(m.buckets[b], p)
		}
	}
}
