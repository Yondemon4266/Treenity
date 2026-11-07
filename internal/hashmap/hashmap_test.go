package hashmap

import (
	"fmt"
	"testing"
)

func TestSetGetRemoveGrow(t *testing.T) {
	m := New[int]()
	for i := 0; i < 1000; i++ {
		m.Set(fmt.Sprintf("k%d", i), i)
	}
	if m.Len() != 1000 {
		t.Fatalf("len=%d", m.Len())
	}
	for i := 0; i < 1000; i++ {
		if v, ok := m.Get(fmt.Sprintf("k%d", i)); !ok || v != i {
			t.Fatalf("get k%d = %d,%v", i, v, ok)
		}
	}
	m.Set("k5", 99)
	if v, _ := m.Get("k5"); v != 99 || m.Len() != 1000 {
		t.Fatal("overwrite broken")
	}
	for i := 0; i < 1000; i += 2 {
		if !m.Remove(fmt.Sprintf("k%d", i)) {
			t.Fatalf("remove k%d", i)
		}
	}
	if m.Remove("k0") || m.Len() != 500 {
		t.Fatal("remove twice / len")
	}
	n := 0
	m.Each(func(string, int) { n++ })
	if n != 500 {
		t.Fatalf("each=%d", n)
	}
	if _, ok := m.Get("nope"); ok {
		t.Fatal("ghost key")
	}
}

// djb2: h = h*33 + byte, starting at 5381.
func TestHashKnownValues(t *testing.T) {
	cases := []struct {
		key  string
		want uint32
	}{
		{"", 5381},
		{"a", 177670},
		{"alice", 253185475},
		{"dave", 2090176933},
	}
	for _, c := range cases {
		if got := hash(c.key); got != c.want {
			t.Errorf("hash(%q) = %d, want %d", c.key, got, c.want)
		}
	}
	// A long key overflows uint32 and wraps around without panicking.
	_ = hash(string(make([]byte, 10000)))
}

func TestEmptyMap(t *testing.T) {
	m := New[*int]()
	if m.Len() != 0 || len(m.buckets) != startBuckets {
		t.Fatalf("new map: len=%d buckets=%d", m.Len(), len(m.buckets))
	}
	if v, ok := m.Get("x"); ok || v != nil {
		t.Errorf("Get on empty map = %v, %v; want nil, false", v, ok)
	}
	if m.Remove("x") {
		t.Error("Remove on empty map returned true")
	}
}

// Keys that fall in the same bucket are chained and stay independent.
func TestCollisionsAreChained(t *testing.T) {
	m := New[string]()
	keys := []string{"k0", "k8", "k17", "k26"} // same bucket out of 8
	for _, k := range keys {
		if m.slot(k) != m.slot(keys[0]) {
			t.Fatalf("%s is not in the same bucket as %s", k, keys[0])
		}
		m.Set(k, "v"+k)
	}
	if n := len(m.buckets[m.slot(keys[0])]); n != len(keys) {
		t.Fatalf("bucket holds %d pairs, want %d", n, len(keys))
	}
	for _, k := range keys {
		if v, ok := m.Get(k); !ok || v != "v"+k {
			t.Errorf("Get(%s) = %q, %v", k, v, ok)
		}
	}

	// Removing one key in the middle of the chain keeps the others.
	if !m.Remove("k8") {
		t.Fatal("Remove(k8) returned false")
	}
	if _, ok := m.Get("k8"); ok {
		t.Error("k8 still present")
	}
	for _, k := range []string{"k0", "k17", "k26"} {
		if v, ok := m.Get(k); !ok || v != "v"+k {
			t.Errorf("after remove: Get(%s) = %q, %v", k, v, ok)
		}
	}

	// Overwriting a chained key does not add a pair.
	m.Set("k17", "new")
	if v, _ := m.Get("k17"); v != "new" || m.Len() != 3 {
		t.Errorf("overwrite in chain: v=%q len=%d", v, m.Len())
	}
}

// The map doubles when the average bucket holds more than maxPerBucket pairs.
func TestGrowThreshold(t *testing.T) {
	m := New[int]()
	limit := maxPerBucket * startBuckets // 16
	for i := 0; i < limit; i++ {
		m.Set(fmt.Sprintf("k%d", i), i)
	}
	if len(m.buckets) != startBuckets {
		t.Fatalf("grew too early: %d buckets with %d pairs", len(m.buckets), limit)
	}
	m.Set("one-more", -1)
	if len(m.buckets) != 2*startBuckets {
		t.Fatalf("after %d pairs: %d buckets, want %d", limit+1, len(m.buckets), 2*startBuckets)
	}
	// Every pair is filed again in the bucket of the new size.
	for b, bucket := range m.buckets {
		for _, p := range bucket {
			if m.slot(p.key) != b {
				t.Errorf("%s is in bucket %d, want %d", p.key, b, m.slot(p.key))
			}
		}
	}
	for i := 0; i < limit; i++ {
		if v, ok := m.Get(fmt.Sprintf("k%d", i)); !ok || v != i {
			t.Errorf("after grow: Get(k%d) = %d, %v", i, v, ok)
		}
	}
}

// With pointer values, Get returns the stored pointer: changes through it are seen.
func TestPointerValues(t *testing.T) {
	type record struct{ offset int32 }
	m := New[*record]()
	m.Set("alice", &record{})
	r, _ := m.Get("alice")
	r.offset = 4
	if again, _ := m.Get("alice"); again.offset != 4 {
		t.Errorf("offset = %d, want 4", again.offset)
	}
}
