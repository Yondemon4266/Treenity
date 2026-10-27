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
