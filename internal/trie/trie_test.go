package trie

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// sorted returns a sorted copy, so that tests do not depend on the order of Match.
func sorted(names []string) []string {
	out := append([]string(nil), names...)
	sort.Strings(out)
	return out
}

// bruteForce is the reference: every name whose prefix starts the key.
func bruteForce(subs map[string][]byte, key []byte) []string {
	var out []string
	for name, prefix := range subs {
		if bytes.HasPrefix(key, prefix) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// The four rules oif the subject (VI.9).
func TestMatchRules(t *testing.T) {
	tr := New()
	tr.Add([]byte("user"), "alice")

	cases := []struct {
		name string
		key  string
		want []string
	}{
		{"exact match", "user", []string{"alice"}},
		{"prefix match", "user.login", []string{"alice"}},
		{"no match", "admin", nil},
		{"key shorter than prefix", "use", nil},
		{"prefix in the middle of the key", "superuser", nil},
		{"same letters, other case", "User", nil},
	}
	for _, c := range cases {
		got := tr.Match([]byte(c.key))
		if len(got) != len(c.want) || (len(got) > 0 && !reflect.DeepEqual(got, c.want)) {
			t.Errorf("%s: Match(%q) = %v, want %v", c.name, c.key, got, c.want)
		}
	}
}

// An empty prefix matches every key and is stored at the root, without traversal.
func TestEmptyPrefix(t *testing.T) {
	tr := New()
	tr.Add(nil, "all")
	tr.Add([]byte{}, "all2")

	if len(tr.root.children) != 0 {
		t.Fatalf("empty prefix created %d child nodes, want 0", len(tr.root.children))
	}
	if !reflect.DeepEqual(tr.root.names, []string{"all", "all2"}) {
		t.Fatalf("root names = %v, want [all all2]", tr.root.names)
	}
	for _, key := range []string{"", "a", "user.login", "\x00\xff"} {
		if got := sorted(tr.Match([]byte(key))); !reflect.DeepEqual(got, []string{"all", "all2"}) {
			t.Errorf("Match(%q) = %v, want [all all2]", key, got)
		}
	}
}

func TestEmptyTrie(t *testing.T) {
	tr := New()
	for _, key := range []string{"", "user", "x"} {
		if got := tr.Match([]byte(key)); len(got) != 0 {
			t.Errorf("empty trie: Match(%q) = %v, want nothing", key, got)
		}
	}
	// Removing from an empty trie must not panic.
	tr.Remove([]byte("user"), "alice")
	tr.Remove(nil, "alice")
}

func TestMinimal(t *testing.T) {
	tr := New()
	tr.Add([]byte("a"), "x")
	if got := tr.Match([]byte("a")); !reflect.DeepEqual(got, []string{"x"}) {
		t.Errorf("Match(a) = %v", got)
	}
	if got := tr.Match([]byte("ab")); !reflect.DeepEqual(got, []string{"x"}) {
		t.Errorf("Match(ab) = %v", got)
	}
	if got := tr.Match([]byte("b")); len(got) != 0 {
		t.Errorf("Match(b) = %v, want nothing", got)
	}
	if got := tr.Match(nil); len(got) != 0 {
		t.Errorf("Match(empty key) = %v, want nothing", got)
	}
}

// Several consumers can share the same prefix (VI.9).
func TestSharedPrefix(t *testing.T) {
	tr := New()
	tr.Add([]byte("user"), "alice")
	tr.Add([]byte("user"), "bob")
	if got := sorted(tr.Match([]byte("user.create"))); !reflect.DeepEqual(got, []string{"alice", "bob"}) {
		t.Errorf("got %v, want [alice bob]", got)
	}
}

// Nested prefixes: every one of them matches, shortest first.
func TestNestedPrefixesShortestFirst(t *testing.T) {
	tr := New()
	tr.Add([]byte("user.create"), "c")
	tr.Add([]byte(""), "root")
	tr.Add([]byte("u"), "a")
	tr.Add([]byte("user"), "b")
	tr.Add([]byte("user.delete"), "d")

	got := tr.Match([]byte("user.create.42"))
	want := []string{"root", "a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Keys and prefixes are bytes, not text.
func TestBinaryBytes(t *testing.T) {
	tr := New()
	tr.Add([]byte{0x00, 0xff}, "bin")
	if got := tr.Match([]byte{0x00, 0xff, 0x10}); !reflect.DeepEqual(got, []string{"bin"}) {
		t.Errorf("got %v", got)
	}
	if got := tr.Match([]byte{0x00, 0xfe}); len(got) != 0 {
		t.Errorf("got %v, want nothing", got)
	}
}

func TestRemove(t *testing.T) {
	tr := New()
	tr.Add([]byte("user"), "alice")
	tr.Add([]byte("user"), "bob")
	tr.Add(nil, "all")

	tr.Remove([]byte("user"), "alice")
	if got := sorted(tr.Match([]byte("user"))); !reflect.DeepEqual(got, []string{"all", "bob"}) {
		t.Errorf("after removing alice: got %v", got)
	}

	// Unknown pairs are ignored.
	tr.Remove([]byte("user"), "nobody")
	tr.Remove([]byte("admin"), "bob")
	tr.Remove([]byte("us"), "bob")
	if got := sorted(tr.Match([]byte("user"))); !reflect.DeepEqual(got, []string{"all", "bob"}) {
		t.Errorf("after unknown removes: got %v", got)
	}

	tr.Remove(nil, "all")
	tr.Remove([]byte("user"), "bob")
	if got := tr.Match([]byte("user")); len(got) != 0 {
		t.Errorf("after removing everyone: got %v", got)
	}

	// The same name can be added again after a remove.
	tr.Add([]byte("user"), "alice")
	if got := tr.Match([]byte("user")); !reflect.DeepEqual(got, []string{"alice"}) {
		t.Errorf("re-add: got %v", got)
	}
}

// Match must return a copy: changing it must not change the trie.
func TestMatchReturnsCopy(t *testing.T) {
	tr := New()
	tr.Add(nil, "all")
	got := tr.Match([]byte("k"))
	got[0] = "changed"
	if again := tr.Match([]byte("k")); !reflect.DeepEqual(again, []string{"all"}) {
		t.Errorf("trie was modified through Match result: %v", again)
	}
}

// Integration: mixed prefix types compared with a brute-force filter.
func TestMixedPrefixesIntegration(t *testing.T) {
	subs := map[string][]byte{
		"wild":    nil,
		"u":       []byte("u"),
		"user":    []byte("user"),
		"create":  []byte("user.create"),
		"create2": []byte("user.create"),
		"admin":   []byte("admin"),
		"exact":   []byte("user.delete"),
		"bin":     {0x00},
	}
	tr := New()
	for name, prefix := range subs {
		tr.Add(prefix, name)
	}

	keys := []string{"", "u", "user", "user.create", "user.create.1", "user.update",
		"user.delete", "admin", "administrator", "root", "\x00", "\x00abc"}
	for _, key := range keys {
		got := sorted(tr.Match([]byte(key)))
		want := bruteForce(subs, []byte(key))
		if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
			t.Errorf("Match(%q) = %v, want %v", key, got, want)
		}
	}

	// Remove some consumers, the result must still agree with the reference.
	for _, name := range []string{"user", "create", "wild"} {
		tr.Remove(subs[name], name)
		delete(subs, name)
	}
	for _, key := range keys {
		got := sorted(tr.Match([]byte(key)))
		want := bruteForce(subs, []byte(key))
		if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
			t.Errorf("after removes, Match(%q) = %v, want %v", key, got, want)
		}
	}
}

// Large scale: many consumers and keys, compared with a brute-force filter.
func TestLargeScale(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	alphabet := []byte("abc.")
	randomBytes := func(max int) []byte {
		b := make([]byte, r.Intn(max+1))
		for i := range b {
			b[i] = alphabet[r.Intn(len(alphabet))]
		}
		return b
	}

	subs := make(map[string][]byte)
	tr := New()
	for i := 0; i < 2000; i++ {
		name := fmt.Sprintf("c%d", i)
		prefix := randomBytes(6)
		subs[name] = prefix
		tr.Add(prefix, name)
	}
	for i := 0; i < 1000; i++ {
		key := randomBytes(10)
		got := sorted(tr.Match(key))
		want := bruteForce(subs, key)
		if len(got) != len(want) || (len(got) > 0 && !reflect.DeepEqual(got, want)) {
			t.Fatalf("Match(%q): got %d names, want %d", key, len(got), len(want))
		}
	}
}
