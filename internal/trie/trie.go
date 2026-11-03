// Package trie finds, for a message key, every name registered under a prefix of that key.
package trie

type node struct {
	children map[byte]*node
	names    []string // names registered under the prefix that ends at this node
}

func newNode() *node {
	return &node{children: make(map[byte]*node)}
}

// Trie maps prefixes to names. Looking up a key costs one step per byte of
// the key, however many prefixes are registered.
type Trie struct {
	root *node
}

func New() *Trie {
	return &Trie{root: newNode()}
}

// Add registers name under prefix. The empty prefix matches every key.
func (t *Trie) Add(prefix []byte, name string) {
	n := t.root
	for _, b := range prefix {
		next, ok := n.children[b]
		if !ok {
			next = newNode()
			n.children[b] = next
		}
		n = next
	}
	n.names = append(n.names, name)
}

// Remove forgets name under prefix. An unknown pair is ignored.
func (t *Trie) Remove(prefix []byte, name string) {
	n := t.root
	for _, b := range prefix {
		n = n.children[b]
		if n == nil {
			return
		}
	}
	for i, registered := range n.names {
		if registered == name {
			n.names = append(n.names[:i], n.names[i+1:]...)
			return
		}
	}
}

// Match returns the names registered under every prefix of key, shortest prefix first.
func (t *Trie) Match(key []byte) []string {
	n := t.root
	found := append([]string(nil), n.names...)
	for _, b := range key {
		n = n.children[b]
		if n == nil {
			break
		}
		found = append(found, n.names...)
	}
	return found
}
