package consistenthash

import (
	"hash/crc32"
	"sort"
)

// HashFunc defines function to generate hash code
type HashFunc func(data []byte) uint32


// Map stores nodes and you can pick node from map
type NodeMap struct {
	hashFunc HashFunc
	keys []int
	hashMap map[int]string
}

// NewNodeMap creates a new NodeMap
func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc: fn,
		hashMap: make(map[int]string),
	}
	if fn == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns if there any node in NodeMap
func (m *NodeMap)IsEmpty() bool {
	return len(m.keys) == 0
}

// AddNode add the given nodes into consistent hash circle
func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		u := m.hashFunc([]byte(key))
		hash := int(u)
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = key
	}
	sort.Ints(m.keys)
}

// PickNode returns the closest node in hashcircle to the given key
func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	u := m.hashFunc([]byte(key))
	hash := int(u)

	index := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	if index == len(m.keys) {
		index = 0
	}

	return m.hashMap[m.keys[index]]
}