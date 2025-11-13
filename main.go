//go:build linux

package btree

import (
	"fmt"
	"unsafe"
)

// tree container struct
type C struct {
	tree  BTree
	ref   map[string]string // reference map to record each b-tree update
	pages map[uint64]BNode  // hashmap to hold pages in-memory, no disk persistence yet
}

func NewC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(!ok, "Note not found in pages!")

				return node
			},
			new: func(node BNode) uint64 {
				assert(node.nbytes() > BTREE_PAGE_SIZE, "Node too large!")

				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert(pages[key].data != nil, "Data at key is not null")

				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(!ok, "Note not found in pages!")

				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) Add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) Del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func (c *C) PrintTree() {
	// fmt.Printf("Root page: %d\n", c.pages[c.tree.root])
	fmt.Println("Pages:")
	for pt, node := range c.pages {
		fmt.Println("Pointer:", pt)
		fmt.Println("BNode data:", node.data)
	}
}
