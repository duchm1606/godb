package main

import (
	"fmt"
	"godb/pkg/btree"
)

func main() {
	// Example usage of the refactored database
	fmt.Println("GoDb - Database implementation from scratch")

	// Create a simple in-memory B-tree for demonstration
	tree := &btree.BTree{}

	// Set up in-memory callbacks (for demo purposes)
	pages := make(map[uint64]btree.BNode)
	var nextPtr uint64 = 1

	tree.SetCallbacks(
		func(ptr uint64) btree.BNode {
			if node, ok := pages[ptr]; ok {
				return node
			}
			panic("page not found")
		},
		func(node btree.BNode) uint64 {
			ptr := nextPtr
			nextPtr++
			pages[ptr] = node
			return ptr
		},
		func(ptr uint64) {
			delete(pages, ptr)
		},
	)

	// Test basic operations
	tree.Insert([]byte("hello"), []byte("world"))
	tree.Insert([]byte("foo"), []byte("bar"))

	if val, found := tree.Get([]byte("hello")); found {
		fmt.Printf("Found: hello -> %s\n", string(val))
	}

	if val, found := tree.Get([]byte("foo")); found {
		fmt.Printf("Found: foo -> %s\n", string(val))
	}

	fmt.Println("Database operations completed successfully!")
}
