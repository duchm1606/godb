package btree

import (
	"bytes"
	"godb/internal/constants"
)

// part of the treeInsert(): kv insertion to an internal node
func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	// recursive insertion to the child node
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := nodeSplit3(knode)
	// update the child link
	nodeReplaceChildNodes(tree, new, node, idx, splited[:nsplit]...)
}

// insert a KV into a node, the result might be split into 2 nodes
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node
	// it's allowed to be bigger than 1 page and will be split if so
	new := NewBNode(make([]byte, 2*constants.PageSize))

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node types
	switch node.btype() {
	case NodeTypeLeaf:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position
			leafInsert(new, node, idx+1, key, val)
		}
	case NodeTypeInternal:
		// internal node, insert it to a child node
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("treeInsert: unknown node type")
	}
	return new
}
