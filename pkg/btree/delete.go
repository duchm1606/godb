package btree

import (
	"bytes"
	"duchm1606/godb/internal/constants"
)

// recursive deletion a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// find index of key to pull key from node
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF: // if leaf
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // not found
		}
		// delete the key in the leaf
		new := BNode{data: make([]byte, constants.BtreePageSize)} // allocate empty node
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE: // if internal
		return nodeDelete(tree, node, idx, key)
	default:
		panic("treeDelete: bad node")
	}
}
