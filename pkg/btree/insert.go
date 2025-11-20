package btree

import (
	"bytes"
	"duchm1606/godb/internal/constants"
	"duchm1606/godb/internal/util"
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
	new := BNode{data: make([]byte, 2*constants.BtreePageSize)}

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node types
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a child node
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("treeInsert: unknown node type")
	}
	return new
}

// insertion interface
func (tree *BTree) Insert(key []byte, val []byte) {
	util.Assert(len(key) == 0, "Empty key!")
	util.Assert(len(key) > constants.BtreeMaxKeySize, "Key length greater than maximum")
	util.Assert(len(key) > constants.BtreeMaxValSize, "Val length greater than maximum")

	if tree.root == 0 { // inserting the first key
		// create first leaf node as root
		root := BNode{data: make([]byte, constants.BtreePageSize)}
		root.setHeader(BNODE_LEAF, 2) // add dummy key = 2 so the tree cover key space
		// lookup can always find a node containing a key
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := tree.get(tree.root)
	tree.del(tree.root)
	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)
	if nsplit > 1 {
		// if the root was split, add a new level and create a new root
		root := BNode{data: make([]byte, constants.BtreePageSize)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil) // add splitted nodes of old root to new root
		}
		tree.root = tree.new(root) // reassign tree root to newly formed root
	} else {
		tree.root = tree.new(splitted[0])
	}
}
