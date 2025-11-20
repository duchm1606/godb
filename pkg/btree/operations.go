package btree

import (
	"duchm1606/godb/internal/constants"
	"duchm1606/godb/internal/util"
)

// Inserting a key into a node can result in either 1, 2 or 3 nodes. The parent node must update itself accordingly. The code for updating an internal node is similar to that for updating a leaf node.
func nodeReplaceChildNodes(tree *BTree, new BNode, old BNode, idx uint16, children ...BNode) {
	inc := uint16(len(children))
	// In this B-Tree layout, internal nodes store one KV per child (key is the child’s low key, value is nil), with the first key being a copy from the parent
	// So splitting a child into inc children increases the parent’s entry count by inc - 1.
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range children {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// determine if updated kid should be merged and if so the direction
// condition of merging
// - node is smaller than 1/4 of a page
// - node has a sibling and the merged results does not exceed one page
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > constants.BtreePageSize/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= constants.BtreePageSize {
			return -1, sibling
		}
	}

	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= constants.BtreePageSize {
			return +1, sibling
		}
	}

	return 0, BNode{}
}

// merging nodes into left or right siblings during deletion of internal nodes
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recursive into delete child
	childPtr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(childPtr), key)
	if len(updated.data) == 0 {
		return BNode{}
	}
	tree.del(childPtr)

	new := BNode{data: make([]byte, constants.BtreePageSize)}

	// get merge direction - either left or right sibling
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // if left
		merged := BNode{data: make([]byte, constants.BtreePageSize)} // prepare new node to merge old into
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Child(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // if right
		merged := BNode{data: make([]byte, constants.BtreePageSize)} // prepare new node to merge old into
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Child(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0:
		if updated.nkeys() == 0 { // parent only has one child, child is empty after deletion
			// no siblings to merge with therefore discard empty kid and return empty parent
			util.Assert(node.nkeys() != 1 || idx != 0, "nodeDelete: bad deletion")
			new.setHeader(BNODE_NODE, 0)
		} else {
			nodeReplaceChildNodes(tree, new, node, idx, updated)
		}
	}
	return new
}
