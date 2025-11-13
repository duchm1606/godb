package btree

import "bytes"

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

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
		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)} // allocate empty node
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE: // if internal
		return nodeDelete(tree, node, idx, key)
	default:
		panic("treeDelete: bad node")
	}
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

	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	// get merge direction - either left or right sibling
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // if left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)} // prepare new node to merge old into
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Child(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // if right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)} // prepare new node to merge old into
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Child(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0:
		if updated.nkeys() == 0 { // parent only has one child, child is empty after deletion
			// no siblings to merge with therefore discard empty kid and return empty parent
			assert(node.nkeys() != 1 || idx != 0, "nodeDelete: bad deletion")
			new.setHeader(BNODE_NODE, 0)
		} else {
			nodeReplaceChildNodes(tree, new, node, idx, updated)
		}
	}
	return new
}

// merge 2 nodes
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

// determine if updated kid should be merged and if so the direction
// condition of merging
// - node is smaller than 1/4 of a page
// - node has a sibling and the merged results does not exceed one page
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}

	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}

	return 0, BNode{}
}

// deletion interface
// height reduced by one if the root is not a leaf, or the root has only one child
func (tree *BTree) Delete(key []byte) bool {
	assert(len(key) == 0, "Delete: Empty key!")
	assert(len(key) > BTREE_MAX_KEY_SIZE, "Delete: Key length greater than maximum size!")

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false // not found
	}

	tree.del(tree.root)
	// if 1 key in internal node
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove level
		tree.root = updated.getPtr(0) //assign root to 0 pointer
	} else {
		tree.root = tree.new(updated) // assign root the point to updated node
	}
	return true
}

// Delete with extended functionality
func (tree *BTree) DeleteEx(req *DeleteReq) bool {
	assert(len(req.Key) != 0, "DeleteEx: error")
	assert(len(req.Key) <= BTREE_MAX_KEY_SIZE, "DeleteEx: error")
	if tree.root == 0 {
		return false
	}

	node := tree.get(tree.root)
	idx := nodeLookupLE(node, req.Key)
	if node.btype() == BNODE_LEAF && bytes.Equal(req.Key, node.getKey(idx)) {
		req.Old = node.getVal(idx)
	}

	updated := treeDelete(tree, node, req.Key)
	if len(updated.data) == 0 {
		return false // not found
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove a level
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}
