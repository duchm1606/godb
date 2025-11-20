package btree

import "bytes"

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
