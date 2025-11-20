package btree

import (
	"bytes"
)

// returns the first kid node whose range intersects the key (kid[i] < = key)

// Insert with extended functionality
func (tree *BTree) InsertEx(req *InsertReq) {
	assert(len(req.Key) != 0, "")
	assert(len(req.Key) <= BTREE_MAX_KEY_SIZE, "")
	assert(len(req.Val) <= BTREE_MAX_VAL_SIZE, "")

	if tree.root == 0 {
		// create the first node
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, req.Key, req.Val)
		tree.root = tree.new(root)
		req.Added = true
		req.Updated = true
		return
	}

	node := tree.get(tree.root)
	tree.del(tree.root)

	// Check if key exists
	idx := nodeLookupLE(node, req.Key)
	if node.btype() == BNODE_LEAF && bytes.Equal(req.Key, node.getKey(idx)) {
		// Key exists, check mode
		if req.Mode == MODE_INSERT_ONLY {
			// Key already exists, don't update
			tree.root = tree.new(node)
			return
		}
		// Store old value
		req.Old = node.getVal(idx)
		req.Updated = true
	} else {
		// Key doesn't exist, check mode
		if req.Mode == MODE_UPDATE_ONLY {
			// Key doesn't exist, don't insert
			tree.root = tree.new(node)
			return
		}
		req.Added = true
		req.Updated = true
	}

	node = treeInsert(tree, node, req.Key, req.Val)
	nsplit, splitted := nodeSplit3(node)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(splitted[0])
	}
}

// Get a value by key
func (tree *BTree) Get(key []byte) ([]byte, bool) {
	assert(len(key) != 0, "")
	if tree.root == 0 {
		return nil, false
	}

	node := tree.get(tree.root)
	for node.btype() == BNODE_NODE {
		idx := nodeLookupLE(node, key)
		node = tree.get(node.getPtr(idx))
	}

	idx := nodeLookupLE(node, key)
	if bytes.Equal(key, node.getKey(idx)) {
		return node.getVal(idx), true
	}
	return nil, false
}
