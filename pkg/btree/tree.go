package btree

import (
	"bytes"
	"godb/internal/constants"
	"godb/internal/util"
)

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

// SetCallbacks sets the callback functions for page management
func (tree *BTree) SetCallbacks(get func(uint64) BNode, new func(BNode) uint64, del func(uint64)) {
	tree.get = get
	tree.new = new
	tree.del = del
}

// GetRoot returns the root page pointer
func (tree *BTree) GetRoot() uint64 {
	return tree.root
}

// SetRoot sets the root page pointer
func (tree *BTree) SetRoot(root uint64) {
	tree.root = root
}

// Operation modes
const (
	ModeUpsert     = 0 // insert or replace
	ModeUpdateOnly = 1 // update existing keys
	ModeInsertOnly = 2 // only add new keys
)

// Comparison operations
const (
	CmpGE = +3 // >=
	CmpGT = +2 // >
	CmpLT = -2 // <
	CmpLE = -3 // <=
)

// DeleteReq for B-tree deletion
type DeleteReq struct {
	tree *BTree
	// in
	Key []byte
	// out
	Old []byte
}

// InsertReq for B-tree insertion with additional metadata
type InsertReq struct {
	tree *BTree
	// out
	Added   bool   // added a new key
	Updated bool   // added a new key or an old key was changed
	Old     []byte // the value before the update
	// in
	Key  []byte
	Val  []byte
	Mode int
}

// Get a value by key
func (tree *BTree) Get(key []byte) ([]byte, bool) {
	util.Assert(len(key) != 0, "")
	if tree.root == 0 {
		return nil, false
	}

	node := tree.get(tree.root)
	for node.btype() == NodeTypeInternal {
		idx := nodeLookupLE(node, key)
		node = tree.get(node.getPtr(idx))
	}

	idx := nodeLookupLE(node, key)
	if bytes.Equal(key, node.getKey(idx)) {
		return node.getVal(idx), true
	}
	return nil, false
}

// Insert inserts a key-value pair
func (tree *BTree) Insert(key []byte, val []byte) {
	util.Assert(len(key) != 0, "Empty key!")
	util.Assert(len(key) <= constants.MaxKeySize, "Key length greater than maximum")
	util.Assert(len(val) <= constants.MaxValSize, "Val length greater than maximum")

	if tree.root == 0 { // inserting the first key
		// create first leaf node as root
		root := NewBNode(make([]byte, constants.PageSize))
		root.setHeader(NodeTypeLeaf, 2) // add dummy key = 2 so the tree cover key space
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
		root := NewBNode(make([]byte, constants.PageSize))
		root.setHeader(NodeTypeInternal, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil) // add splitted nodes of old root to new root
		}
		tree.root = tree.new(root) // reassign tree root to newly formed root
	} else {
		tree.root = tree.new(splitted[0])
	}
}

// InsertEx inserts with extended functionality
func (tree *BTree) InsertEx(req *InsertReq) {
	util.Assert(len(req.Key) != 0, "")
	util.Assert(len(req.Key) <= constants.MaxKeySize, "")
	util.Assert(len(req.Val) <= constants.MaxValSize, "")

	if tree.root == 0 {
		// create the first node
		root := NewBNode(make([]byte, constants.PageSize))
		root.setHeader(NodeTypeLeaf, 2)
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
	if node.btype() == NodeTypeLeaf && bytes.Equal(req.Key, node.getKey(idx)) {
		// Key exists, check mode
		if req.Mode == ModeInsertOnly {
			// Key already exists, don't update
			tree.root = tree.new(node)
			return
		}
		// Store old value
		req.Old = node.getVal(idx)
		req.Updated = true
	} else {
		// Key doesn't exist, check mode
		if req.Mode == ModeUpdateOnly {
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
		root := NewBNode(make([]byte, constants.PageSize))
		root.setHeader(NodeTypeInternal, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(splitted[0])
	}
}

// Delete deletes a key
func (tree *BTree) Delete(key []byte) bool {
	util.Assert(len(key) != 0, "Delete: Empty key!")
	util.Assert(len(key) <= constants.MaxKeySize, "Delete: Key length greater than maximum size!")

	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.GetData()) == 0 {
		return false // not found
	}

	tree.del(tree.root)
	// if 1 key in internal node
	if updated.btype() == NodeTypeInternal && updated.nkeys() == 1 {
		// remove level
		tree.root = updated.getPtr(0) //assign root to 0 pointer
	} else {
		tree.root = tree.new(updated) // assign root the point to updated node
	}
	return true
}

// CmpOK performs key comparison
func CmpOK(key []byte, cmp int, ref []byte) bool {
	r := bytes.Compare(key, ref)
	switch cmp {
	case CmpGE:
		return r >= 0
	case CmpGT:
		return r > 0
	case CmpLT:
		return r < 0
	case CmpLE:
		return r <= 0
	default:
		panic("comparison not expected")
	}
}
