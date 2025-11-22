package btree

import (
	"bytes"
	"godb/internal/util"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	util.Assert(node1max <= BTREE_PAGE_SIZE, "node1max must be less than BTREE_PAGE_SIZE")
}

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

// update modes
const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

type UpdateReq struct {
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
type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) []byte // dereference a pointer
	new func([]byte) uint64 // allocate a new page
	del func(uint64)        // deallocate a page
}

func (tree *BTree) SetRoot(root uint64) {
	tree.root = root
}

func (tree *BTree) GetRoot() uint64 {
	return tree.root
}

// SetCallbacks sets the callback functions for page management
func (tree *BTree) SetCallbacks(get func(uint64) []byte, new func([]byte) uint64, del func(uint64)) {
	tree.get = get
	tree.new = new
	tree.del = del
}

// Interfaces

func (tree *BTree) Delete(key []byte) bool {
	util.Assert(len(key) != 0, "Delete: key is empty")
	util.Assert(len(key) <= BTREE_MAX_KEY_SIZE, "Delete: key is too long")
	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated) == 0 {
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

func nodeGetKey(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			return node.getVal(idx), true
		} else {
			return nil, false
		}
	case BNODE_NODE:
		return nodeGetKey(tree, tree.get(node.getPtr(idx)), key)
	default:
		panic("bad node!")
	}
}
func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}
	return nodeGetKey(tree, tree.get(tree.root), key)
}

func (tree *BTree) Update(req *UpdateReq) bool {
	util.Assert(len(req.Key) != 0, "Update: key is empty")
	util.Assert(len(req.Key) <= BTREE_MAX_KEY_SIZE, "Update: key is too long")
	util.Assert(len(req.Val) <= BTREE_MAX_VAL_SIZE, "Update: val is too long")

	if tree.root == 0 {
		// create the first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, req.Key, req.Val)
		tree.root = tree.new(root)
		req.Added = true
		req.Updated = true
		return true
	}

	req.tree = tree
	updated := treeInsert(req, tree.get(tree.root))
	if len(updated) == 0 {
		return false // not updated
	}

	// replace the root node
	nsplit, split := nodeSplit3(updated)
	tree.del(tree.root)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
	return true
}
