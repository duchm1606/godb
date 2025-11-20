package btree

import (
	"bytes"
)

// assert panics when condition is false.
func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}

const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
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

// add some constraints on the size of the keys and values. So that a node with a single KV pair always fits on a single page
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

type BIter struct {
	tree *BTree
	path []BNode  // from root to leaf
	pos  []uint16 //indexes into nodes
}

const (
	CMP_GE = +3 // >=
	CMP_GT = +2 // >
	CMP_LT = -2 // <
	CMP_LE = -3 // <=
)

// find the closest position that is less or equal to input key
func (tree *BTree) Seek(key []byte, cmp int) *BIter {
	iter := tree.SeekLE(key)
	if cmp != CMP_LE && iter.Valid() {
		cur, _ := iter.Deref()
		if !cmpOK(cur, cmp, key) {
			// off by one
			if cmp > 0 {
				iter.Next()
			} else {
				iter.Prev()
			}
		}
	}
	return iter
}

// key comparison
func cmpOK(key []byte, cmp int, ref []byte) bool {
	r := bytes.Compare(key, ref)
	switch cmp {
	case CMP_GE:
		return r >= 0
	case CMP_GT:
		return r > 0
	case CMP_LT:
		return r < 0
	case CMP_LE:
		return r <= 0
	default:
		panic("comparison not expected")
	}
}

// get the current KV pair
func (iter *BIter) Deref() ([]byte, []byte) {
	// current level
	level := len(iter.path) - 1
	// current node
	node := iter.path[level]
	// current index
	index := iter.pos[len(iter.pos)-1]
	return node.getKey(index), node.getVal(index)
}

// precondition of the Deref()
func (iter *BIter) Valid() bool {
	return len(iter.path)-1 >= 0
}

// moving backward and forward
func (iter *BIter) Prev() {
	iterPrev(iter, len(iter.path)-1)
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]-- // move with this node
	} else if level > 0 {
		iterPrev(iter, level-1) // move to sibling node
	} else {
		return // dummy key
	}
	if level+1 < len(iter.pos) {
		// update the kid node
		node := iter.path[level]
		kid := iter.tree.get(node.getPtr(iter.pos[level]))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nkeys() - 1
	}
}

func (iter *BIter) Next() {
	iterNext(iter, len(iter.path)-1)
}

func iterNext(iter *BIter, level int) {
	if iter.pos[level]+1 < iter.path[level].nkeys() {
		iter.pos[level]++ // move within this node
	} else if level > 0 {
		iterNext(iter, level-1) // move to sibling node
	} else {
		iter.pos[len(iter.pos)-1]++ // past the last key
		return
	}
	if level+1 < len(iter.pos) { // update the child node
		node := iter.path[level]
		kid := iter.tree.get(node.getPtr(iter.pos[level]))
		iter.path[level+1] = kid
		iter.pos[level+1] = 0
	}
}

// BTree.SeekLE is the function for finding the initial position in a range query. It is just a
// normal B-tree lookup with the path recorded.
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := tree.get(ptr)
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		ptr = node.getPtr((idx))
		if node.btype() == BNODE_NODE {
			ptr = node.getPtr(idx)
		} else {
			ptr = 0
		}
	}
	return iter
}
