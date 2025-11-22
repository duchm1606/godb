package btree

import (
	"bytes"
	"encoding/binary"
	"godb/internal/constants"
	"godb/internal/util"
)

/**
* Node data structure
* 1. Header:
* - Type (2 bytes): Indicates whether the node is a leaf node or an internal node.
* - nkeys (2 bytes): Represents the number of keys stored in the node.
*
* 2. Pointers (List of nkeys * 8 bytes):
* - Present only in internal nodes.
* - Each pointer (8 bytes) corresponds to a child node. Internal nodes use these pointers to navigate through the tree structure.
*
* 3. Offsets (List of nkeys * 2 bytes):
* - Each offset points to the location of the corresponding key-value pair within the key-values section.
* - The offset is relative to the start of the key-values section.
* - The first offset is always 0, as it points to the beginning of the key-values section.
*
* | type | nkeys | pointers  | offsets  | key-values
* | 2B	 | 2B 	 | nkeys *8B | nkeys*2B | ...
*
* 4. Key-Values (Packed KV pairs):
* Pairs of key-value data.
* - klen (2 bytes): Length of the key.
* - vlen (2 bytes): Length of the value.
* - key (variable length): The actual key data.
* - val (variable length): The actual value data.
* - These pairs are packed together without any separators.
*
* | klen | vlen  | key | val |
* | 2B	 | 2B    | ... | ... |
 */

type BNode []byte // can be dumped to the disk

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	util.Assert(idx < node.nkeys(), "getPtr: idx out of range")
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
	util.Assert(idx < node.nkeys(), "setPtr: idx out of range")
	// assert(node.btype() == BNODE_LEAF || val != 0)
	// assert(node.btype() == BNODE_NODE || val == 0)
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// Node Types
const (
	NodeTypeInternal = 1 // internal nodes without values
	NodeTypeLeaf     = 2 // leaf nodes with values
)

// offset list
// returns the value of the offset i.e. the location of the kv-pair at given index
func offsetPos(node BNode, idx uint16) uint16 {
	util.Assert(1 <= idx && idx <= node.nkeys(), "offsetPos: idx out of range")
	return constants.HeaderSize + 8*node.nkeys() + 2*(idx-1)
}

// returns offset of kv-pair at given index
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

// Key-values
// returns position/byte-offset of kv-pair at idx
func (node BNode) kvPos(idx uint16) uint16 {
	util.Assert(idx <= node.nkeys(), "kvPos: idx out of range")
	return constants.HeaderSize + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// returns key of kv-pair at idx from data array
func (node BNode) getKey(idx uint16) []byte {
	util.Assert(idx <= node.nkeys(), "getKey: idx out of range")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen] // skip klen, vlen, return key
}

// returns value of kv-pair at idx from data array
func (node BNode) getVal(idx uint16) []byte {
	util.Assert(idx <= node.nkeys(), "getVal: idx out of range")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// Less-than-or-Equal operation to find the index of the first key that is <= the given key.
// This is a fundamental operation used by insertion, deletion, and traversal.
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()

	found := uint16(0)
	// the first key is a copy from the parent node
	// thus it's always less than or equal to the key

	// start at index 1, if key is greater than current index in node, quit (meaning to add in that index and push everything up)
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// update a KV in a leaf node
func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(NodeTypeLeaf, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)                         // copy everything from old node to new up until the index
	nodeAppendKV(new, idx, 0, key, val)                          // add new kv to new node
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1)) // copy everything from old node to new node starting after the inserted kv
}

// add a new key to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(NodeTypeLeaf, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   // copy everything from old node to new up until the index
	nodeAppendKV(new, idx, 0, key, val)                    // add new kv to new node
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) // copy everything from old node to new node starting after the inserted kv
}

// replace a link with the same key
func nodeReplaceKid1ptr(new BNode, old BNode, idx uint16, ptr uint64) {
	copy(new, old[:old.nbytes()])
	new.setPtr(idx, ptr) // only the pointer is changed
}

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	if inc == 1 && bytes.Equal(kids[0].getKey(0), old.getKey(idx)) {
		// common case, only replace 1 pointer
		nodeReplaceKid1ptr(new, old, idx, tree.new(kids[0]))
		return
	}

	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(
	new BNode, old BNode, idx uint16,
	ptr uint64, key []byte,
) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// copy multiple KVs into the position
func nodeAppendRange(new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	util.Assert(srcOld+n <= old.nkeys(), "nodeAppendRange: srcOld+n out of range")
	util.Assert(dstNew+n <= new.nkeys(), "nodeAppendRange: dstNew+n out of range")

	if n == 0 {
		return
	}

	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}

	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		// The range is [1, n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new[new.kvPos(dstNew):], old[begin:end])
}

// split a bigger-than-allowed node into two
// the second node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
	// code omitted
	util.Assert(old.nkeys() >= 2, "nodeSplit2: old node must have at least 2 keys")
	// the initial guess
	nleft := old.nkeys() / 2

	// try to fit the left half
	left_bytes := func() uint16 {
		return HEADER + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	util.Assert(nleft >= 1, "nodeSplit2: nleft must be at least 1")

	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + HEADER
	}
	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	util.Assert(nleft < old.nkeys(), "nodeSplit2: nleft must be less than old.nkeys()")
	nright := old.nkeys() - nleft

	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	// the left half may be still too big
	util.Assert(right.nbytes() <= BTREE_PAGE_SIZE, "nodeSplit2: right node is too large")
}

// split a node if it's too big. The results are 1~3 nodes
// In the worst case, the fat node is split into 3 nodes (one large KV pair in the middle)
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	util.Assert(leftleft.nbytes() <= BTREE_PAGE_SIZE, "nodeSplit3: leftleft is too large")
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(NodeTypeLeaf, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}
