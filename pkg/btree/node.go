package btree

import (
	"bytes"
	"duchm1606/godb/internal/constants"
	"duchm1606/godb/internal/util"
	"encoding/binary"
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

type BNode struct {
	data []byte // can be dumped to the disk
}

const HEADER = 4

const (
	BNODE_NODE = 1 // internal node without values
	BNODE_LEAF = 2 // leaf nodes with values
)

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	util.Assert(idx < node.nkeys(), "getPtr: idx out of range")
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	util.Assert(idx < node.nkeys(), "setPtr: idx out of range")
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
// returns the value of the offset i.e. the location of the kv-pair at given index
func offsetPos(node BNode, idx uint16) uint16 {
	util.Assert(1 <= idx && idx <= node.nkeys(), "offsetPos: idx out of range")
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

// returns offset of kv-pair at given index
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}
func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// Key-values
// returns position/byte-offset of kv-pair at idx
func (node BNode) kvPos(idx uint16) uint16 {
	util.Assert(idx <= node.nkeys(), "kvPos: idx out of range")
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// returns key of kv-pair at idx from data array
func (node BNode) getKey(idx uint16) []byte {
	util.Assert(idx <= node.nkeys(), "getKey: idx out of range")
	pos := node.kvPos(idx)                              // byte position of kv-pair
	klen := binary.LittleEndian.Uint16(node.data[pos:]) // 2 bytes that represent the key length
	return node.data[pos+4:][:klen]                     // skip klen, vlen, return key
}

// returns value of kv-pair at idx from data array
func (node BNode) getVal(idx uint16) []byte {
	util.Assert(idx <= node.nkeys(), "getVal: idx out of range")
	pos := node.kvPos(idx) // byte position of kv-pair
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
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
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)                         // copy everything from old node to new up until the index
	nodeAppendKV(new, idx, 0, key, val)                          // add new kv to new node
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1)) // copy everything from old node to new node starting after the inserted kv
}

// add a new key to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   // copy everything from old node to new up until the index
	nodeAppendKV(new, idx, 0, key, val)                    // add new kv to new node
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) // copy everything from old node to new node starting after the inserted kv
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)

	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)

	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
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
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// split a bigger-than-allowed node into two
// the second node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
	// code omitted
}

// split a node if it's too big. The results are 1~3 nodes
// In the worst case, the fat node is split into 3 nodes (one large KV pair in the middle)
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= constants.BtreePageSize {
		old.data = old.data[:constants.BtreePageSize]
		return 1, [3]BNode{old}
	}
	left := BNode{data: make([]byte, 2*constants.BtreePageSize)} // might be split later
	right := BNode{data: make([]byte, constants.BtreePageSize)}
	nodeSplit2(left, right, old)
	if left.nbytes() <= constants.BtreePageSize {
		left.data = left.data[:constants.BtreePageSize]
		return 2, [3]BNode{left, right}
	}
	// the left node is still too large
	leftleft := BNode{make([]byte, constants.BtreePageSize)}
	middle := BNode{make([]byte, constants.BtreePageSize)}
	nodeSplit2(leftleft, middle, left)
	util.Assert(leftleft.nbytes() <= constants.BtreePageSize, "nodeSplit3: leftleft is too large")
	return 3, [3]BNode{leftleft, middle, right}
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// merge 2 nodes
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func nodeReplace2Child(new BNode, old BNode, idx uint16, merged uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, merged, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+1))
}
