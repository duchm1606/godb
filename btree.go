package btree

import (
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

// assert panics when condition is false.
func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}

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
	assert(idx < node.nkeys(), "getPtr: idx out of range")
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys(), "setPtr: idx out of range")
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
// returns the value of the offset i.e. the location of the kv-pair at given index
func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys(), "offsetPos: idx out of range")
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
	assert(idx <= node.nkeys(), "kvPos: idx out of range")
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// returns key of kv-pair at idx from data array
func (node BNode) getKey(idx uint16) []byte {
	assert(idx <= node.nkeys(), "getKey: idx out of range")
	pos := node.kvPos(idx)                              // byte position of kv-pair
	klen := binary.LittleEndian.Uint16(node.data[pos:]) // 2 bytes that represent the key length
	return node.data[pos+4:][:klen]                     // skip klen, vlen, return key
}

// returns value of kv-pair at idx from data array
func (node BNode) getVal(idx uint16) []byte {
	assert(idx <= node.nkeys(), "getVal: idx out of range")
	pos := node.kvPos(idx) // byte position of kv-pair
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

const (
	BNODE_NODE = 1 // internal node without values
	BNODE_LEAF = 2 // leaf nodes with values
)

const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

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
const HEADER = 4
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000
