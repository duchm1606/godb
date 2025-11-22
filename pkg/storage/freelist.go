package storage

import (
	"encoding/binary"
	"godb/internal/constants"
	"godb/internal/util"
)

const FREE_LIST_HEADER = 8

/*
Present a free list node

Each node starts with a pointer to the next node.
Items are appended next to it.

Node format:

| next | pointers | unused |

| 8B | n*8B | ... |
*/
type LNode []byte

func (node LNode) getNext() uint64 {
	return binary.LittleEndian.Uint64(node[:8])
}

func (node LNode) setNext(next uint64) {
	binary.LittleEndian.PutUint64(node[:8], next)
}

func (node LNode) getPtr(idx int) uint64 {
	offset := FREE_LIST_HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[offset:])
}
func (node LNode) setPtr(idx int, ptr uint64) {
	offset := FREE_LIST_HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[offset:], ptr)
}

// FreeList manages unused pages
type FreeList struct {
	// callbacks for managing on-disk pages
	get func(uint64) []byte // read a page
	new func([]byte) uint64 // append a new page
	set func(uint64) []byte // update an existing page

	// persited data in the meta page
	headPage uint64 // pointer to the list head node
	headSeq  uint64 // monotonic sequence number to index into the list head
	tailPage uint64
	tailSeq  uint64

	// in-memory states
	maxSeq uint64 // save `tailSeq` to prevent consuming newly added items
}

// SetCallbacks sets the callback functions for page management
func (fl *FreeList) SetCallbacks(get func(uint64) []byte, new func([]byte) uint64, set func(uint64) []byte) {
	fl.get = get
	fl.new = new
	fl.set = set
}

/**
* Sequence numbers
* ----------------------------
* Sequence number (`headSeq`, `tailSeq`) indexes into head and tail nodes. A clever aspect of the design is using monotonically increasing sequence numbers
* They provide a unique identifier for each position in the list. They make it easy to determine when a node is full or empty.
 */

// Convert a sequence number to an index within a node
func seq2idx(seq uint64) int {
	return int(seq % uint64(constants.FreeListCap))
}

/**
* During an update transaction, the free list is both
* - Added to (when pages are freed)
* - Removed from (when pages are needed)
 */

// make the newly added items available for consumption
func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

// Consuming from the free list
//
// Remove 1 item from the head node, and remove the head node if empty
func flPop(fl *FreeList) (ptr uint64, head uint64) {
	if fl.headSeq == fl.maxSeq {
		return 0, 0 // cannot advance
	}
	node := LNode(fl.get(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq)) // item
	fl.headSeq++
	// move to the next one if the head node is empty
	if seq2idx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext()
		util.Assert(fl.headPage != 0, "free list should never be empty")
	}
	return
}

// get 1 item from the list head. return 0 on failure.
func (fl *FreeList) PopHead() uint64 {
	ptr, head := flPop(fl)
	if head != 0 { // the empty head node is recycled
		fl.PushTail(head)
	}
	return ptr
}

func (fl *FreeList) PushTail(ptr uint64) {
	// add it to the tail node
	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	// add a new tail node if it's full (the list is never empty)
	if seq2idx(fl.tailSeq) == 0 {
		// try to reuse from the list head
		next, head := flPop(fl) // may remove the head node
		if next == 0 {
			// or allocate a new node by appending
			next = fl.new(make([]byte, constants.PageSize))
		}
		// link to the new tail node
		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		// also add the head node if it's removed
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

// Initialize a new free list
func NewFreeList(get func(uint64) []byte, new func([]byte) uint64, set func(uint64) []byte) *FreeList {
	fl := &FreeList{
		get:      get,
		new:      new,
		set:      set,
		headPage: 0,
		headSeq:  0,
		tailPage: 0,
		tailSeq:  0,
		maxSeq:   0,
	}
	return fl
}
