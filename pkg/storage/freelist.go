package storage

import (
	"encoding/binary"
	"errors"
	"godb/internal/constants"
	"godb/internal/util"
	"godb/pkg/btree"
)

const NodeTypeFreeList = 3

var (
	ErrEmptyFreeListNode = errors.New("empty free list node")
	ErrIndexOutOfBound   = "ErrIndexOutOfBound"
)

type FreeList struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(uint64) btree.BNode  // dereference a pointer
	new func(btree.BNode) uint64  // append a new page
	use func(uint64, btree.BNode) // reuse a page
}

func flnSize(node btree.BNode) int {
	return int(binary.LittleEndian.Uint16(node.GetData()[2:4]))
}

func flnNext(node btree.BNode) uint64 {
	return binary.LittleEndian.Uint64(node.GetData()[12:20])
}

func flnPtr(node btree.BNode, idx int) uint64 {
	util.Assert(idx < flnSize(node), ErrIndexOutOfBound)
	pos := constants.FreeListHeader + 8*idx
	return binary.LittleEndian.Uint64(node.GetData()[pos:])
}

func flnSetPtr(node btree.BNode, idx int, ptr uint64) {
	util.Assert(idx < flnSize(node), ErrIndexOutOfBound)
	pos := constants.FreeListHeader + 8*idx
	binary.LittleEndian.PutUint64(node.GetData()[pos:], ptr)
}

func flnSetHeader(node btree.BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node.GetData()[2:4], size)
	binary.LittleEndian.PutUint64(node.GetData()[12:20], next)
}

func flnSetTotal(node btree.BNode, total uint64) {
	binary.LittleEndian.PutUint64(node.GetData()[4:12], total)
}

// number of items in the list
func (fl *FreeList) Total() int {
	if fl.head == 0 {
		return 0
	}
	node := fl.get(fl.head)
	return int(binary.LittleEndian.Uint64(node.GetData()[4:12]))
}

// get the nth pointer
func (fl *FreeList) Get(topn int) uint64 {
	util.Assert(0 <= topn && topn < fl.Total(), "ERROR")
	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		util.Assert(next != 0, "ERROR")
		node = fl.get(next)
	}
	return flnPtr(node, flnSize(node)-topn-1)
}

// remove `popn` pointers and add some new pointers
func (fl *FreeList) Update(popn int, freed []uint64) {
	util.Assert(popn <= fl.Total(), "ERROR")
	if popn == 0 && len(freed) == 0 {
		return // nothing to do
	}
	// prepare to construct the new list
	total := fl.Total()
	reuse := []uint64{}
	for fl.head != 0 && len(reuse)*constants.FreeListCap < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head) // recycle the node itself
		if popn >= flnSize(node) {
			// phase 1
			// remove all pointers in this node
			popn -= flnSize(node)
		} else {
			// phase 2:
			// remove some pointers
			remain := flnSize(node) - popn
			popn = 0
			// reuse pointers from the free list itself
			for remain > 0 && len(reuse)*constants.FreeListCap < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}
			// move the node into the `freed` list
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}
		// discard the node and move to the next node
		total -= flnSize(node)
		fl.head = flnNext(node)
	}
	util.Assert(len(reuse)*constants.FreeListCap >= len(freed) || fl.head == 0, "ERROR")
	// phase 3: prepend new nodes
	flPush(fl, freed, reuse)
	// done
	flnSetTotal(fl.get(fl.head), uint64(total+len(freed)))
}

func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		new := btree.NewBNode(make([]byte, constants.PageSize))
		// construct a new node
		size := len(freed)
		if size > constants.FreeListCap {
			size = constants.FreeListCap
		}
		flnSetHeader(new, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}
		freed = freed[size:]
		if len(reuse) > 0 {
			// reuse a pointer from the list
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, new)
		} else {
			// or append a page to house the new node
			fl.head = fl.new(new)
		}
	}
	util.Assert(len(reuse) == 0, "ERROR")
}
