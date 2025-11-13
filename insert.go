package btree

import (
	"bytes"
	"encoding/binary"
)

// returns the first kid node whose range intersects the key (kid[i] < = key)
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

// add a new key to a leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   // copy everything from old node to new up until the index
	nodeAppendKV(new, idx, 0, key, val)                    // add new kv to new node
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) // copy everything from old node to new node starting after the inserted kv
}

// update a KV in a leaf node
func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)                         // copy everything from old node to new up until the index
	nodeAppendKV(new, idx, 0, key, val)                          // add new kv to new node
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1)) // copy everything from old node to new node starting after the inserted kv
}

// copy multiple KVs into the position
func nodeAppendRange(new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	assert(srcOld+n <= old.nkeys(), "nodeAppendRange: srcOld+n out of range")
	assert(dstNew+n <= new.nkeys(), "nodeAppendRange: dstNew+n out of range")

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

// insert a KV into a node, the result might be split into 2 nodes
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node types
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a child node
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("treeInsert: unknown node type")
	}
	return new
}

// part of the treeInsert(): kv insertion to an internal node
func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	// recursive insertion to the child node
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := nodeSplit3(knode)
	// update the child link
	nodeReplaceChildNodes(tree, new, node, idx, splited[:nsplit]...)
}

// split a bigger-than-allowed node into two
// the second node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
	// code omitted
}

// split a node if it's too big. The results are 1~3 nodes
// In the worst case, the fat node is split into 3 nodes (one large KV pair in the middle)
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE, "nodeSplit3: leftleft is too large")
	return 3, [3]BNode{leftleft, middle, right}
}

// Inserting a key into a node can result in either 1, 2 or 3 nodes. The parent node must update itself accordingly. The code for updating an internal node is similar to that for updating a leaf node.
func nodeReplaceChildNodes(tree *BTree, new BNode, old BNode, idx uint16, children ...BNode) {
	inc := uint16(len(children))
	// In this B-Tree layout, internal nodes store one KV per child (key is the child’s low key, value is nil), with the first key being a copy from the parent
	// So splitting a child into inc children increases the parent’s entry count by inc - 1.
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range children {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func nodeReplace2Child(new BNode, old BNode, idx uint16, merged uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, merged, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+1))
}

// insertion interface
func (tree *BTree) Insert(key []byte, val []byte) {
	assert(len(key) == 0, "Empty key!")
	assert(len(key) > BTREE_MAX_KEY_SIZE, "Key length greater than maximum")
	assert(len(key) > BTREE_MAX_VAL_SIZE, "Val length greater than maximum")

	if tree.root == 0 { // inserting the first key
		// create first leaf node as root
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2) // add dummy key = 2 so the tree cover key space
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
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil) // add splitted nodes of old root to new root
		}
		tree.root = tree.new(root) // reassign tree root to newly formed root
	} else {
		tree.root = tree.new(splitted[0])
	}
}

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
