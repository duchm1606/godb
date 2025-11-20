package btree

type BIter struct {
	tree *BTree
	path []BNode  // from root to leaf
	pos  []uint16 // indexes into nodes
}

// find the closest position that is less or equal to input key
func (tree *BTree) Seek(key []byte, cmp int) *BIter {
	iter := tree.SeekLE(key)
	if cmp != CmpLE && iter.Valid() {
		cur, _ := iter.Deref()
		if !CmpOK(cur, cmp, key) {
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

// SeekLE finds the initial position in a range query
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := tree.get(ptr)
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		if node.btype() == NodeTypeInternal {
			ptr = node.getPtr(idx)
		} else {
			ptr = 0
		}
	}
	return iter
}

// get the current KV pair
func (iter *BIter) Deref() ([]byte, []byte) {
	level := len(iter.path) - 1
	node := iter.path[level]
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
		iter.pos[level]-- // move within this node
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
