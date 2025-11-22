package btree

import (
	"bytes"
)

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(req *UpdateReq, new BNode, node BNode, idx uint16) BNode {
	kptr := node.getPtr(idx)
	// recursive insertion to the kid node
	updated := treeInsert(req, req.tree.get(kptr))
	if len(updated) == 0 {
		return BNode{}
	}
	// split the result
	nsplit, split := nodeSplit3(updated)
	// deallocate the kid node
	req.tree.del(kptr)
	// update the kid links
	nodeReplaceKidN(req.tree, new, node, idx, split[:nsplit]...)
	return new
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(req *UpdateReq, node BNode) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	// where to insert the key?
	idx := nodeLookupLE(node, req.Key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(req.Key, node.getKey(idx)) {
			// found the key, update it.
			if req.Mode == MODE_INSERT_ONLY {
				return BNode{}
			}
			if bytes.Equal(req.Val, node.getVal(idx)) {
				return BNode{}
			}
			leafUpdate(new, node, idx, req.Key, req.Val)
			req.Updated = true
			req.Old = node.getVal(idx)
		} else {
			// insert it after the position.
			if req.Mode == MODE_UPDATE_ONLY {
				return BNode{}
			}
			leafInsert(new, node, idx+1, req.Key, req.Val)
			req.Updated = true
			req.Added = true
		}
		return new
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		return nodeInsert(req, new, node, idx)
	default:
		panic("bad node!")
	}
}
