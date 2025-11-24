package storage

import (
	"godb/internal/util"
	"godb/pkg/btree"
	"runtime"
)

type KVTX struct {
	db   *KV
	meta []byte // for the rollback
	root uint64 // the saved root pointer for skipping empty transactions
	done bool   // check misuses
}

// begin a transaction
func (kv *KV) Begin(tx *KVTX) {
	tx.db = kv
	tx.meta = saveMeta(tx.db)
	tx.root = tx.db.Tree.GetRoot()
	util.Assert(kv.page.nappend == 0 && len(kv.page.updates) == 0, "KV.Begin: page.nappend or page.updates is not empty")
	// XXX: sanity check; unreliable
	runtime.SetFinalizer(tx, func(tx *KVTX) { util.Assert(tx.done, "KV.Begin: transaction is not done") })
}

// end a transaction: commit updates
func (kv *KV) Commit(tx *KVTX) error {
	util.Assert(!tx.done, "storage.KV.Commit: transaction is already done")
	tx.done = true
	if kv.Tree.GetRoot() == tx.root {
		return nil // no updates?
	}
	return updateOrRevert(tx.db, tx.meta)
}

// end a transaction: rollback
func (kv *KV) Abort(tx *KVTX) {
	util.Assert(!tx.done, "storage.KV.Abort: transaction is already done")
	tx.done = true
	// nothing has written, just revert the in-memory states
	loadMeta(tx.db, tx.meta)
	// discard temporaries
	tx.db.page.nappend = 0
	tx.db.page.updates = map[uint64][]byte{}
}

// KV interfaces
func (tx *KVTX) Get(key []byte) ([]byte, bool) {
	return tx.db.Tree.Get(key)
}
func (tx *KVTX) Seek(key []byte, cmp int) *btree.BIter {
	return tx.db.Tree.Seek(key, cmp)
}
func (tx *KVTX) Update(req *btree.UpdateReq) bool {
	return tx.db.Tree.Update(req)
}
func (tx *KVTX) Set(key []byte, val []byte) bool {
	return tx.Update(&btree.UpdateReq{Key: key, Val: val})
}
func (tx *KVTX) Del(req *btree.DeleteReq) bool {
	return tx.db.Tree.Delete(req)
}
