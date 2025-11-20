package query

import (
	"godb/pkg/btree"
	"godb/pkg/database"
	"godb/pkg/schema"
	"godb/internal/util"
)

type Scanner struct {
	// the range from key1 to key2
	Cmp1 int
	Cmp2 int
	Key1 schema.Record
	Key2 schema.Record
	// internal
	db      *database.DB
	tdef    *schema.TableDef
	indexNo int // -1: use primary key; >= 0; use an index
	iter    *btree.BIter
	keyEnd  []byte
}

// within the range or not?
func (sc Scanner) Valid() bool {
	if !sc.iter.Valid() {
		return false
	}
	key, _ := sc.iter.Deref()
	return btree.CmpOK(key, sc.Cmp2, sc.keyEnd)
}

// move the underlying b-tree iterator
func (sc *Scanner) Next() {
	util.Assert(sc.Valid(), "ErrSomethingWentWrong")
	if sc.Cmp1 > 0 {
		sc.iter.Next()
	} else {
		sc.iter.Prev()
	}
}

// TODO: implement this
func (sc *Scanner) Deref(rec *schema.Record) {
	util.Assert(sc.Valid(), "ErrSomethingWentWrong")

	tdef := sc.tdef
	rec.Cols = tdef.Cols
	rec.Vals = rec.Vals[:0]
	key, val := sc.iter.Deref()

	if sc.indexNo < 0 {
		// primary key, decode the KV pair
		// Initialize values with correct types
		rec.Vals = make([]schema.Value, len(tdef.Cols))
		for i, typ := range tdef.Types {
			rec.Vals[i].Type = typ
		}
		
		// Decode primary key from key
		keyVals := make([]schema.Value, tdef.PKeys)
		for i := 0; i < tdef.PKeys; i++ {
			keyVals[i].Type = tdef.Types[i]
		}
		schema.DecodeValues(key[4:], keyVals) // skip 4-byte prefix
		copy(rec.Vals[:tdef.PKeys], keyVals)
		
		// Decode remaining values from val
		if len(val) > 0 && tdef.PKeys < len(tdef.Cols) {
			schema.DecodeValues(val, rec.Vals[tdef.PKeys:])
		}
	} else {
		// secondary key - not implemented yet
		panic("secondary indexes not implemented")
	}
}
