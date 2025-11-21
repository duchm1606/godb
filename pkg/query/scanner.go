package query

import (
	"fmt"
	"godb/internal/util"
	"godb/pkg/btree"
	"godb/pkg/database"
	"godb/pkg/schema"
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
	indexNo int          // -1: use primary key; >= 0; use an index
	iter    *btree.BIter // underlying b-tree iterator
	keyEnd  []byte       // end of the key range
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

// Scan initializes a scanner for range queries on a table
func Scan(db *database.DB, table string, sc *Scanner) error {
	// todo: add table definition
	// tdef := db.getTableDef(table) // You'll need to expose this method
	// if tdef == nil {
	// 	return fmt.Errorf("table not found: %s", table)
	// }

	// sanity checks
	switch {
	case sc.Cmp1 > 0 && sc.Cmp2 < 0:
	case sc.Cmp2 > 0 && sc.Cmp1 < 0:
	default:
		return fmt.Errorf("bad range")
	}

	values1, err := schema.CheckRecord(sc.tdef, sc.Key1, sc.tdef.PKeys)
	if err != nil {
		return err
	}
	values2, err := schema.CheckRecord(sc.tdef, sc.Key2, sc.tdef.PKeys)
	if err != nil {
		return err
	}

	sc.db = db

	// seek to start key
	keyStart := schema.EncodeKey(nil, sc.tdef.Prefix, values1[:sc.tdef.PKeys])
	sc.keyEnd = schema.EncodeKey(nil, sc.tdef.Prefix, values2[:sc.tdef.PKeys])
	sc.iter = db.Kv.GetTree().Seek(keyStart, sc.Cmp1) // Use the new Seek method
	return nil
}
