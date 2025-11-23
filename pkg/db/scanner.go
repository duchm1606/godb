package db

import (
	"fmt"
	"godb/internal/util"
	"godb/pkg/btree"
)

// Scanner is a wrapper of the B+tree iterator. It decodes KVs into rows.

// the iterator for range queries
type Scanner struct {
	// the range, from Key1 to Key2
	Cmp1 int // CMP_??
	Cmp2 int
	Key1 Record
	Key2 Record
	// internal
	tdef   *TableDef
	iter   *btree.BIter // the underlying B-tree iterator
	keyEnd []byte       // the encoded Key2
}

// within the range or not?
func (sc *Scanner) Valid() bool {
	if !sc.iter.Valid() {
		return false
	}
	key, _ := sc.iter.Deref()
	return btree.CmpOK(key, sc.Cmp2, sc.keyEnd)
}

// move the underlying B-tree iterator
func (sc *Scanner) Next() {
	util.Assert(sc.Valid(), "Scanner: invalid scanner")
	if sc.Cmp1 > 0 {
		sc.iter.Next()
	} else {
		sc.iter.Prev()
	}
}

func decodeKey(in []byte, out []Value) {
	decodeValues(in[4:], out)
}

// return the current row
func (sc *Scanner) Deref(rec *Record) {
	util.Assert(sc.Valid(), "Scanner: invalid scanner")
	// fetch the KV from the iterator
	key, val := sc.iter.Deref()
	// decode the KV into columns
	rec.Cols = sc.tdef.Cols
	rec.Vals = rec.Vals[:0]
	for _, type_ := range sc.tdef.Types {
		rec.Vals = append(rec.Vals, Value{Type: type_})
	}
	decodeKey(key, rec.Vals[:sc.tdef.PKeys])
	decodeValues(val, rec.Vals[sc.tdef.PKeys:])
}

func dbScan(db *DB, tdef *TableDef, req *Scanner) error {
	// 0. sanity checks
	switch {
	case req.Cmp1 > 0 && req.Cmp2 < 0:
	case req.Cmp2 > 0 && req.Cmp1 < 0:
	default:
		return fmt.Errorf("bad range")
	}
	req.tdef = tdef
	// 1. reorder the input columns according to the schema
	// TODO: allow prefixes
	values1, err := checkRecord(tdef, req.Key1, tdef.PKeys)
	if err != nil {
		return err
	}
	values2, err := checkRecord(tdef, req.Key2, tdef.PKeys)
	if err != nil {
		return err
	}
	// 2. encode the primary key
	keyStart := encodeKey(nil, tdef.Prefix, values1[:tdef.PKeys])
	req.keyEnd = encodeKey(nil, tdef.Prefix, values2[:tdef.PKeys])
	// 3. seek to the start key
	req.iter = db.kv.Tree.Seek(keyStart, req.Cmp1)
	return nil
}

func (db *DB) Scan(table string, req *Scanner) error {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return fmt.Errorf("table not found: %s", table)
	}
	return dbScan(db, tdef, req)
}
