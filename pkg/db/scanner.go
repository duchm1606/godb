package db

import (
	"fmt"
	"godb/internal/util"
	"godb/pkg/btree"
	"slices"
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
	db     *DB
	tdef   *TableDef
	index  int          // which index?
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
	tdef := sc.tdef
	// prepare the output record
	rec.Cols = slices.Concat(tdef.Indexes[0], nonPrimaryKeyCols(tdef))
	rec.Vals = rec.Vals[:0]
	for _, c := range rec.Cols {
		tp := tdef.Types[slices.Index(tdef.Cols, c)]
		rec.Vals = append(rec.Vals, Value{Type: tp})
	}
	// fetch the KV from the iterator
	key, val := sc.iter.Deref()
	// primary key or secondary index?
	if sc.index == 0 {
		// decode the full row
		npk := len(tdef.Indexes[0])
		decodeKey(key, rec.Vals[:npk])
		decodeValues(val, rec.Vals[npk:])
	} else {
		// decode the index key
		util.Assert(len(val) == 0, "Scanner: index key is not empty") // index key is not empty
		index := tdef.Indexes[sc.index]
		irec := Record{index, make([]Value, len(index))}
		for i, c := range index {
			irec.Vals[i].Type = tdef.Types[slices.Index(tdef.Cols, c)]
		}
		decodeKey(key, irec.Vals)
		// extract the primary key
		for i, c := range tdef.Indexes[0] {
			rec.Vals[i] = *irec.Get(c)
		}
		// fetch the row by the primary key
		// TODO: skip this if the index contains all the columns
		ok, err := dbGet(sc.db, tdef, rec)
		util.Assert(ok && err == nil, "Scanner: internal consistency") // internal consistency
	}
}

// check column types
func checkTypes(tdef *TableDef, rec Record) error {
	if len(rec.Cols) != len(rec.Vals) {
		return fmt.Errorf("bad record")
	}
	for i, c := range rec.Cols {
		j := slices.Index(tdef.Cols, c)
		if j < 0 || tdef.Types[j] != rec.Vals[i].Type {
			return fmt.Errorf("bad column: %s", c)
		}
	}
	return nil
}

func dbScan(db *DB, tdef *TableDef, req *Scanner) error {
	// 0. sanity checks
	switch {
	case req.Cmp1 > 0 && req.Cmp2 < 0:
	case req.Cmp2 > 0 && req.Cmp1 < 0:
	default:
		return fmt.Errorf("bad range")
	}
	if !slices.Equal(req.Key1.Cols, req.Key2.Cols) {
		return fmt.Errorf("bad range key")
	}
	if err := checkTypes(tdef, req.Key1); err != nil {
		return err
	}
	if err := checkTypes(tdef, req.Key2); err != nil {
		return err
	}
	req.db = db
	req.tdef = tdef
	// Select the index
	isCovered := func(index []string) bool {
		key := req.Key1.Cols
		return len(index) >= len(key) && slices.Equal(index[:len(key)], key)
	}
	req.index = slices.IndexFunc(tdef.Indexes, isCovered)
	if req.index < 0 {
		return fmt.Errorf("no index")
	}
	// Encode the start key
	prefix := tdef.Prefixes[req.index]
	keyStart := encodeKeyPartial(nil, prefix, req.Key1.Vals, req.Cmp1)
	req.keyEnd = encodeKeyPartial(nil, prefix, req.Key2.Vals, req.Cmp2)

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
