//go:build linux

package btree

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// table definition
type TableDef struct {
	// user defined
	Name  string
	Types []uint32 // column types
	Cols  []string // column names
	PKeys int      // the first PKeys columns are the primary key
	// auto-assigned B-tree key prefixes for different tables
	// To support multiple tables, the keys in the KV store are prefixed with a unique 32-bit number.
	Prefix uint32
}

// Table definitions have to be stored somewhere, we'll use an internal table to store them.
// And we'll also add an internal table to store the metadata used by the DB itself.

// internal table: metadata
var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// internal table: table schemas
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

// get a single row by primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	// We only allow range queries on the full primary key, but range queries on a prefix of the primary key are also legitimate. We’ll fix this in the next chapter, along with secondary index
	// verify input is a complete primary key
	sc := Scanner{
		Cmp1: CMP_GE,
		Cmp2: CMP_LE,
		Key1: *rec,
		Key2: *rec,
	}
	if err := dbScan(db, tdef, &sc); err != nil {
		return false, err
	}
	if sc.Valid() {
		sc.Deref(rec)
		return true, nil
	} else {
		return false, nil
	}
}

// reorder a record and check for missing columns
// n == tdef.PKeys: record is exactly a primary key
// n == len(tdef.Cols): record contains all columns
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
	// initalize values array
	values := make([]Value, len(tdef.Cols))

	for i := range n {
		found := false
		for j, recCol := range rec.Cols {
			if tdef.Cols[i] == recCol {
				values = append(values, rec.Vals[j])
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column not found: %s", tdef.Cols[i])
		}
	}

	return values, nil
}

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0)
		default:
			panic("value type is unexpected")
		}
	}
	return out
}

func escapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 {
		return in
	}

	out := make([]byte, len(in)+zeros+ones)
	pos := 0
	for _, ch := range in {
		if ch <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

func decodeValues(in []byte, vals []Value) []Value {
	for i := range vals {
		switch vals[i].Type {
		case TYPE_INT64:
			vals[i].I64 = int64(binary.BigEndian.Uint64(in[:8]))
			in = in[8:]
		case TYPE_BYTES:
			vals[i].Str = unescapeString(in)
			in = in[len(vals[i].Str)+1:]
		}
	}
	return vals
}

func unescapeString(in []byte) []byte {
	out := make([]byte, 0, len(in))
	for len(in) > 0 {
		if in[0] == 0x01 {
			out = append(out, in[1]+1)
			in = in[2:]
		} else {
			out = append(out, in[0])
			in = in[1:]
		}
	}
	return out
}

// encode columns for the "key" of the kv
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

func getTableDef(db *DB, name string) *TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*TableDef{}
		}
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

// TODO: make the error code into a constant collection
func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	assert(err == nil, "ERROR!")
	if !ok {
		return nil
	}
	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	assert(err == nil, "ERROR!")
	return tdef
}

// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])
	req := &InsertReq{Key: key, Val: val, Mode: mode}
	return db.kv.Update(req)
}

func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	return db.kv.Del(key)
}

func dbScan(db *DB, tdef *TableDef, sc *Scanner) error {
	// sanity checks
	switch {
	case sc.Cmp1 > 0 && sc.Cmp2 < 0:
	case sc.Cmp2 > 0 && sc.Cmp1 < 0:
	default:
		return fmt.Errorf("bad range")
	}

	values1, err := checkRecord(tdef, sc.Key1, tdef.PKeys)
	if err != nil {
		return err
	}
	values2, err := checkRecord(tdef, sc.Key2, tdef.PKeys)
	if err != nil {
		return err
	}

	sc.tdef = tdef

	// seek to start key
	keyStart := encodeKey(nil, tdef.Prefix, values1[:tdef.PKeys])
	sc.keyEnd = encodeKey(nil, tdef.Prefix, values2[:tdef.PKeys])
	sc.iter = db.kv.tree.Seek(keyStart, sc.Cmp1)
	return nil
}
