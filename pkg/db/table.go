package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"godb/internal/util"
	"godb/pkg/btree"
	"godb/pkg/storage"
	"slices"
)

// DB is a wrapper of KV
type DB struct {
	Path string
	// internals
	kv     storage.KV
	tables map[string]*TableDef // cached table schemas
}

// DB transaction
type DBTX struct {
	kv storage.KVTX
	db *DB
}

func (db *DB) Begin(tx *DBTX) {
	tx.db = db
	db.kv.Begin(&tx.kv)
}
func (db *DB) Commit(tx *DBTX) error {
	return db.kv.Commit(&tx.kv)
}
func (db *DB) Abort(tx *DBTX) {
	db.kv.Abort(&tx.kv)
}

type TableDef struct {
	// user defined
	Name    string
	Types   []uint32   // column types
	Cols    []string   // column names
	Indexes [][]string // the first index is the primary key
	// auto-assigned B-tree key prefixes for different tables
	Prefixes []uint32
}

const (
	TYPE_ERROR = 0    // uninitialized
	TYPE_BYTES = 1    // string (of arbitrary bytes)
	TYPE_INT64 = 2    // integer; 64-bit signed
	TYPE_INF   = 0xff // do not use
)

// table cell
type Value struct {
	Type uint32 // tagged union
	I64  int64
	Str  []byte
}

// table row
type Record struct {
	Cols []string
	Vals []Value
}

func (rec *Record) AddStr(col string, val []byte) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_BYTES, Str: val})
	return rec
}

func (rec *Record) AddInt64(col string, val int64) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_INT64, I64: val})
	return rec
}

func (rec *Record) Get(key string) *Value {
	for i, c := range rec.Cols {
		if c == key {
			return &rec.Vals[i]
		}
	}
	return nil
}

// internal table: metadata
// JSON serialized TableDef
// create table `@table` (
// `name` string, -- table name
// `def` string,  -- schema
// primary key (`name`)
// )
var TDEF_TABLE = &TableDef{
	Prefixes: []uint32{2},
	Name:     "@table",
	Types:    []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:     []string{"name", "def"},
	Indexes:  [][]string{{"name"}},
}

// internal table: table schemas
var TDEF_META = &TableDef{
	Prefixes: []uint32{1},
	Name:     "@meta",
	Types:    []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:     []string{"key", "val"},
	Indexes:  [][]string{{"key"}},
}

var INTERNAL_TABLES map[string]*TableDef = map[string]*TableDef{
	"@meta":  TDEF_META,
	"@table": TDEF_TABLE,
}

// DB operations

// escape the null byte so that the string contains no null byte.
func escapeString(in []byte) []byte {
	toEscape := bytes.Count(in, []byte{0}) + bytes.Count(in, []byte{1})
	if toEscape == 0 {
		return in // fast path: no escape
	}

	out := make([]byte, len(in)+toEscape)
	pos := 0
	for _, ch := range in {
		if ch <= 1 {
			// using 0x01 as the escaping byte:
			// 00 -> 01 01
			// 01 -> 01 02
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

func unescapeString(in []byte) []byte {
	if bytes.Count(in, []byte{1}) == 0 {
		return in // fast path: no unescape
	}

	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		if in[i] == 0x01 {
			// 01 01 -> 00
			// 01 02 -> 01
			i++
			util.Assert(in[i] == 1 || in[i] == 2, "unescapeString: invalid escaping byte")
			out = append(out, in[i]-1)
		} else {
			out = append(out, in[i])
		}
	}
	return out
}

// order-preserving encoding, explained in the next chapter
func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)        // flip the sign bit
			binary.BigEndian.PutUint64(buf[:], u) // big endian
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null-terminated
		default:
			panic("what?")
		}
	}
	return out
}

func encodeKeyPartial(
	out []byte, prefix uint32, vals []Value, cmp int,
) []byte {
	out = encodeKey(out, prefix, vals)
	if cmp == btree.CMP_GT || cmp == btree.CMP_LE { // encode missing columns as infinity
		out = append(out, 0xff) // unreachable +infinity
	} // else: -infinity is the empty string
	return out
}

// encode columns for the "key" of the KV
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	// 4-byte table prefix
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	// order-preserving encoded keys
	out = encodeValues(out, vals)
	return out
}

// decode columns from the "value" of the KV
func decodeValues(in []byte, out []Value) {
	for i := range out {
		switch out[i].Type {
		case TYPE_INT64:
			u := binary.BigEndian.Uint64(in[:8])
			out[i].I64 = int64(u - (1 << 63))
			in = in[8:]
		case TYPE_BYTES:
			idx := bytes.IndexByte(in, 0)
			util.Assert(idx >= 0, "decodeValues: null byte not found")
			out[i].Str = unescapeString(in[:idx])
			in = in[idx+1:]
		default:
			panic("what?")
		}
	}
	util.Assert(len(in) == 0, "decodeValues: extra data")
}

// extract multiple column values
func getValues(tdef *TableDef, rec Record, cols []string) ([]Value, error) {
	vals := make([]Value, len(cols))
	for i, c := range cols {
		v := rec.Get(c)
		if v == nil {
			return nil, fmt.Errorf("missing column: %s", tdef.Cols[i])
		}
		if v.Type != tdef.Types[slices.Index(tdef.Cols, c)] {
			return nil, fmt.Errorf("bad column type: %s", c)
		}
		vals[i] = *v
	}
	return vals, nil
}

// get a single row by the primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	// 1. reorder the input columns according to the schema
	values, err := getValues(tdef, *rec, tdef.Indexes[0])
	if err != nil {
		return false, err // not a primary key
	}
	// just a shortcut for the scan operation
	sc := Scanner{
		Cmp1: btree.CMP_GE,
		Cmp2: btree.CMP_LE,
		Key1: Record{tdef.Indexes[0], values},
		Key2: Record{tdef.Indexes[0], values},
	}
	if err := dbScan(db, tdef, &sc); err != nil || !sc.Valid() {
		return false, err
	}
	sc.Deref(rec)
	return true, nil
}

// get the table schema by name
func getTableDef(db *DB, name string) *TableDef {
	if tdef, ok := INTERNAL_TABLES[name]; ok {
		return tdef // expose internal tables
	}
	tdef := db.tables[name]
	if tdef == nil {
		if tdef = getTableDefDB(db, name); tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	util.Assert(err == nil, "getTableDefDB: failed to get table definition")
	if !ok {
		return nil
	}

	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	util.Assert(err == nil, "getTableDefDB: failed to unmarshal table definition")
	return tdef
}

const TABLE_PREFIX_MIN = 100

func checkIndexCols(tdef *TableDef, index []string) ([]string, error) {
	if len(index) == 0 {
		return nil, fmt.Errorf("empty index")
	}
	seen := map[string]bool{}
	for _, c := range index {
		// check the index columns
		if slices.Index(tdef.Cols, c) < 0 {
			return nil, fmt.Errorf("unknown index column: %s", c)
		}
		if seen[c] {
			return nil, fmt.Errorf("duplicated column in index: %s", c)
		}
		seen[c] = true
	}
	// add the primary key to the index
	for _, c := range tdef.Indexes[0] {
		if !seen[c] {
			index = append(index, c)
		}
	}
	util.Assert(len(index) <= len(tdef.Cols), "checkIndexCols: index columns exceed the table columns")
	return index, nil
}

func tableDefCheck(tdef *TableDef) error {
	// verify the table schema
	bad := tdef.Name == "" || len(tdef.Cols) == 0 || len(tdef.Indexes) == 0 ||
		len(tdef.Cols) != len(tdef.Types)
	if bad {
		return fmt.Errorf("bad table schema: %s", tdef.Name)
	}
	// verify the indexes
	for i, index := range tdef.Indexes {
		index, err := checkIndexCols(tdef, index)
		if err != nil {
			return err
		}
		tdef.Indexes[i] = index
	}
	return nil
}

func (db *DB) TableNew(tdef *TableDef) error {
	// 0. sanity checks
	if err := tableDefCheck(tdef); err != nil {
		return err
	}
	// 1. check the existing table
	table := (&Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, TDEF_TABLE, table)
	util.Assert(err == nil, "TableNew: failed to get table definition")
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}
	// 2. allocate a new prefix
	prefix := uint32(TABLE_PREFIX_MIN)
	meta := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err = dbGet(db, TDEF_META, meta)
	util.Assert(err == nil, "TableNew: failed to get next prefix")
	if ok {
		prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
		util.Assert(prefix > TABLE_PREFIX_MIN, "TableNew: next prefix is less than TABLE_PREFIX_MIN")
	} else {
		meta.AddStr("val", make([]byte, 4))
	}
	util.Assert(len(tdef.Prefixes) == 0, "TableNew: prefixes are already set")
	for i := range tdef.Indexes {
		tdef.Prefixes = append(tdef.Prefixes, prefix+uint32(i))
	}
	// 3. update the next prefix
	// FIXME: integer overflow.
	next := prefix + uint32(len(tdef.Indexes))
	binary.LittleEndian.PutUint32(meta.Get("val").Str, next)
	_, err = dbUpdate(db, TDEF_META, &DBUpdateReq{Record: *meta})
	if err != nil {
		return err
	}
	// 4. store the schema
	val, err := json.Marshal(tdef)
	util.Assert(err == nil, "TableNew: failed to marshal table definition")
	table.AddStr("def", val)
	_, err = dbUpdate(db, TDEF_TABLE, &DBUpdateReq{Record: *table})
	return err
}

type DBUpdateReq struct {
	// in
	Record Record
	Mode   int
	// out
	Updated bool
	Added   bool
}

// get a single row by the primary key
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

// add a row to the table
func dbUpdate(db *DB, tdef *TableDef, dbreq *DBUpdateReq) (bool, error) {
	// reorder the columns so that they start with the primary key
	cols := slices.Concat(tdef.Indexes[0], nonPrimaryKeyCols(tdef))
	values, err := getValues(tdef, dbreq.Record, cols)
	if err != nil {
		return false, err // expect a full row
	}

	// insert the row
	npk := len(tdef.Indexes[0]) // number of primary key columns
	key := encodeKey(nil, tdef.Prefixes[0], values[:npk])
	val := encodeValues(nil, values[npk:])
	req := btree.UpdateReq{Key: key, Val: val, Mode: dbreq.Mode}
	if _, err = db.kv.Update(&req); err != nil {
		return false, err
	}
	dbreq.Added, dbreq.Updated = req.Added, req.Updated

	// maintain secondary indexes
	if req.Updated && !req.Added {
		// construct the old record
		decodeValues(req.Old, values[npk:])
		oldRec := Record{cols, values}
		// delete the indexed keys
		if err = indexOp(db, tdef, INDEX_DEL, oldRec); err != nil {
			return false, err // TODO: revert previous updates
		}
	}
	if req.Updated {
		// add the new indexed keys
		if err = indexOp(db, tdef, INDEX_ADD, dbreq.Record); err != nil {
			return false, err // TODO: revert previous updates
		}
	}
	return req.Updated, nil
}

func nonPrimaryKeyCols(tdef *TableDef) (out []string) {
	for _, c := range tdef.Cols {
		if slices.Index(tdef.Indexes[0], c) < 0 {
			out = append(out, c)
		}
	}
	return
}

const (
	INDEX_ADD = 1
	INDEX_DEL = 2
)

// add or remove secondary index keys
func indexOp(db *DB, tdef *TableDef, op int, rec Record) error {
	for i := 1; i < len(tdef.Indexes); i++ {
		// the indexed key
		values, err := getValues(tdef, rec, tdef.Indexes[i])
		util.Assert(err == nil, "indexOp: full row") // full row
		key := encodeKey(nil, tdef.Prefixes[i], values)
		switch op {
		case INDEX_ADD:
			req := btree.UpdateReq{Key: key, Val: nil}
			_, err = db.kv.Update(&req)
			util.Assert(req.Added, "indexOp: internal consistency") // internal consistency
		case INDEX_DEL:
			deleted := false
			deleted, err = db.kv.Del(&btree.DeleteReq{Key: key})
			util.Assert(deleted, "indexOp: internal consistency") // internal consistency
		default:
			panic("unreachable")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// add a record
func (db *DB) Set(table string, dbreq *DBUpdateReq) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, dbreq)
}

func (db *DB) Insert(table string, rec Record) (bool, error) {
	return db.Set(table, &DBUpdateReq{Record: rec, Mode: btree.MODE_INSERT_ONLY})
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	return db.Set(table, &DBUpdateReq{Record: rec, Mode: btree.MODE_UPDATE_ONLY})
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	return db.Set(table, &DBUpdateReq{Record: rec, Mode: btree.MODE_UPSERT})
}

// delete a record by its primary key
func dbDelete(db *DB, tdef *TableDef, rec Record) (bool, error) {
	values, err := getValues(tdef, rec, tdef.Indexes[0])
	if err != nil {
		return false, err
	}
	// delete the row
	key := encodeKey(nil, tdef.Prefixes[0], values)
	req := btree.DeleteReq{Key: key}
	deleted, err := db.kv.Del(&req)
	if err != nil {
		return false, err
	}
	// maintain secondary indexes
	if deleted {
		for _, c := range nonPrimaryKeyCols(tdef) {
			tp := tdef.Types[slices.Index(tdef.Cols, c)]
			values = append(values, Value{Type: tp})
		}
		decodeValues(req.Old, values[len(tdef.Indexes[0]):])
		old := Record{tdef.Cols, values}
		if err = indexOp(db, tdef, INDEX_DEL, old); err != nil {
			return false, err // TODO: revert previous updates
		}
	}
	return deleted, nil
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func (db *DB) Open() error {
	db.kv.Path = db.Path
	db.tables = map[string]*TableDef{}
	return db.kv.Open()
}

func (db *DB) Close() {
	db.kv.Close()
}
