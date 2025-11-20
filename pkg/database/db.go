package database

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"godb/pkg/btree"
	"godb/pkg/schema"
	"godb/pkg/storage"
)

// wrapper of KV
type DB struct {
	Path string
	// internals
	kv     storage.KV
	tables map[string]*schema.TableDef // cached table definition
}

// get a single row by the primary key
func (db *DB) Get(table string, rec *schema.Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

// add a record
func (db *DB) Set(table string, rec schema.Record, mode int) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbUpdate(db, tdef, rec, mode)
}

func (db *DB) Insert(table string, rec schema.Record) (bool, error) {
	return db.Set(table, rec, btree.ModeInsertOnly)
}

func (db *DB) Update(table string, rec schema.Record) (bool, error) {
	return db.Set(table, rec, btree.ModeUpdateOnly)
}

func (db *DB) Upsert(table string, rec schema.Record) (bool, error) {
	return db.Set(table, rec, btree.ModeUpsert)
}

func (db *DB) Delete(table string, rec schema.Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbDelete(db, tdef, rec)
}

func (db *DB) TableNew(tdef *schema.TableDef) error {
	// check table definition
	if err := tableDefCheck(tdef); err != nil {
		return err
	}

	// check the existing table
	table := (&schema.Record{}).AddStr("name", []byte(tdef.Name))
	ok, err := dbGet(db, schema.TDefTable, table)
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("table exists: %s", tdef.Name)
	}

	// allocate a new prefix
	if tdef.Prefix == 0 {
		tdef.Prefix = schema.TablePrefixMin
		meta := (&schema.Record{}).AddStr("key", []byte("next_prefix"))
		ok, err = dbGet(db, schema.TDefMeta, meta)
		if err != nil {
			return err
		}
		if ok {
			tdef.Prefix = binary.LittleEndian.Uint32(meta.Get("val").Str)
			if tdef.Prefix <= schema.TablePrefixMin {
				return fmt.Errorf("invalid prefix")
			}
		} else {
			meta.AddStr("val", make([]byte, 4))
		}

		// update the next prefix
		binary.LittleEndian.PutUint32(meta.Get("val").Str, tdef.Prefix+1)
		_, err = dbUpdate(db, schema.TDefMeta, *meta, 0)
		if err != nil {
			return err
		}
	}

	// store the definition
	val, err := json.Marshal(tdef)
	if err != nil {
		return err
	}
	table.AddStr("def", val)
	_, err = dbUpdate(db, schema.TDefTable, *table, 0)
	return err
}

func tableDefCheck(tdef *schema.TableDef) error {
	if tdef.Prefix != 0 && tdef.Prefix < schema.TablePrefixMin {
		return fmt.Errorf("invalid prefix")
	}
	// TODO: implement more validation
	return nil
}

func getTableDef(db *DB, name string) *schema.TableDef {
	tdef, ok := db.tables[name]
	if !ok {
		if db.tables == nil {
			db.tables = map[string]*schema.TableDef{}
		}
		tdef = getTableDefDB(db, name)
		if tdef != nil {
			db.tables[name] = tdef
		}
	}
	return tdef
}

func getTableDefDB(db *DB, name string) *schema.TableDef {
	rec := (&schema.Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, schema.TDefTable, rec)
	if err != nil || !ok {
		return nil
	}
	tdef := &schema.TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	if err != nil {
		return nil
	}
	return tdef
}

// add a row to the table
func dbUpdate(db *DB, tdef *schema.TableDef, rec schema.Record, mode int) (bool, error) {
	values, err := schema.CheckRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := schema.EncodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := schema.EncodeValues(nil, values[tdef.PKeys:])
	req := &btree.InsertReq{Key: key, Val: val, Mode: mode}
	return db.kv.Update(req)
}

func dbDelete(db *DB, tdef *schema.TableDef, rec schema.Record) (bool, error) {
	values, err := schema.CheckRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := schema.EncodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	return db.kv.Del(key)
}

// get a single row by primary key
func dbGet(db *DB, tdef *schema.TableDef, rec *schema.Record) (bool, error) {
	// TODO: implement scanner-based approach
	values, err := schema.CheckRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	key := schema.EncodeKey(nil, tdef.Prefix, values)
	val, found := db.kv.Get(key)
	if !found {
		return false, nil
	}

	// decode the result
	rec.Cols = tdef.Cols
	rec.Vals = make([]schema.Value, len(tdef.Cols))
	for i, typ := range tdef.Types {
		rec.Vals[i].Type = typ
	}

	// copy primary key values
	copy(rec.Vals[:tdef.PKeys], values)

	// decode remaining values
	if len(val) > 0 {
		schema.DecodeValues(val, rec.Vals[tdef.PKeys:])
	}

	return true, nil
}
