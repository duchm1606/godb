package schema

import (
	"bytes"
	"encoding/binary"
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
	Prefix uint32

	// Migrate to multiple indexes and prefixes
	Indexes  [][]string // the first index is the primary key
	Prefixed []uint32
}

const TablePrefixMin = 3

// internal table: metadata
var TDefMeta = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TypeBytes, TypeBytes},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// internal table: table schemas
var TDefTable = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TypeBytes, TypeBytes},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

// encode columns for the "key" of the kv
func EncodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = EncodeValues(out, vals)
	return out
}

func EncodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TypeInt64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TypeBytes:
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

func DecodeValues(in []byte, vals []Value) []Value {
	for i := range vals {
		switch vals[i].Type {
		case TypeInt64:
			vals[i].I64 = int64(binary.BigEndian.Uint64(in[:8]))
			in = in[8:]
		case TypeBytes:
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

// reorder a record and check for missing columns
func CheckRecord(tdef *TableDef, rec Record, n int) ([]Value, error) {
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
