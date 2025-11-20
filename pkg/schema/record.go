package schema

// Value types
const (
	TypeError = 0 // error
	TypeBytes = 1 // string (of arbitrary bytes)
	TypeInt64 = 2 // integer; 64-bit signed
)

// table cell
type Value struct {
	Type uint32 // tagged union
	I64  int64
	Str  []byte
}

// table row - list of column names and values
type Record struct {
	Cols []string
	Vals []Value
}

func (rec *Record) AddStr(col string, val []byte) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TypeBytes, Str: val})
	return rec
}

func (rec *Record) AddInt64(col string, val int64) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TypeInt64, I64: val})
	return rec
}

func (rec *Record) Get(col string) *Value {
	for i, recCol := range rec.Cols {
		if recCol == col {
			return &rec.Vals[i]
		}
	}
	return nil
}
