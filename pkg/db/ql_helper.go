package db

type StmtSplitter struct {
	input []byte
}

func (s *StmtSplitter) Feed(line string) {
	s.input = append(s.input, line...)
}

func (s *StmtSplitter) Pop() ([]byte, bool) {
	idx := 0
loop:
	for idx < len(s.input) {
		switch s.input[idx] {
		case '"', '\'':
			quote := s.input[idx]
			idx++
		str:
			for idx < len(s.input) {
				switch s.input[idx] {
				case quote:
					idx++
					break str
				case '\\':
					idx += 2
				default:
					idx++
				}
			}
		case ';':
			break loop
		default:
			idx++
		}
	}

	if !(idx < len(s.input) && s.input[idx] == ';') {
		return nil, false
	}

	out := s.input[:idx:idx]
	s.input = s.input[idx+1:]
	return out, true
}

func fullStmt(input []byte) (interface{}, error) {
	p := Parser{input: input}
	parsed := pStmt(&p)
	skipSpace(&p)
	if p.idx != len(p.input) {
		pErr(&p, "stmt not ended")
	}
	if p.err != nil {
		return nil, p.err
	}
	return parsed, nil
}

func (db *DB) ExecString(input []byte) (QLResult, error) {
	stmt, err := fullStmt(input)
	if err != nil {
		return QLResult{}, err
	}
	return qlExec(db, stmt)
}
