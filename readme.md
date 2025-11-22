https://github.com/JeffersonJefferson-pixel/build-your-own-database/tree/main

pkg/
  btree/           # B-tree implementation
    - node.go
    - tree.go
    - insert.go
    - delete.go
    - iterator.go  # (future)
  
  storage/         # Storage layer
    - kv.go        # Key-value store
    - freelist.go  # Free list management
  
  table/           # Table management (NEW)
    - table.go     # TableDef, table operations
    - schema.go    # Schema management (getTableDef, etc.)
    - internal.go  # Internal tables (@meta, @table)
  
  db/              # High-level database API
    - db.go        # DB struct, lifecycle (Open, Close)
    - types.go     # Value, Record, DBUpdateReq types
    - record.go    # Record operations
    - encoding.go  # Encoding/decoding
    - operations.go # CRUD operations (Get, Set, Insert, etc.)
    - scanner.go   # Table scanning/iteration (future)
  
  ql/              # Query language executor (future)
  
  tx/              # Transaction management (future)