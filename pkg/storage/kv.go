package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"godb/internal/constants"
	"godb/internal/util"
	"godb/pkg/btree"
	"os"
	"path"
	"syscall"
)

const DB_SIG = "BuildYourOwnDB07"

type KV struct {
	Path string

	// internals
	Fsync func(int) error // overridable; for testing
	fd    int             // file descriptor
	mmap  struct {
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}

	page struct {
		flushed uint64 // database size in number of pages
		nappend uint64 // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page.
		updates map[uint64][]byte // pending updates, including appended pages
	}
	Tree btree.BTree
	free FreeList

	failed bool // Did the last update fail?
}

// `BTree.new`, allocate a new page.
func (db *KV) pageAlloc(node []byte) uint64 {
	// Try to get a page from the free list
	if ptr := db.free.PopHead(); ptr != 0 {
		db.page.updates[ptr] = node
		return ptr
	}
	// If free list is empty, append a new page
	return db.pageAppend(node)
}

// `FreeList.new`, append a new page
func (db *KV) pageAppend(node []byte) uint64 {
	util.Assert(len(node) == constants.PageSize, "pageAppend: node size must be page size")
	ptr := db.page.flushed + db.page.nappend
	db.page.nappend++
	util.Assert(db.page.updates[ptr] == nil, "pageAppend: page already exists")
	db.page.updates[ptr] = node
	return ptr
}

// Update an existing page (used by free list)
func (db *KV) pageWrite(ptr uint64) []byte {
	// Check if there's already a pending update
	if node, ok := db.page.updates[ptr]; ok {
		return node // pending update
	}
	// Create a new buffer and copy the current page content
	node := make([]byte, constants.PageSize)
	copy(node, db.pageReadFile(ptr))
	db.page.updates[ptr] = node
	return node
}

// `BTree.get`, read a page (used by both B+tree and free list)
func (db *KV) pageRead(ptr uint64) []byte {
	// Check for pending updates first
	if node, ok := db.page.updates[ptr]; ok {
		return node // pending update
	}
	// Otherwise read from the file
	return db.pageReadFile(ptr)
}

func (db *KV) pageReadFile(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/constants.PageSize
		if ptr < end {
			offset := constants.PageSize + (ptr - start)
			return chunk[offset : offset+constants.PageSize]
		}
		start = end
	}
	panic("bad ptr")
}

/*
the 1st page stores the root pointer and other auxiliary data.
| sig | root_ptr | page_used | head_page | head_seq | tail_page | tail_seq |
| 16B |    8B    |     8B    |     8B    |    8B    |     8B    |    8B    |
*/
func loadMeta(db *KV, data []byte) {
	db.Tree.SetRoot(binary.LittleEndian.Uint64(data[16:24]))
	db.page.flushed = binary.LittleEndian.Uint64(data[24:32])
	db.free.headPage = binary.LittleEndian.Uint64(data[32:40])
	db.free.headSeq = binary.LittleEndian.Uint64(data[40:48])
	db.free.tailPage = binary.LittleEndian.Uint64(data[48:56])
	db.free.tailSeq = binary.LittleEndian.Uint64(data[56:64])
}

func saveMeta(db *KV) []byte {
	var data [64]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:24], db.Tree.GetRoot())
	binary.LittleEndian.PutUint64(data[24:32], db.page.flushed)
	binary.LittleEndian.PutUint64(data[32:40], db.free.headPage)
	binary.LittleEndian.PutUint64(data[40:48], db.free.headSeq)
	binary.LittleEndian.PutUint64(data[48:56], db.free.tailPage)
	binary.LittleEndian.PutUint64(data[56:64], db.free.tailSeq)
	return data[:]
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize%constants.PageSize != 0 {
		return errors.New("file is not a multiple of pages")
	}
	if fileSize == 0 { // empty file
		// reserve 2 pages: the meta page and a free list node
		db.page.flushed = 2
		// add an initial node to the free list so it's never empty
		db.free.headPage = 1 // the 2nd page
		db.free.tailPage = 1
		return nil // the meta page will be written in the 1st update
	}
	// read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	// initialize the free list
	db.free.SetMaxSeq()
	// verify the page
	bad := !bytes.Equal([]byte(DB_SIG), data[:16])
	// pointers are within range?
	maxpages := uint64(fileSize / constants.PageSize)
	bad = bad || !(0 < db.page.flushed && db.page.flushed <= maxpages)
	bad = bad || !(0 < db.Tree.GetRoot() && db.Tree.GetRoot() < db.page.flushed)
	bad = bad || !(0 < db.free.headPage && db.free.headPage < db.page.flushed)
	bad = bad || !(0 < db.free.tailPage && db.free.tailPage < db.page.flushed)
	if bad {
		return errors.New("bad meta page")
	}
	return nil
}

// update the meta page. it must be atomic.
func updateRoot(db *KV) error {
	// NOTE: atomic?
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

// extend the mmap by adding new mappings.
func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil // enough range
	}
	alloc := max(db.mmap.total, 64<<20) // double the current address space
	for db.mmap.total+alloc < size {
		alloc *= 2 // still not enough?
	}
	chunk, err := syscall.Mmap(
		db.fd, int64(db.mmap.total), alloc,
		syscall.PROT_READ, syscall.MAP_SHARED, // read-only
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func updateFile(db *KV) error {
	// 1. Write new nodes.
	if err := writePages(db); err != nil {
		return err
	}
	// 2. `fsync` to enforce the order between 1 and 3.
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	// 3. Update the root pointer atomically.
	if err := updateRoot(db); err != nil {
		return err
	}
	// 4. `fsync` to make everything persistent.
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	// prepare the free list for the next update
	db.free.SetMaxSeq()
	return nil
}

func updateOrRevert(db *KV, meta []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if db.failed {
		if _, err := syscall.Pwrite(db.fd, meta, 0); err != nil {
			return fmt.Errorf("rewrite meta page: %w", err)
		}
		if err := db.Fsync(db.fd); err != nil {
			return err
		}
		db.failed = false
	}
	// 2-phase update
	err := updateFile(db)
	// revert on error
	if err != nil {
		// the on-disk meta page is in an unknown state.
		// mark it to be rewritten on later recovery.
		db.failed = true
		// in-memory states are reverted immediately to allow reads
		loadMeta(db, meta)
		// discard temporaries
		db.page.nappend = 0
		db.page.updates = map[uint64][]byte{}
	}
	return err
}

func writePages(db *KV) error {
	// extend the mmap if needed
	size := (db.page.flushed + db.page.nappend) * constants.PageSize
	if err := extendMmap(db, int(size)); err != nil {
		return err
	}
	// write data pages to the file
	for ptr, node := range db.page.updates {
		offset := int64(ptr * constants.PageSize)
		// TODO: check whether this is in linux
		// if _, err := unix.Pwrite(db.fd, node, offset); err != nil {
		// 	return err
		// }
		if _, err := syscall.Pwrite(db.fd, node, offset); err != nil {
			return err
		}
	}
	// discard in-memory data
	db.page.flushed += db.page.nappend
	db.page.nappend = 0
	db.page.updates = map[uint64][]byte{}
	return nil
}

// Open or create a file and fsync the directory
func createFileSync(file string) (int, error) {
	// obtain the directory id
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)

	// Open or create the file
	flags = os.O_RDWR | os.O_CREATE
	// TODO: find another way for linux
	// fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	f, err := os.OpenFile(file, flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	fd := int(f.Fd())
	return fd, nil
}

// KV interfaces

// open or create a DB file
func (db *KV) Open() error {
	if db.Fsync == nil {
		db.Fsync = syscall.Fsync
	}
	var err error
	db.page.updates = map[uint64][]byte{}
	// B+tree callbacks
	db.Tree.SetCallbacks(db.pageRead, db.pageAlloc, db.free.PushTail)
	// free list callbacks
	db.free.SetCallbacks(db.pageRead, db.pageAppend, db.pageWrite)
	// open or create the DB file
	if db.fd, err = createFileSync(db.Path); err != nil {
		return err
	}
	// get the file size
	finfo := syscall.Stat_t{}
	if err = syscall.Fstat(db.fd, &finfo); err != nil {
		goto fail
	}
	// create the initial mmap
	if err = extendMmap(db, int(finfo.Size)); err != nil {
		goto fail
	}
	// read the meta page
	if err = readRoot(db, finfo.Size); err != nil {
		goto fail
	}
	return nil
	// error
fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.Tree.Get(key)
}
func (db *KV) Set(key []byte, val []byte) (bool, error) {
	return db.Update(&btree.UpdateReq{
		Key: key,
		Val: val,
	})
}

func (db *KV) Update(req *btree.UpdateReq) (bool, error) {
	meta := saveMeta(db)
	if !db.Tree.Update(req) {
		return false, nil
	}
	err := updateOrRevert(db, meta)
	return err == nil, err
}

func (db *KV) Del(req *btree.DeleteReq) (bool, error) {
	meta := saveMeta(db)
	if !db.Tree.Delete(req) {
		return false, nil
	}
	err := updateOrRevert(db, meta)
	return err == nil, err
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		util.Assert(err == nil, "KV.Close: munmap failed")
	}
	_ = syscall.Close(db.fd)
}
