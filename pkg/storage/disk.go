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
	"syscall"
)

const DBSig = "BuildYourOwnDB06"

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree btree.BTree
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64 // database size in number of pages
		nfree   int    // number of pages taken from the free list
		nappend int    // number of pages to be appended
		// newly allocated or deallocated pages keyed by the pointer.
		// nil value denotes a deallocated page.
		updates map[uint64][]byte
	}
	free FreeList
}

// create the initial mmap that covers the whole file.
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	if fi.Size()%constants.PageSize != 0 {
		return 0, nil, errors.New("file size is not a multiple of page size")
	}
	mmapSize := 64 << 20
	util.Assert(mmapSize%constants.PageSize == 0, "ERROR!")
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}
	// mmapSize can be larger than the file
	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(fi.Size()), chunk, nil
}

func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*constants.PageSize {
		return nil
	}
	// double the address space
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// Callback for BTree, dereference a pointer.
func (db *KV) pageGet(ptr uint64) btree.BNode {
	if page, ok := db.page.updates[ptr]; ok {
		util.Assert(page != nil, "ERROR!")
		return btree.NewBNode(page) // for new pages
	}
	return pageGetMapped(db, ptr) // for written pages
}

func pageGetMapped(db *KV, ptr uint64) btree.BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/constants.PageSize
		if ptr < end {
			offset := constants.PageSize * (ptr - start)
			return btree.NewBNode(chunk[offset : offset+constants.PageSize])
		}
		start = end
	}
	panic("bad ptr")
}

// the master page format.
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used |
// | 16B | 	   8B     |     8B    |
func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty file, the master page will be created on the first write.
		db.page.flushed = 1 // reserved for the master page
		return nil
	}
	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	// verify the page
	if !bytes.Equal([]byte(DBSig), data[:16]) {
		return errors.New("bad signature")
	}
	bad := !(1 <= used && used <= uint64(db.mmap.file/constants.PageSize))
	bad = bad || !(root < used)
	if bad {
		return errors.New("bad master page")
	}
	db.tree.SetRoot(root)
	db.page.flushed = used
	return nil
}

// update the master page. it must be atomic.
func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DBSig))
	binary.LittleEndian.PutUint64(data[16:], db.tree.GetRoot())
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	// NOTE: Updating the page via mmap is not atomic.
	// 		 Use the `pwrite()` syscall instead.
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

// Callback for BTree, allocate a new page.
func (db *KV) pageNew(node btree.BNode) uint64 {
	util.Assert(len(node.GetData()) <= constants.PageSize, "ERROR!")
	ptr := uint64(0)
	if db.page.nfree < db.free.Total() {
		// reuse a deallocated page
		ptr = db.free.Get(db.page.nfree)
		db.page.nfree++
	} else {
		// append a new page
		ptr = db.page.flushed + uint64(db.page.nappend)
		db.page.nappend++
	}
	db.page.updates[ptr] = node.GetData()
	return ptr
}

// Callback for BTree, deallocate a page.
func (db *KV) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

func (db *KV) Open() error {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp
	// create the initial mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}
	// btree callbacks
	db.tree.SetCallbacks(db.pageGet, db.pageNew, db.pageDel)
	// read the master page
	err = masterLoad(db)
	if err != nil {
		goto fail
	}
	// done
	return nil
fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		util.Assert(err == nil, "ERROR!")
	}
	_ = db.fp.Close()
}

// Read the db
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// Update the db
func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

// Delete from the db
func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

// Update with extended functionality
func (db *KV) Update(req *btree.InsertReq) (bool, error) {
	db.tree.InsertEx(req)
	return req.Added, flushPages(db)
}

// persist the newly allocated pages after updates
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func writePages(db *KV) error {
	// update the free list
	freed := []uint64{}
	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.free.Update(db.page.nfree, freed)

	// extend the file & mmap if needed
	npages := int(db.page.flushed) + db.page.nappend + db.page.nfree
	if err := extendFile(db, npages); err != nil {
		return err
	}
	if err := extendMmap(db, npages); err != nil {
		return err
	}
	// copy pages to the file
	for ptr, page := range db.page.updates {
		if page != nil {
			copy(pageGetMapped(db, ptr).GetData(), page)
		}
	}
	return nil
}

func syncPages(db *KV) error {
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	db.page.flushed += uint64(db.page.nappend)
	db.page.nfree = 0
	db.page.nappend = 0
	clear(db.page.updates)

	// update & flush the master page
	if err := masterStore(db); err != nil {
		return err
	}
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

// extend the file to at least `npages`.
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / constants.PageSize
	if filePages >= npages {
		return nil
	}
	for filePages < npages {
		// the file size is increased exponentially,
		// so that we don't have to extend the file for every update.
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}
	fileSize := filePages * constants.PageSize
	err := db.fp.Truncate(int64(fileSize))
	if err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	db.mmap.file = fileSize
	return nil
}

func (db *KV) GetTree() *btree.BTree {
	return &db.tree
}
