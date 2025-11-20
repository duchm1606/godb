package constants

// B-Tree Configuration
const (
	// PageSize is the size of a B-tree page in bytes.
	PageSize = 4096

	// MaxKeySize is the maximum size of a key in bytes.
	MaxKeySize = 1000

	// MaxValSize is the maximum size of a value in bytes.
	MaxValSize = 3000

	// HeaderSize is the size of the B-node header in bytes.
	HeaderSize = 4
)

// Free List Configuration
const (
	// FreeListHeader is the size of the free list node header.
	FreeListHeader = 4 + 8 + 8
)

// Calculated constants
var (
	// FreeListCap is the maximum number of pointers in a free list node.
	FreeListCap = (PageSize - FreeListHeader) / 8
)
