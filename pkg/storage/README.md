# Storage: Disk & FreeList

## FreeList: Recycle & Reuse
### Memory Management Fundamentals
1. The problem
In a database with a copy-on-write B+tree:
- Each update create new pages
- Old pages become unused
- Without reuse, the database would grow indefinitely

> Without page reuses:
> 1. Initial state: 10 pages
> 2. Update key: Creates 3 new pages (new root, internal node, leaf)
> 3. Old pages are abandoned
> 4. Database now has 13 pages, with 3 unused
> 5. Repeat many times -> database size grows unnecessarily

-> Memory management approaches
- Embedded (Intrusive) Linked List: Store "next" pointers inside the unused pages themselves. No extra space needed for the list structure

    **Problem**: Conflicts with copy-on-write (modifies pages in-place)

- External Lists: Store pointers to unused pages in a separate data structure

    **Challenge**: How to manage the space used by this list itself


2. Linked List on disk
### Linked List on Disk
Requirements for the Free List:
1. Must be page-based (to manage itself)
2. Must handle both additions and removals efficiently
3. Must work with copy-on-write principles
4. Must be crash-safe

The solution is an unrolled linked list:

Node format:

| next_ptr | page_ptrs[] | unused_space |
|   8B     |   n*8B      |     ...      |

**Key Design**
1. Structure
- Each node has a pointer to the next node
- Each node can store multiple page pointers (`FREE_LIST_CAP`)
- Meta page stores pointers to both head and tail nodes
2. Operations
- Add freed pages to the tail
- Take pages for reuse from the head
- When a head node becomes empty, it's recycled (added to the tail)
3. In-place Updates
- Unlike the B+tree nodes, free list nodes are updated in-place
- But crucially, no data is overwritten within a page
- Updates are append-only within each node

### Key Insights
Self-Management
The free list is self-managing in two ways:
Node Recycling: When a head node becomes empty, it's added back to the tail
Growth Management: When a new tail node is needed, it tries to get one from itself first

// The free list manages its own growth:
// 1. When it needs a new node, it tries to get one from itself
// 2. Only if that fails does it allocate a new page
// 3. When a node becomes empty, it's recycled

// This creates a balanced system where:
// - The free list size is proportional to the number of free pages
// - The overhead is minimal

In-Place Updates vs. Copy-on-Write
The free list uses in-place updates, which seems to contradict copy-on-write principles, but:

// Free list updates are safe because:
// 1. No data is overwritten within a page (append-only within the page)
// 2. The meta page is still updated atomically
// 3. If an update is interrupted, the meta page still points to valid data

// This means:
// - No special crash recovery is needed
// - The database remains consistent

Never Empty
The free list is designed to always have at least one node:

// The free list is never empty because:
// 1. It's initialized with one node
// 2. When the tail node is full, a new one is added before it's needed
// 3. When the head node becomes empty, we move to the next node first

// This avoids special cases for an empty list
assert(fl.headPage != 0)  // The list should never be empty

Transaction Boundaries
The free list handles transaction boundaries carefully:

// At transaction boundaries:
// 1. At the start: maxSeq = tailSeq (remember current tail position)
// 2. During transaction: headSeq can't advance past maxSeq
// 3. At the end: SetMaxSeq() (make newly added items available)

// This ensures:
// - Pages freed in the current transaction aren't reused immediately
// - Pages freed in previous transactions are available for reuse