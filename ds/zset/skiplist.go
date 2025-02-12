package zset

import (
	"math/bits"
	"math/rand/v2"
)

// skiplist is an improved version of
// sorted linked list
// it uses multi-level struture to
// make express lane to fast search keys
// rather than a linked list
// a skip list is more like a table of
// rows of linked list
// 1 ----------------->6
// 1 ------->3-------->6
// 1 -> 2 -> 3 -> 4 -> 6

const (
	maxLevel = 16
)

type element struct {
	score  float64
	member string
}

type column struct {
	element
	backward *column // to go back to last column

	// rows are the rows that the element occupy
	// like above case, for element 3:
	// rows: [row(0), row(1)]
	rows []*row
}

type row struct {
	forward *column // used to continue to next  column
	span    int64   // to jump to next column, you will cross [span] columns
}

type skiplist struct {
	header *column // hear is a empty column that points to the first column
	tail   *column // tail is used to traverse from behind
	length int64
	level  int16 // is the max level number of this skiplist
}

func NewColumn(level int16, score float64, member string) *column {
	return &column{
		element: element{
			score:  score,
			member: member,
		},
		rows: make([]*row, level),
	}
}

func NewSkipList() *skiplist {
	return &skiplist{
		header: NewColumn(maxLevel, 0, ""),
		level:  1,
	}
}

// assigns a random level number for new node
// doing so to make it
// more likely to be assigned in lower levels
// less likely to be assigned in upper levels
func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k+1)) + 1
}

// 1 ----------------->6
// 1 ------->3-------->6
// 1 -> 2 -> 3 -> 4 -> 6
func (sl *skiplist) Insert(score float64, member string) *column {
	// first, to do decide the position of insertion
	// keep track of the start point the interval in each level
	// for example insert 5, starts: [4, 3, 1], here
	// 4 means the new column should be inserted into the interval
	// between 4 -> 6
	startAts := make([]*column, maxLevel)
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		// traverse forward until either:
		// it reaches the end
		// or the forward score is greater than input
		for current.rows[i].forward != nil &&
			(current.rows[i].forward.score < score ||
				(current.rows[i].forward.score == score &&
					current.rows[i].forward.member < member)) {
			// 'current' is now the column right before where we want to insert at level i
			current = current.rows[i].forward
		}
		startAts[i] = current
	}

	// second, flip coin to decide if we need
	// to add the newLevel of the new column
	newLevel := randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			startAts[i] = sl.header
			startAts[i].rows[i].span = sl.length
		}
		sl.level = newLevel
	}

	// last, make and insert the column into the table
	// or node into the linked list
	column := NewColumn(newLevel, score, member)
	return column
}
