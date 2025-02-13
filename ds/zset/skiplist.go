package zset

import (
	"fmt"
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
	col := &column{
		element: element{
			score:  score,
			member: member,
		},
		rows: make([]*row, level),
	}

	for i := range col.rows {
		col.rows[i] = new(row)
	}

	return col
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
	spans := make([]int64, maxLevel)
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		// traverse forward until either:
		// it reaches the end
		// or the forward score is greater than input
		rank := int64(0)
		for current.rows[i].forward != nil &&
			(current.rows[i].forward.score < score ||
				(current.rows[i].forward.score == score &&
					current.rows[i].forward.member < member)) {
			// 'current' is now the column right before where we want to insert at level i
			current = current.rows[i].forward
			rank += current.rows[i].span
		}
		startAts[i] = current
		spans[i] = rank
	}

	// second, flip coin to decide if we need
	// to add the newLevel of the new column
	newLevel := randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			startAts[i] = sl.header
			spans[i] = 0
		}
		sl.level = newLevel
	}

	// last, make and insert the column into the table
	// or say node into the linked list
	column := NewColumn(newLevel, score, member)
	for i := int16(0); i < newLevel; i++ {
		column.rows[i].forward = startAts[i].rows[i].forward
		startAts[i].rows[i].forward = column

		column.rows[i].span = startAts[i].rows[i].span - (spans[0] - spans[i])
		startAts[i].rows[i].span = (spans[0] - spans[i]) + 1
	}

	// fix backward pointers
	if newLevel > 1 {
		column.backward = startAts[0]
		if column.rows[0].forward != nil {
			column.rows[0].forward.backward = column
		} else {
			sl.tail = column // inserted as the tail
		}
	} else if sl.tail == startAts[0] { // inserted at the tail
		sl.tail = column
	}

	// increment span for levels not touched
	for i := newLevel; i < sl.level; i++ {
		startAts[i].rows[i].span++
	}

	sl.length++
	return column
}

func (sl *skiplist) Search(score float64, member string) *column {
	// start from the header
	// only go down the level when:
	// either current forward to the tail
	// or current forward has higher score
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for current.rows[i].forward != nil &&
			(current.rows[i].forward.score < score ||
				(current.rows[i].forward.score == score &&
					current.rows[i].forward.member < member)) {
			current = current.rows[i].forward
		}
	}

	current = current.rows[0].forward
	if current != nil && current.member == member && current.score == score {
		return current
	}
	return nil
}

func (sl *skiplist) String() string {
	output := fmt.Sprintf("Skiplist Level: %d, Length: %d\n", sl.level, sl.length)
	for level := int16(maxLevel - 1); level >= 0; level-- {
		output += fmt.Sprintf("Level %2d: ", level)
		current := sl.header
		for current != nil {
			if level < int16(len(current.rows)) {
				if level == 0 { // Only print member at level 0
					if current == sl.header {
						output += "header"
					} else {
						output += fmt.Sprintf("[%s:%v]", current.member, current.score)
					}
				} else {
					if current == sl.header {
						output += "header"
					} else {
						output += fmt.Sprintf("[%s:%v]", current.member, current.score)
					}
				}

				if level < int16(len(current.rows)) && current.rows[level] != nil {
					if current.rows[level].forward != nil {
						output += "-->"
					} else {
						output += "-->nil"
					}
					current = current.rows[level].forward
				} else {
					current = nil // Stop at this level if no more rows
				}

			} else {
				current = nil // Should not happen, but for safety
			}

		}
		output += "\n"
	}
	return output
}
