package zset

// skiplist is an improved version of
// sorted linked list
// it uses multi-level struture to
// make express lane to fast search keys
// rather than a linked list
// a skip list is more like a table of
// rows of linked list
// 1 ----------------->5
// 1 ------->3-------->5
// 1 -> 2 -> 3 -> 4 -> 5

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
