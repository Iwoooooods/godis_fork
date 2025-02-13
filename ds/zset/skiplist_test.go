package zset

import (
	"testing"
)

func TestRandomLevel(t *testing.T) {
	numbers := make(map[int16]int, 100)
	for i := 0; i < 100; i++ {
		numbers[randomLevel()]++
	}
	t.Logf("numbers: %v", numbers)
}

// ... existing code ...

func TestSkiplistInsertAndSearch(t *testing.T) {
	sl := NewSkipList()

	// Test Insert
	column1 := sl.Insert(1.0, "member1")
	if column1 == nil {
		t.Errorf("Insert(1.0, \"member1\") returned nil, expected a column")
	}
	if sl.length != 1 {
		t.Errorf("skiplist length is %d, expected 1", sl.length)
	}

	column2 := sl.Insert(2.0, "member2")
	if column2 == nil {
		t.Errorf("Insert(2.0, \"member2\") returned nil, expected a column")
	}
	if sl.length != 2 {
		t.Errorf("skiplist length is %d, expected 2", sl.length)
	}

	column3 := sl.Insert(1.5, "member3")
	if column3 == nil {
		t.Errorf("Insert(1.5, \"member3\") returned nil, expected a column")
	}
	if sl.length != 3 {
		t.Errorf("skiplist length is %d, expected 3", sl.length)
	}

	// Test Search - positive cases
	foundColumn1 := sl.Search(1.0, "member1")
	if foundColumn1 == nil {
		t.Errorf("Search(1.0, \"member1\") returned nil, expected column1")
	}
	if foundColumn1 != column1 {
		t.Errorf("Search(1.0, \"member1\") returned different column than inserted")
	}

	foundColumn2 := sl.Search(2.0, "member2")
	if foundColumn2 == nil {
		t.Errorf("Search(2.0, \"member2\") returned nil, expected column2")
	}
	if foundColumn2 != column2 {
		t.Errorf("Search(2.0, \"member2\") returned different column than inserted")
	}

	foundColumn3 := sl.Search(1.5, "member3")
	if foundColumn3 == nil {
		t.Errorf("Search(1.5, \"member3\") returned nil, expected column3")
	}
	if foundColumn3 != column3 {
		t.Errorf("Search(1.5, \"member3\") returned different column than inserted")
	}

	// Test Search - negative cases
	notFoundColumn := sl.Search(3.0, "member4")
	if notFoundColumn != nil {
		t.Errorf("Search(3.0, \"member4\") returned %v, expected nil", notFoundColumn)
	}

	notFoundColumn2 := sl.Search(1.0, "wrong_member")
	if notFoundColumn2 != nil {
		t.Errorf("Search(1.0, \"wrong_member\") returned %v, expected nil", notFoundColumn2)
	}
	t.Log(sl.String())
}
