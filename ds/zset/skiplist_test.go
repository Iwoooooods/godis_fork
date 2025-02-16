package zset

import (
	"fmt"
	"testing"
)

func TestSkiplistInsert(t *testing.T) {
	skl := newSkiplist()
	skl.insert("a", 1.0)
	skl.insert("b", 2.0)
	skl.insert("c", 3.0)

	if skl.length != 3 {
		t.Errorf("Expected length 3, got %d", skl.length)
	}

	if skl.getRank("a", 1.0) != 1 {
		t.Errorf("Expected rank of 'a' to be 1, got %d", skl.getRank("a", 1.0))
	}
	if skl.getRank("b", 2.0) != 2 {
		t.Errorf("Expected rank of 'b' to be 2, got %d", skl.getRank("b", 2.0))
	}
	if skl.getRank("c", 3.0) != 3 {
		t.Errorf("Expected rank of 'c' to be 3, got %d", skl.getRank("c", 3.0))
	}

	node1 := skl.getByRank(1)
	if node1 == nil || node1.Member != "a" {
		t.Errorf("Expected node at rank 1 to be 'a', got %+v", node1)
	}
	node2 := skl.getByRank(2)
	if node2 == nil || node2.Member != "b" {
		t.Errorf("Expected node at rank 2 to be 'b', got %+v", node2)
	}
	node3 := skl.getByRank(3)
	if node3 == nil || node3.Member != "c" {
		t.Errorf("Expected node at rank 3 to be 'c', got %+v", node3)
	}
}

func TestSkiplistRemove(t *testing.T) {
	skl := newSkiplist()
	skl.insert("a", 1.0)
	skl.insert("b", 2.0)
	skl.insert("c", 3.0)

	if !skl.remove("b", 2.0) {
		t.Errorf("Expected remove 'b' to return true")
	}

	if skl.length != 2 {
		t.Errorf("Expected length 2 after remove, got %d", skl.length)
	}

	if skl.getRank("a", 1.0) != 1 {
		t.Errorf("Expected rank of 'a' to be 1 after remove, got %d", skl.getRank("a", 1.0))
	}
	if skl.getRank("c", 3.0) != 2 {
		t.Errorf("Expected rank of 'c' to be 2 after remove, got %d", skl.getRank("c", 3.0))
	}
	if skl.getRank("b", 2.0) != 0 {
		t.Errorf("Expected rank of 'b' to be 0 after remove, got %d", skl.getRank("b", 2.0))
	}

	node1 := skl.getByRank(1)
	if node1 == nil || node1.Member != "a" {
		t.Errorf("Expected node at rank 1 to be 'a' after remove, got %+v", node1)
	}
	node2 := skl.getByRank(2)
	if node2 == nil || node2.Member != "c" {
		t.Errorf("Expected node at rank 2 to be 'c' after remove, got %+v", node2)
	}
	node3 := skl.getByRank(3)
	if node3 != nil {
		t.Errorf("Expected node at rank 3 to be nil after remove, got %+v", node3)
	}

	if skl.remove("d", 4.0) {
		t.Errorf("Expected remove 'd' to return false as it doesn't exist")
	}
}

func TestSkiplistGetRank(t *testing.T) {
	skl := newSkiplist()
	skl.insert("a", 1.0)
	skl.insert("b", 2.0)
	skl.insert("c", 3.0)

	if skl.getRank("a", 1.0) != 1 {
		t.Errorf("Expected rank of 'a' to be 1, got %d", skl.getRank("a", 1.0))
	}
	if skl.getRank("b", 2.0) != 2 {
		t.Errorf("Expected rank of 'b' to be 2, got %d", skl.getRank("b", 2.0))
	}
	if skl.getRank("c", 3.0) != 3 {
		t.Errorf("Expected rank of 'c' to be 3, got %d", skl.getRank("c", 3.0))
	}
	if skl.getRank("d", 4.0) != 0 {
		t.Errorf("Expected rank of 'd' to be 0, got %d", skl.getRank("d", 4.0))
	}
}

func TestSkiplistGetByRank(t *testing.T) {
	skl := newSkiplist()
	skl.insert("a", 1.0)
	skl.insert("b", 2.0)
	skl.insert("c", 3.0)

	node1 := skl.getByRank(1)
	if node1 == nil || node1.Member != "a" {
		t.Errorf("Expected node at rank 1 to be 'a', got %+v", node1)
	}
	node2 := skl.getByRank(2)
	if node2 == nil || node2.Member != "b" {
		t.Errorf("Expected node at rank 2 to be 'b', got %+v", node2)
	}
	node3 := skl.getByRank(3)
	if node3 == nil || node3.Member != "c" {
		t.Errorf("Expected node at rank 3 to be 'c', got %+v", node3)
	}
	node4 := skl.getByRank(4)
	if node4 != nil {
		t.Errorf("Expected node at rank 4 to be nil, got %+v", node4)
	}

}

func TestSkiplistString(t *testing.T) {
	skl := newSkiplist()
	skl.insert("a", 1.0)
	skl.insert("b", 2.0)
	skl.insert("c", 3.0)

	str := skl.String()
	fmt.Println(str)
}

func TestForEachFunc(t *testing.T) {
	skl := newSkiplist()
	skl.insert("a", 1.0)
	skl.insert("b", 2.0)
	skl.insert("c", 3.0)
	fmt.Println(skl.String())

	skl.forEach(true, func(element *Element) bool {
		t.Logf("member %s with score %f", element.Member, element.Score)
		return true
	})
}
