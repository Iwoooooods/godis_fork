package zset

import "testing"

func TestSortedSet(t *testing.T) {
	zset := NewSortedSet()
	element, ok := zset.Get("a")
	if ok || element != nil {
		t.Errorf("expected zset.Get('a') to return nil")
	}
}

func TestSortedSetRangeByRank(t *testing.T) {
	zset := NewSortedSet()

	// Test case 1: Empty zset
	members := zset.Range(&IntBorder{value: 0}, &IntBorder{value: 2}, false)
	if len(members) != 0 {
		t.Errorf("Test case 1 failed: Expected empty slice for empty zset, got: %v",
			members)
	}

	zset.Add("a", 1)
	zset.Add("b", 2)
	zset.Add("c", 3)
	zset.Add("d", 4)
	zset.Add("e", 5)

	// Test case 2: Range within bounds
	members = zset.Range(&IntBorder{value: 1}, &IntBorder{value: 3}, false)
	if len(members) != 3 || members[0].Member != "b" || members[1].Member != "c" || members[2].Member != "d" {
		t.Errorf("Test case 2 failed: Expected [b c d], got: %v", members)
	}

	// Test case 3: Range out of bounds (start > length)
	members = zset.Range(&IntBorder{value: 5}, &IntBorder{value: 7}, false)
	if len(members) != 0 {
		t.Errorf("Test case 3 failed: Expected empty slice for start out of bounds, got: %v", members)
	}

	members = zset.Range(&IntBorder{value: 0}, &IntBorder{value: -5}, false)
	if len(members) != 1 || members[0].Member != "a" {
		t.Errorf("Test case 4 failed: Expected empty slice for stop out of bounds (negative and too large), got: ")
		for _, mem := range members {
			t.Log(mem)
		}
	}

	// Test case 4: Range out of bounds (stop < 0)
	members = zset.Range(&IntBorder{value: 0}, &IntBorder{value: -6}, false)
	if len(members) != 0 {
		t.Errorf("Test case 4 failed: Expected empty slice for stop out of bounds (negative and too small), got: %v", members)
	}

	// Test case 5: Range with negative indices
	members = zset.Range(&IntBorder{value: -3}, &IntBorder{value: -1}, false)
	if len(members) != 3 || members[0].Member != "c" || members[1].Member != "d" || members[2].Member != "e" {
		t.Errorf("Test case 5 failed: Expected [c d e], got: %v", members)
	}

	// Test case 6: Range to the end
	members = zset.Range(&IntBorder{value: 2}, &IntBorder{value: 10}, false)
	if len(members) != 3 || members[0].Member != "c" || members[1].Member != "d" || members[2].Member != "e" {
		t.Errorf("Test case 6 failed: Expected [c d e], got: %v", members)
	}

	// Test case 7: start > stop
	members = zset.Range(&IntBorder{value: 3}, &IntBorder{value: 1}, false)
	if len(members) != 0 {
		t.Errorf("Test case 7 failed: Expected empty slice for start > stop, got: %v", members)
	}
}

func TestSortedSetRangeByScore(t *testing.T) {
	zset := NewSortedSet()

	// Test case 1: Empty zset
	members := zset.Range(&FloatBorder{value: 0}, &FloatBorder{value: 2.0}, true)
	if len(members) != 0 {
		t.Errorf("Test case 1 failed: Expected empty slice for empty zset, got: %v",
			members)
	}

	zset.Add("a", 1)
	zset.Add("b", 2)
	zset.Add("c", 3)
	zset.Add("d", 4)
	zset.Add("e", 5)

	// Test case 2: Range within bounds
	members = zset.Range(&FloatBorder{value: 1.0}, &FloatBorder{value: 3.0}, true)
	if len(members) != 3 || members[0].Member != "a" || members[1].Member != "b" || members[2].Member != "c" {
		t.Errorf("Test case 2 failed: Expected [b c d], got: ")
		for _, m := range members {
			t.Errorf("%v", m.Member)
		}
	}

	// Test case 3: Range out of bounds (start > length)
	members = zset.Range(&FloatBorder{value: 100.0}, &FloatBorder{value: 200.0}, true)
	if len(members) != 0 {
		t.Errorf("Test case 3 failed: Expected empty slice for start out of bounds, got: %v", len(members))
	}

	// Test case 4: Min greater than max, should equal to (max, min)
	members = zset.Range(&FloatBorder{value: 1.0}, &FloatBorder{value: -1.0}, true)
	if len(members) != 1 || members[0].Member != "a" {
		t.Errorf("Test case 4 failed: Expected [a], got: ")
		for _, mem := range members {
			t.Error(mem)
		}
	}

	// Test case 5: start > stop
	members = zset.Range(&FloatBorder{value: 3.0}, &FloatBorder{value: 1.0}, true)
	if len(members) != 3 || members[0].Member != "a" || members[1].Member != "b" || members[2].Member != "c" {
		t.Errorf("Test case 5 failed: Expected [a, b, c], got: ")
		for _, mem := range members {
			t.Error(mem)
		}
	}

	// Test case 6: with border excluded
	members = zset.Range(&FloatBorder{value: 1, excluded: true}, &FloatBorder{value: 3}, true)
	if len(members) != 2 || members[0].Member != "b" || members[1].Member != "c" {
		t.Errorf("Test case 6 failed: Expected [b, c], got: ")
		for _, mem := range members {
			t.Error(mem)
		}
	}
}
