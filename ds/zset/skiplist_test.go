package zset

import "testing"

func TestRandomLevel(t *testing.T) {
	numbers := make(map[int16]int, 100)
	for i := 0; i < 100; i++ {
		numbers[randomLevel()]++
	}
	t.Logf("numbers: %v", numbers)
}
