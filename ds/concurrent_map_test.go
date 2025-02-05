package ds

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestConcurrentPut(t *testing.T) {
	shards := rand.Intn(11)
	d := NewShardedMap(shards)
	count := 100
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "k" + strconv.Itoa(i)
			ret := d.Put(key, i)
			if !ret {
				t.Error("put test failed: expected result 1, actual: " + strconv.Itoa(1) + ", key: " + key)
			}
			val, ok := d.Get(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != i {
					t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) + ", key: " + key)
				}
			} else {
				_, ok := d.Get(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}
		}(i)
	}
	wg.Wait()
}

func TestRandomOpsConcurrently(t *testing.T) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	shards := rand.Intn(11)
	d := NewShardedMap(shards)
	ops := make(map[string]int)

	// Run for 10 seconds
	done := make(chan bool)
	go func() {
		time.Sleep(10 * time.Second)
		close(done)
	}()

	// Launch 10 goroutines to perform random operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))

			for {
				select {
				case <-done:
					return
				default:
					key := "k" + strconv.Itoa(r.Intn(100))
					op := r.Intn(4) // 0: Put, 1: Get, 2: Del, 3: Len

					switch op {
					case 0: // Put
						value := r.Intn(1000)
						d.Put(key, value)
						mu.Lock()
						ops["put"]++
						mu.Unlock()

					case 1: // Get
						_, exists := d.Get(key)
						mu.Lock()
						if exists {
							ops["get_hit"]++
						} else {
							ops["get_miss"]++
						}
						mu.Unlock()

					case 2: // Del
						deleted := d.Del(key)
						mu.Lock()
						if deleted {
							ops["del_hit"]++
						} else {
							ops["del_miss"]++
						}
						mu.Unlock()

					case 3: // Len
						d.Len()
						mu.Lock()
						ops["len"]++
						mu.Unlock()
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Print operation statistics
	t.Logf("Operation statistics:")
	for op, count := range ops {
		t.Logf("%s: %d", op, count)
	}
}
