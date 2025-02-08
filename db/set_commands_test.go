package db

import (
	"bufio"
	"bytes"
	"fmt"
	"godis/lib/utils"
	"godis/redis/protocol"
	"log"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestBasicSetOperations(t *testing.T) {
	db := NewStandAloneDb()
	key := "myset"

	// Test SADD
	result := SAdd(db, utils.ToCmdLine("myset", "a", "b", "c"))
	intResult := result.ToBytes()
	if string(intResult) != ":3\r\n" {
		t.Errorf("SADD expected 3 new members, got: %s", string(intResult))
	}

	// Test duplicate SADD
	result = SAdd(db, utils.ToCmdLine("myset", "a", "d"))
	intResult = result.ToBytes()
	if string(intResult) != ":1\r\n" {
		t.Errorf("SADD with duplicate expected 1 new member, got: %s", string(intResult))
	}

	// Test SCARD
	result = SCard(db, utils.ToCmdLine(key))
	intResult = result.ToBytes()
	if string(intResult) != ":4\r\n" {
		t.Errorf("SCARD expected 4 members, got: %s", string(intResult))
	}

	// Test SISMEMBER
	result = SIsMember(db, utils.ToCmdLine(key, "a"))
	intResult = result.ToBytes()
	if string(intResult) != ":1\r\n" {
		t.Errorf("SISMEMBER expected 1 for existing member, got: %s", string(intResult))
	}

	result = SIsMember(db, utils.ToCmdLine(key, "x"))
	intResult = result.ToBytes()
	if string(intResult) != ":0\r\n" {
		t.Errorf("SISMEMBER expected 0 for non-existing member, got: %s", string(intResult))
	}

	// Test SREM
	result = SRem(db, utils.ToCmdLine(key, "a", "b"))
	intResult = result.ToBytes()
	if string(intResult) != ":2\r\n" {
		t.Errorf("SREM expected 2 removed members, got: %s", string(intResult))
	}

	// Verify size after removal
	result = SCard(db, utils.ToCmdLine(key))
	intResult = result.ToBytes()
	if string(intResult) != ":2\r\n" {
		t.Errorf("SCARD after removal expected 2 members, got: %s", string(intResult))
	}
}

func TestSetOperations(t *testing.T) {
	db := NewStandAloneDb()

	// Create set1: {a, b, c}
	SAdd(db, utils.ToCmdLine("set1", "a", "b", "c"))
	// Create set2: {b, c, d}
	SAdd(db, utils.ToCmdLine("set2", "b", "c", "d"))
	// Create set3: {c, d, e}
	SAdd(db, utils.ToCmdLine("set3", "c", "d", "e"))

	// Test SINTER
	result := SInter(db, utils.ToCmdLine("set1", "set2"))
	if !checkSetResult(result, []string{"b", "c"}) {
		t.Error("SINTER set1 set2 failed")
	}

	result = SInter(db, utils.ToCmdLine("set1", "set2", "set3"))
	if !checkSetResult(result, []string{"c"}) {
		t.Error("SINTER set1 set2 set3 failed")
	}

	// Test SUNION
	result = SUnion(db, utils.ToCmdLine("set1", "set2"))
	if !checkSetResult(result, []string{"a", "b", "c", "d"}) {
		t.Error("SUNION set1 set2 failed")
	}

	// Test SDIFF
	result = SDiff(db, utils.ToCmdLine("set1", "set2"))
	if !checkSetResult(result, []string{"a"}) {
		t.Error("SDIFF set1 set2 failed")
	}
}

func TestConcurrentSetOperations(t *testing.T) {
	db := NewStandAloneDb()
	key := "concurrent_set"
	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	// Concurrent SADD operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				member := fmt.Sprintf("member_%d_%d", id, j)
				SAdd(db, utils.ToCmdLine(key, member))
			}
		}(i)
	}
	wg.Wait()

	// Verify total number of members
	result := SCard(db, utils.ToCmdLine(key))
	intResult := result.ToBytes()
	expectedMembers := numGoroutines * opsPerGoroutine
	if string(intResult) != fmt.Sprintf(":%d\r\n", expectedMembers) {
		t.Errorf("Expected %d members after concurrent SADD, got: %s", expectedMembers, string(intResult))
	}

	// Concurrent SREM and SADD operations
	wg.Add(numGoroutines * 2)
	for i := 0; i < numGoroutines; i++ {
		// Goroutine for SREM
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				member := fmt.Sprintf("member_%d_%d", id, j)
				SRem(db, utils.ToCmdLine(key, member))
			}
		}(i)

		// Goroutine for SADD
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				member := fmt.Sprintf("new_member_%d_%d", id, j)
				SAdd(db, utils.ToCmdLine(key, member))
			}
		}(i)
	}
	wg.Wait()

	// Verify final number of members
	result = SCard(db, utils.ToCmdLine(key))
	intResult = result.ToBytes()
	expectedMembers = numGoroutines * opsPerGoroutine // Only the new members should remain
	if string(intResult) != fmt.Sprintf(":%d\r\n", expectedMembers) {
		t.Errorf("Expected %d members after concurrent SREM and SADD, got: %s", expectedMembers, string(intResult))
	}
}

func TestConcurrentSetOperationsMultipleSets(t *testing.T) {
	db := NewStandAloneDb()
	numSets := 3
	numGoroutines := 100
	opsPerGoroutine := 10
	var wg sync.WaitGroup

	// Initialize sets with concurrent operations
	for setID := 0; setID < numSets; setID++ {
		setKey := fmt.Sprintf("set_%d", setID)
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int, key string) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					member := fmt.Sprintf("member_%d_%d", id, j)
					t.Logf("add %s to %s", member, key)
					SAdd(db, utils.ToCmdLine(key, member))
					// check if the member is added
					result := SIsMember(db, utils.ToCmdLine(key, member))
					if string(result.ToBytes()) != ":1\r\n" {
						t.Errorf("Expected 1 for member %s in set %s, got: %s", member, key, string(result.ToBytes()))
					}
				}
			}(i, setKey)
		}
	}
	wg.Wait()

	// Verify each set has the correct number of members
	for setID := 0; setID < numSets; setID++ {
		setKey := fmt.Sprintf("set_%d", setID)
		result := SCard(db, utils.ToCmdLine(setKey))
		intResult := result.ToBytes()
		expectedMembers := numGoroutines * opsPerGoroutine
		if string(intResult) != fmt.Sprintf(":%d\r\n", expectedMembers) {
			mems := SMembers(db, utils.ToCmdLine(setKey))
			memsStr := strings.Join(strings.Split(string(mems.ToBytes()), "\r\n"), ", ")
			t.Logf("Set %s: Members: %s", setKey, memsStr)
			t.Errorf("Set %s: Expected %d members, got: %s", setKey, expectedMembers, string(intResult))
		}
	}

	// Test concurrent set operations (SINTER, SUNION, SDIFF)
	wg.Add(numGoroutines * 3) // For SINTER, SUNION, and SDIFF
	for i := 0; i < numGoroutines; i++ {
		// Concurrent SINTER
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine/10; j++ { // Reduced operations due to complexity
				SInter(db, utils.ToCmdLine("set_0", "set_1", "set_2"))
			}
		}()

		// Concurrent SUNION
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine/10; j++ {
				SUnion(db, utils.ToCmdLine("set_0", "set_1", "set_2"))
			}
		}()

		// Concurrent SDIFF
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine/10; j++ {
				SDiff(db, utils.ToCmdLine("set_0", "set_1", "set_2"))
			}
		}()
	}
	wg.Wait()
}

// Helper function to check if a set result contains expected members
func checkSetResult(result protocol.Reply, expected []string) bool {
	resultBs := result.ToBytes()
	if resultBs == nil {
		log.Println("result is nil")
		return false
	}

	// Parse RESP response
	reader := bufio.NewReader(bytes.NewReader(resultBs))
	respStr, err := utils.ParseRESP(reader)
	if err != nil {
		log.Printf("Failed to parse RESP: %v", err)
		return false
	}

	// For set results, we expect a multi-bulk reply
	if !strings.HasPrefix(respStr, "*") {
		log.Printf("Expected multi-bulk reply, got: %s", respStr)
		return false
	}

	// Split the response into lines
	lines := strings.Split(respStr, "\r\n")
	if len(lines) < 2 { // At least count line and one element
		log.Printf("Invalid response format: %s", respStr)
		return false
	}

	// Parse count (skip the '*' prefix)
	count, err := strconv.Atoi(lines[0][1:])
	if err != nil {
		log.Printf("Failed to parse count: %v", err)
		return false
	}

	if count != len(expected) {
		log.Printf("Count mismatch: expected %d, got %d", len(expected), count)
		return false
	}

	// Create a map of expected values for O(1) lookup
	expectedMap := make(map[string]bool)
	for _, e := range expected {
		expectedMap[e] = true
	}

	// Check each element in the response
	// Each bulk string has two lines: $length and value
	foundMap := make(map[string]bool)
	for i := 0; i < count; i++ {
		valueIndex := 2 + (i * 2) // Skip count line and account for length lines
		if valueIndex >= len(lines) {
			log.Printf("Response truncated at element %d", i)
			return false
		}
		value := lines[valueIndex]
		if _, exists := expectedMap[value]; !exists {
			log.Printf("Unexpected value in response: %s", value)
			return false
		}
		foundMap[value] = true
	}

	// Verify all expected values were found
	for e := range expectedMap {
		if !foundMap[e] {
			log.Printf("Expected value not found: %s", e)
			return false
		}
	}

	return true
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr
}
