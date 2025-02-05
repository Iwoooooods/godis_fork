package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestStringCommands(t *testing.T) {
	// Start server
	addr := ":8080"
	go Serve(addr, NewRedisHandler())
	time.Sleep(time.Second) // Wait for server to start

	// Connect to server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tests := []struct {
		name     string
		command  string
		expected string
	}{
		{
			name:     "simple ping",
			command:  "PING\r\n",
			expected: "+PONG",
		},
		{
			name:     "ping with argument",
			command:  "PING hello\r\n",
			expected: "+hello",
		},
		{
			name:     "set command",
			command:  "SET name hehuaisen\r\n",
			expected: "+OK",
		},
		{
			name:     "unknown command",
			command:  "UNKNOWN\r\n",
			expected: "-ERR unknown command 'unknown'",
		},
		{
			name:     "get existing key",
			command:  "GET name\r\n",
			expected: "$9\r\nhehuaisen\r\n", // $9 because "hehuaisen" is 9 bytes long
		},
		{
			name:     "get non-existing key",
			command:  "GET nonexistingkey\r\n",
			expected: "$-1", // This is the RESP nil bulk string
		},
		{
			name:     "get without arguments",
			command:  "GET\r\n",
			expected: "-ERR wrong number of arguments for 'get' command",
		},
		{
			name:     "del without arguments",
			command:  "DEL\r\n",
			expected: "-ERR wrong number of arguments for 'del' command",
		},
		{
			name:     "del single existing key",
			command:  "DEL name\r\n",
			expected: "+OK",
		},
		{
			name:     "verify key is deleted",
			command:  "GET name\r\n",
			expected: "$-1",
		},
		{
			name:     "set multiple keys",
			command:  "SET key1 value1\r\n",
			expected: "+OK",
		},
		{
			name:     "set multiple keys 2",
			command:  "SET key2 value2\r\n",
			expected: "+OK",
		},
		{
			name:     "del multiple existing keys",
			command:  "DEL key1 key2\r\n",
			expected: ":2",
		},
		{
			name:     "del non-existing keys",
			command:  "DEL nosuchkey1 nosuchkey2\r\n",
			expected: ":0",
		},
	}

	reader := bufio.NewReader(conn)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Send command
			_, err = conn.Write([]byte(tt.command))
			if err != nil {
				t.Fatal(err)
			}

			// Read response
			response := ""
			line, _, err := reader.ReadLine()
			if err != nil {
				t.Fatal(err)
			}
			response = string(line)

			// For bulk string responses ($), we need to read the data line too
			if len(response) > 0 && response[0] == '$' && response != "$-1" {
				// Read the actual data line
				data, _, err := reader.ReadLine()
				if err != nil {
					t.Fatal(err)
				}
				response = response + "\r\n" + string(data) + "\r\n"
			}

			if response != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, response)
			}
		})
	}
}

func TestListCommands(t *testing.T) {
	// Start server
	addr := ":8080"
	go Serve(addr, NewRedisHandler())
	time.Sleep(time.Second) // Wait for server to start

	// Connect to server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tests := []struct {
		name     string
		command  string
		expected string
	}{
		{
			name:     "lpush empty list",
			command:  "LPUSH mylist value1\r\n",
			expected: ":1",
		},
		{
			name:     "lpush multiple values",
			command:  "LPUSH mylist value2 value3\r\n",
			expected: ":3",
		},
		{
			name:     "rpush values",
			command:  "RPUSH mylist value4 value5\r\n",
			expected: ":5",
		},
		{
			name:     "llen command",
			command:  "LLEN mylist\r\n",
			expected: ":5",
		},
		{
			name:     "lindex first element",
			command:  "LINDEX mylist 0\r\n",
			expected: "$6\r\nvalue3\r\n",
		},
		{
			name:     "lindex last element",
			command:  "LINDEX mylist -1\r\n",
			expected: "$6\r\nvalue5\r\n",
		},
		{
			name:     "lindex out of range",
			command:  "LINDEX mylist 10\r\n",
			expected: "$-1",
		},
		{
			name:     "lpop command",
			command:  "LPOP mylist\r\n",
			expected: "$6\r\nvalue3\r\n",
		},
		{
			name:     "rpop command",
			command:  "RPOP mylist\r\n",
			expected: "$6\r\nvalue5\r\n",
		},
		{
			name:     "verify length after pops",
			command:  "LLEN mylist\r\n",
			expected: ":3",
		},
		{
			name:     "lpop empty list",
			command:  "LPOP emptylist\r\n",
			expected: "$-1",
		},
		{
			name:     "rpop empty list",
			command:  "RPOP emptylist\r\n",
			expected: "$-1",
		},
		{
			name:     "llen empty list",
			command:  "LLEN emptylist\r\n",
			expected: ":0",
		},
		{
			name:     "lindex invalid type",
			command:  "SET wrongtype string\r\n",
			expected: "+OK",
		},
		{
			name:     "lindex wrong type error",
			command:  "LINDEX wrongtype 0\r\n",
			expected: "-WRONGTYPE Operation against a key holding the wrong kind of value",
		},
	}

	reader := bufio.NewReader(conn)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Send command
			_, err = conn.Write([]byte(tt.command))
			if err != nil {
				t.Fatal(err)
			}

			// Read response
			response := ""
			line, _, err := reader.ReadLine()
			if err != nil {
				t.Fatal(err)
			}
			response = string(line)

			// For bulk string responses ($), we need to read the data line too
			if len(response) > 0 && response[0] == '$' && response != "$-1" {
				// Read the actual data line
				data, _, err := reader.ReadLine()
				if err != nil {
					t.Fatal(err)
				}
				response = response + "\r\n" + string(data) + "\r\n"
			}

			if response != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, response)
			}
		})
	}
}

func TestListRangeCommands(t *testing.T) {
	// Start server
	addr := ":8080"
	go Serve(addr, NewRedisHandler())
	time.Sleep(time.Second) // Wait for server to start

	// Connect to server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	tests := []struct {
		name     string
		command  string
		expected string
	}{
		// Setup initial list
		{
			name:     "lpush for test",
			command:  "LPUSH testlist one\r\n",
			expected: ":1",
		},
		{
			name:     "lpush more items",
			command:  "LPUSH testlist two three four\r\n",
			expected: ":4",
		},
		// Test LRANGE with different ranges
		{
			name:     "lrange full list",
			command:  "LRANGE testlist 0 -1\r\n",
			expected: "*4\r\n$4\r\nfour\r\n$5\r\nthree\r\n$3\r\ntwo\r\n$3\r\none\r\n",
		},
		{
			name:     "lrange first two elements",
			command:  "LRANGE testlist 0 1\r\n",
			expected: "*2\r\n$4\r\nfour\r\n$5\r\nthree\r\n",
		},
		{
			name:     "lrange last two elements",
			command:  "LRANGE testlist -2 -1\r\n",
			expected: "*2\r\n$3\r\ntwo\r\n$3\r\none\r\n",
		},
		{
			name:     "lrange middle elements",
			command:  "LRANGE testlist 1 2\r\n",
			expected: "*2\r\n$5\r\nthree\r\n$3\r\ntwo\r\n",
		},
		// Test edge cases
		{
			name:     "lrange out of range (start > stop)",
			command:  "LRANGE testlist 2 1\r\n",
			expected: "*0\r\n",
		},
		{
			name:     "lrange out of range (negative start)",
			command:  "LRANGE testlist -5 -4\r\n",
			expected: "*0\r\n",
		},
		{
			name:     "lrange out of range (too large indices)",
			command:  "LRANGE testlist 5 10\r\n",
			expected: "*0\r\n",
		},
		// Test on empty list
		{
			name:     "lrange on empty list",
			command:  "LRANGE emptylist 0 -1\r\n",
			expected: "*0\r\n",
		},
		// Test on wrong type
		{
			name:     "set string for type checking",
			command:  "SET wrongtype string\r\n",
			expected: "+OK",
		},
		{
			name:     "lrange on wrong type",
			command:  "LRANGE wrongtype 0 -1\r\n",
			expected: "-WRONGTYPE Operation against a key holding the wrong kind of value",
		},
		// Test invalid arguments
		{
			name:     "lrange with invalid start index",
			command:  "LRANGE testlist invalid 1\r\n",
			expected: "-ERR value is not an integer or out of range",
		},
		{
			name:     "lrange with invalid stop index",
			command:  "LRANGE testlist 0 invalid\r\n",
			expected: "-ERR value is not an integer or out of range",
		},
		{
			name:     "lrange with too few arguments",
			command:  "LRANGE testlist 0\r\n",
			expected: "-ERR wrong number of arguments for 'lrange' command",
		},
		{
			name:     "lrange with too many arguments",
			command:  "LRANGE testlist 0 1 2\r\n",
			expected: "-ERR wrong number of arguments for 'lrange' command",
		},
		{
			name:     "lrange full list",
			command:  "LRANGE mylist 0 -1\r\n",
			expected: "*3\r\n$6\r\nvalue2\r\n$6\r\nvalue4\r\n$6\r\nvalue1\r\n",
		},
		{
			name:     "lrange partial list",
			command:  "LRANGE mylist 0 1\r\n",
			expected: "*2\r\n$6\r\nvalue2\r\n$6\r\nvalue4\r\n",
		},
		{
			name:     "lrange negative indices",
			command:  "LRANGE mylist -2 -1\r\n",
			expected: "*2\r\n$6\r\nvalue4\r\n$6\r\nvalue1\r\n",
		},
		{
			name:     "lrange out of range",
			command:  "LRANGE mylist 10 20\r\n",
			expected: "*0\r\n",
		},
		{
			name:     "lrange empty list",
			command:  "LRANGE emptylist 0 -1\r\n",
			expected: "*0\r\n",
		},
		{
			name:     "lrange wrong type",
			command:  "LRANGE wrongtype 0 -1\r\n",
			expected: "-WRONGTYPE Operation against a key holding the wrong kind of value",
		},
	}

	reader := bufio.NewReader(conn)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Send command
			_, err = conn.Write([]byte(tt.command))
			if err != nil {
				t.Fatal(err)
			}

			// Read response
			response := ""
			line, _, err := reader.ReadLine()
			if err != nil {
				t.Fatal(err)
			}
			response = string(line)

			// For multi-bulk replies (*), we need to read additional lines
			if len(response) > 0 && response[0] == '*' {
				count, _ := strconv.Atoi(response[1:])
				for i := 0; i < count*2; i++ { // *2 because each element has a length line and a value line
					line, _, err := reader.ReadLine()
					if err != nil {
						t.Fatal(err)
					}
					response += "\r\n" + string(line)
				}
			}

			if response != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, response)
			}
		})
	}
}

func TestConcurrentListOperations(t *testing.T) {
	addr := ":8080"
	go Serve(addr, NewRedisHandler())
	time.Sleep(time.Second)

	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 10

	// Create connection pool
	conns := make([]net.Conn, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conns[i] = conn
	}

	// Run concurrent operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(connIndex int) {
			defer wg.Done()
			conn := conns[connIndex]
			reader := bufio.NewReader(conn)

			for j := 0; j < numOperations; j++ {
				// Send command
				command := fmt.Sprintf("LPUSH mylist value%d\r\n", j)
				_, err := conn.Write([]byte(command))
				if err != nil {
					t.Errorf("Failed to send command: %v", err)
					return
				}

				// Read response
				_, _, err = reader.ReadLine()
				if err != nil {
					t.Errorf("Failed to read response: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Check list length
	reader := bufio.NewReader(conn)
	_, err = conn.Write([]byte("LLEN mylist\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	response, _, err := reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}

	expectedLen := numGoroutines * numOperations
	if string(response) != fmt.Sprintf(":%d", expectedLen) {
		t.Errorf("Expected length %d, got %s", expectedLen, string(response))
	}
}
