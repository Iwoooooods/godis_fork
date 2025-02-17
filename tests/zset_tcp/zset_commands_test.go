package zsettcp

import (
	"bufio"
	"godis/lib/utils"
	"godis/tcp/server"
	"log"
	"net"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.LUTC)
}

func TestZSetRem(t *testing.T) {
	// Start server
	addr := ":8080"
	go server.Serve(addr, server.NewRedisHandler())
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
			name:     "insert one into key myzset",
			command:  "ZADD myzset 1 one\r\n",
			expected: ":1\r\n",
		},
		{
			name:     "insert two and three into key myzset",
			command:  "ZADD myzset 2 two 3 three\r\n",
			expected: ":2\r\n",
		},
		{
			name:     "remove one from key myzset",
			command:  "ZREM myzset one\r\n",
			expected: ":1\r\n",
		},
		{
			name:     "get nonexistent member from key myzset",
			command:  "ZSCORE myzset one\r\n",
			expected: "$-1\r\n",
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
			actual, err := utils.ParseRESP(reader)
			if err != nil {
				t.Log("Error when parsing the response", err.Error())
			}

			if actual != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, actual)
			}
		})
	}
}

func TestZSetRange(t *testing.T) {
	// Start server
	addr := ":8081"
	go server.Serve(addr, server.NewRedisHandler())
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
			name:     "insert one into key myzset",
			command:  "ZADD myzset 1 one\r\n",
			expected: ":1\r\n",
		},
		{
			name:     "insert two and three into key myzset",
			command:  "ZADD myzset 2 two 3 three\r\n",
			expected: ":2\r\n",
		},
		{
			name:     "zrange with exclude score",
			command:  "ZRANGE myzset (1 3 BYSCORE\r\n",
			expected: "*4\r\n$3\r\ntwo\r\n$1\r\n2\r\n$5\r\nthree\r\n$1\r\n3\r\n",
		},
		{
			name:     "zrange all elements by rank",
			command:  "ZRANGE myzset 0 -1\r\n",
			expected: "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n",
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
			actual, err := utils.ParseRESP(reader)
			if err != nil {
				t.Log("Error when parsing the response", err.Error())
			}

			if actual != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, actual)
			}
		})
	}
}
