package server

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestRedisCommands(t *testing.T) {
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
