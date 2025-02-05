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
			command:  "SET mykey myvalue\r\n",
			expected: "+OK",
		},
		{
			name:     "unknown command",
			command:  "UNKNOWN\r\n",
			expected: "-ERR unknown command 'unknown'",
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
			line, _, err := reader.ReadLine()
			if err != nil {
				t.Fatal(err)
			}

			if string(line) != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, string(line))
			}
		})
	}
}
