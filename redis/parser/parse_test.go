package parser

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestParseRESPMessages(t *testing.T) {
	// Start a connection to the server
	conn, err := net.Dial("tcp", ":8080")
	if err != nil {
		t.Fatalf("could not connect to server: %v", err)
	}
	defer conn.Close()

	tests := []struct {
		name     string
		input    string
		expected string // You can modify this based on your expected response type
	}{
		{
			name:     "simple string",
			input:    "+OK\r\n",
			expected: "+OK",
		},
		{
			name:     "bulk string",
			input:    "$5\r\nhello\r\n",
			expected: "hello",
		},
		{
			name:     "array",
			input:    "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
			expected: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write the test input
			_, err := conn.Write([]byte(tt.input))
			if err != nil {
				t.Fatalf("failed to write to connection: %v", err)
			}

			// Read the response
			buf := make([]byte, 1024)
			conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatalf("failed to read from connection: %v", err)
			}

			response := string(bytes.TrimSpace(buf[:n]))
			if response != tt.expected {
				t.Errorf("got %q, want %q", response, tt.expected)
			}
		})
	}
}

// Helper function to test the parser directly without network
func TestParseDirectly(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple string",
			input:    "+OK\r\n",
			expected: "+OK",
		},
		{
			name:     "error message",
			input:    "-Error message\r\n",
			expected: "Error message",
		},
		{
			name:     "integer",
			input:    ":1000\r\n",
			expected: "1000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bytes.NewBufferString(tt.input)
			ch := ParseStream(reader)

			payload := <-ch
			if payload.Err != nil {
				t.Fatalf("unexpected error: %v", payload.Err)
			}

			result := string(payload.Data.ToBytes())
			if !bytes.Contains([]byte(result), []byte(tt.expected)) {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}
