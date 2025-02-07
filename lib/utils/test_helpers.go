package utils

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// RESP type identifiers
const (
	StringReply    = '+'
	ErrorReply     = '-'
	IntReply       = ':'
	BulkReply      = '$'
	MultiBulkReply = '*'
)

// ParseRESP reads and parses a RESP response from the reader
func ParseRESP(reader *bufio.Reader) (string, error) {
	line, _, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	if len(line) == 0 {
		return "", fmt.Errorf("empty response")
	}

	response := string(line) + "\r\n"
	switch line[0] {
	case StringReply:
		return ParseSimpleString(response)
	case ErrorReply:
		return ParseError(response)
	case IntReply:
		return ParseInt(response)
	case BulkReply:
		return ParseBulkString(reader, response)
	case MultiBulkReply:
		return ParseMultiBulk(reader, response)
	default:
		return "", fmt.Errorf("unknown RESP type: %c", line[0])
	}
}

// ParseSimpleString handles simple string responses (+...)
func ParseSimpleString(response string) (string, error) {
	if len(response) < 3 { // +\r\n minimum length
		return "", fmt.Errorf("invalid simple string response: %q", response)
	}
	return response, nil
}

// ParseError handles error responses (-...)
func ParseError(response string) (string, error) {
	if len(response) < 3 { // -\r\n minimum length
		return "", fmt.Errorf("invalid error response: %q", response)
	}
	return response, nil
}

// ParseInt handles integer responses (:...)
func ParseInt(response string) (string, error) {
	if len(response) < 3 { // :\r\n minimum length
		return "", fmt.Errorf("invalid integer response: %q", response)
	}
	// Validate that the content is actually an integer
	_, err := strconv.ParseInt(strings.TrimSuffix(response[1:], "\r\n"), 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid integer in response: %q", response)
	}
	return response, nil
}

// ParseBulkString handles bulk string responses ($...)
func ParseBulkString(reader *bufio.Reader, initial string) (string, error) {
	if initial == "$-1\r\n" { // null bulk string
		return initial, nil
	}

	// Parse length
	length, err := strconv.Atoi(strings.TrimSuffix(initial[1:], "\r\n"))
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %q", initial)
	}
	if length < 0 {
		return "", fmt.Errorf("invalid bulk string length: %d", length)
	}

	// Read the actual data
	data, _, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	if len(data) != length {
		return "", fmt.Errorf("bulk string length mismatch: expected %d, got %d", length, len(data))
	}

	return initial + string(data) + "\r\n", nil
}

// ParseMultiBulk handles array responses (*...)
func ParseMultiBulk(reader *bufio.Reader, initial string) (string, error) {
	count, err := strconv.Atoi(strings.TrimSuffix(initial[1:], "\r\n"))
	if err != nil {
		return "", fmt.Errorf("invalid multi-bulk count: %q", initial)
	}

	if count == 0 {
		return "*0\r\n", nil
	}
	if count < 0 {
		return "", fmt.Errorf("invalid multi-bulk count: %d", count)
	}

	response := initial
	// Each element in the multi-bulk reply has two lines:
	// 1. Length prefix ($...)
	// 2. Actual data
	for i := 0; i < count*2; i++ {
		line, _, err := reader.ReadLine()
		if err != nil {
			return "", err
		}
		response += string(line) + "\r\n"
	}

	return response, nil
}

// Helper function to extract the integer value from an int reply
func ExtractInt(response string) (int64, error) {
	if len(response) < 3 || response[0] != IntReply {
		return 0, fmt.Errorf("not an integer reply: %q", response)
	}
	return strconv.ParseInt(strings.TrimSuffix(response[1:], "\r\n"), 10, 64)
}

// Helper function to extract the string value from a bulk string reply
func ExtractBulkString(response string) (string, error) {
	if response == "$-1\r\n" {
		return "", nil // null bulk string
	}
	parts := strings.Split(response, "\r\n")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid bulk string response: %q", response)
	}
	return parts[1], nil
}
