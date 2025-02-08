package db

import (
	"godis/interfaces"
	"godis/redis/protocol"
	"testing"
)

func TestRegisterCmds(t *testing.T) {
	executor := func(db interfaces.DB, args [][]byte) protocol.Reply {
		return nil
	}
	Register("GET", executor)
	if CommandMap["get"].name != "get" {
		t.Error("command name not match")
	}
}
