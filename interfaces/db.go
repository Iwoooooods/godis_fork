package interfaces

import (
	"godis/redis/protocol"
)

type Connection interface {
}

type DB interface {
	Close()
	// Exec() of a DB implementation should be called in:
	// a implementation of a ExecF() of a command
	Exec(conn Connection, cmdL [][]byte) protocol.Reply
}
