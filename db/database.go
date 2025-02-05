package db

import (
	"godis/ds"
	"godis/interfaces"
	"godis/redis/protocol"
	"log"
	"strings"
)

type Redis struct {
	data *ds.ShardedMap
}

func NewStandAloneDb() *Redis {
	return &Redis{
		data: ds.NewShardedMap(16),
	}
}

func (r *Redis) Close() {
	// Clean up
}

func (r *Redis) Exec(conn interfaces.Connection, cmdL [][]byte) protocol.Reply {
	if len(cmdL) == 0 {
		return protocol.MakeErrReply("ERR empty command")
	}

	// commands are case-insensitive
	cmdName := strings.ToLower(string(cmdL[0]))

	cmd, ok := CommandMap[cmdName]
	if !ok {
		log.Printf("ERR unknown command '%s'", cmdName)
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	// Execute command
	return cmd.executor(r, cmdL[1:])
}
