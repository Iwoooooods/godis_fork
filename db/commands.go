package db

import (
	"godis/interfaces"
	"godis/redis/protocol"
	"strings"
)

// CommandMap maps all commands to their functionalities
// e.g. set -> Put()
// use Register() to register a new command
var CommandMap = make(map[string]*cmd)

type cmd struct {
	name     string
	executor ExecF
}

// ExecF is the function for executing the corresponding command
type ExecF func(db interfaces.DB, args [][]byte) protocol.Reply

func Register(cmdN string, cmdF ExecF) *cmd {
	name := strings.ToLower(cmdN)
	cmd := &cmd{
		name:     name,
		executor: cmdF,
	}
	CommandMap[name] = cmd
	return cmd
}

func init() {
	Register("PING", Ping)
	Register("SET", Set)
	// Register other commands...
}

func Ping(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

func Set(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'set' command")
	}

	key := string(args[0])
	value := args[1]

	redis, _ := db.(*Redis)
	redis.data.Put(key, value)

	return protocol.MakeOkReply()
}
