package db

import (
	"godis/interfaces"
	"godis/redis/protocol"
	"log"
	"strings"
)

// CommandMap maps all commands to their functionalities
// e.g. set -> Put()
// use Register() to register a new command
// CommandMap is initialized when package loaded
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

func Get(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'get' command")
	}

	key := string(args[0])
	redis, ok := db.(*Redis)
	if !ok {
		return protocol.MakeErrReply("ERR incorrect db type")
	}
	val, ok := redis.data.Get(key)
	if !ok {
		log.Printf("key %s not exists", key)
		return protocol.MakeNullBulkReply()
	}
	valBytes := val.([]byte)

	return protocol.MakeBulkReply(valBytes)
}

// DEL could delete one or more keys from the db
func Del(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}

	redis, ok := db.(*Redis)
	if !ok {
		return protocol.MakeErrReply("ERR incorrect db type")
	}

	// counter records the number of successful deletion
	// return counter if multiple keys deleted
	// return Ok if single key deleted
	var counter int64

	for _, b := range args {
		key := string(b)
		log.Printf("deleting keys: %v", key)
		if !redis.data.Del(key) {
			log.Printf("failed to delete key %s", key)
			continue
		}
		counter++
	}

	if counter == 1 {
		return protocol.MakeOkReply()
	} else {
		return protocol.MakeIntReply(counter)
	}

}

// init() will be called before main() after the package is loaded
func init() {
	Register("PING", Ping)
	Register("SET", Set)
	Register("GET", Get)
	Register("DEL", Del)
}
