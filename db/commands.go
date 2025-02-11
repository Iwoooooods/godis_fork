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

const (
	TypeString = iota
	TypeList
	TypeHash
	TypeSet
)

type DataEntity struct {
	Type  int
	Value any
}

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
	Register("DEL", Del)

	// string commands
	Register("SET", Set)
	Register("GET", Get)

	// list commands
	Register("LPUSH", LPush)
	Register("RPUSH", RPush)
	Register("LPOP", LPop)
	Register("RPOP", RPop)
	Register("LLEN", LLen)
	Register("LINDEX", LIndex)
	Register("LRANGE", LRange)

	// hash commands
	Register("HSET", HSet)
	Register("HGET", HGet)
	Register("HDEL", HDel)
	Register("HGETALL", HGetAll)
	Register("HEXISTS", HExists)
	Register("HLEN", HLen)

	// set commands
	Register("SADD", SAdd)
	Register("SREM", SRem)
	Register("SISMEMBER", SIsMember)
	Register("SMEMBERS", SMembers)
	Register("SCARD", SCard)
	Register("SINTER", SInter)
	Register("SUNION", SUnion)
	Register("SDIFF", SDiff)
}
