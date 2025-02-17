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
	TypeZset
)

type DataEntity struct {
	Type  int
	Value any
}

type cmd struct {
	name     string
	executor Exec
}

// Exec is the function for executing the corresponding command
type Exec func(db interfaces.DB, args [][]byte) protocol.Reply

func Register(cmdN string, cmdF Exec, ifPersist bool) *cmd {
	name := strings.ToLower(cmdN)

	executePersist := func(db interfaces.DB, args [][]byte) protocol.Reply {
		reply := cmdF(db, args)
		// if the server creahed during the execution
		// the command then will not be appended to the file
		if ifPersist {
			// TODO: persist AOF
		}
		return reply
	}

	cmd := &cmd{
		name:     name,
		executor: executePersist,
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
	Register("PING", Ping, true)
	Register("DEL", Del, true)

	// string commands
	Register("SET", Set, true)
	Register("GET", Get, true)

	// list commands
	Register("LPUSH", LPush, true)
	Register("RPUSH", RPush, true)
	Register("LPOP", LPop, true)
	Register("RPOP", RPop, true)
	Register("LLEN", LLen, true)
	Register("LINDEX", LIndex, true)
	Register("LRANGE", LRange, true)

	// hash commands
	Register("HSET", HSet, true)
	Register("HGET", HGet, true)
	Register("HDEL", HDel, true)
	Register("HGETALL", HGetAll, true)
	Register("HEXISTS", HExists, true)
	Register("HLEN", HLen, true)

	// set commands
	Register("SADD", SAdd, true)
	Register("SREM", SRem, true)
	Register("SISMEMBER", SIsMember, true)
	Register("SMEMBERS", SMembers, true)
	Register("SCARD", SCard, true)
	Register("SINTER", SInter, true)
	Register("SUNION", SUnion, true)
	Register("SDIFF", SDiff, true)

	Register("ZADD", ZAdd, true)
	Register("ZREM", ZRemove, true)
	Register("ZRANGE", ZRange, true)
	Register("ZCARD", ZCard, true)
	Register("ZSCORE", ZScore, true)
	Register("ZRANK", ZRank, true)
}
