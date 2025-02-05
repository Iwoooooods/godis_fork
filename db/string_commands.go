package db

import (
	"godis/interfaces"
	"godis/redis/protocol"
	"log"
)

func Set(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'set' command")
	}

	key := string(args[0])
	value := args[1]

	redis, _ := db.(*Redis)
	redis.data.Put(key, &DataEntity{
		Type:  TypeString,
		Value: value,
	})

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

	dataEntity, ok := val.(*DataEntity)
	if !ok || dataEntity.Type != TypeString {
		return protocol.MakeErrReply("ERR Operation against a key holding the wrong kind of value")
	}

	valBytes := dataEntity.Value.([]byte)

	return protocol.MakeBulkReply(valBytes)
}
