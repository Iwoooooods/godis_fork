package db

import (
	"godis/ds/list"
	"godis/interfaces"
	"godis/redis/protocol"
	"strconv"
)

// getAsList returns the list in key, or creates a new one if it doesn't exist
// Returns (list, nil) if the operation is successful
// Returns (nil, error) if:
// - key exists but is not a list
func getAsList(db *Redis, key string) (list.List, *protocol.StandardErrReply) {
	entity, exists := db.data.Get(key)
	if !exists {
		l := list.NewConcurrentList()
		db.data.Put(key, &DataEntity{
			Type:  TypeList,
			Value: l,
		})
		return l, nil
	}

	dataEntity, ok := entity.(*DataEntity)
	if !ok {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if dataEntity.Type != TypeList {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return dataEntity.Value.(list.List), nil
}

// LPush adds one or more elements to the head of the list
func LPush(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lpush' command")
	}

	key := string(args[0])
	values := args[1:]

	redis, _ := db.(*Redis)
	list, errReply := getAsList(redis, key)
	if errReply != nil {
		return errReply
	}

	// Insert values from left to right at the head
	for i := 0; i < len(values); i++ {
		list.InsertAt(0, values[i])
	}
	return protocol.MakeIntReply(int64(list.Len()))
}

// RPush adds one or more elements to the tail of the list
func RPush(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rpush' command")
	}

	key := string(args[0])
	values := args[1:]

	redis, _ := db.(*Redis)
	list, errReply := getAsList(redis, key)
	if errReply != nil {
		return errReply
	}

	// Insert values at the tail
	for _, value := range values {
		list.InsertAt(list.Len(), value)
	}
	return protocol.MakeIntReply(int64(list.Len()))
}

// LPop removes and returns the first element of the list
func LPop(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lpop' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)

	entity, exists := redis.data.Get(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}

	dataEntity, ok := entity.(*DataEntity)
	if !ok || dataEntity.Type != TypeList {
		return protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := dataEntity.Value.(list.List)
	if list.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}

	value := list.RemoveAt(0)
	if value == nil {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(value)
}

// RPop removes and returns the last element of the list
func RPop(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rpop' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)

	entity, exists := redis.data.Get(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}

	dataEntity, ok := entity.(*DataEntity)
	if !ok || dataEntity.Type != TypeList {
		return protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := dataEntity.Value.(list.List)
	if list.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}

	value := list.RemoveAt(list.Len() - 1)
	if value == nil {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(value)
}

// LLen returns the length of the list
func LLen(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'llen' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)

	entity, exists := redis.data.Get(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	dataEntity, ok := entity.(*DataEntity)
	if !ok || dataEntity.Type != TypeList {
		return protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := dataEntity.Value.(list.List)
	return protocol.MakeIntReply(int64(list.Len()))
}

// LIndex returns the element at index in the list
func LIndex(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lindex' command")
	}

	key := string(args[0])
	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	redis, _ := db.(*Redis)
	entity, exists := redis.data.Get(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}

	dataEntity, ok := entity.(*DataEntity)
	if !ok || dataEntity.Type != TypeList {
		return protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := dataEntity.Value.(list.List)
	value := list.GetAt(int(index))
	if value == nil {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(value)
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes.
// These offsets can be negative numbers, where -1 is the last element.
func LRange(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lrange' command")
	}

	// Get key
	key := string(args[0])

	// Parse start index
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	// Parse stop index
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	redis, _ := db.(*Redis)
	entity, exists := redis.data.Get(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}

	dataEntity, ok := entity.(*DataEntity)
	if !ok || dataEntity.Type != TypeList {
		return protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := dataEntity.Value.(list.List)
	size := list.Len()

	// Convert negative indices
	if start < 0 {
		start = int64(size) + start
	}
	if stop < 0 {
		stop = int64(size) + stop
	}

	// Return empty array if start is beyond list bounds after conversion
	if start >= int64(size) || start < 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	// Clamp indices to valid range
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = 0
	}
	if stop >= int64(size) {
		stop = int64(size) - 1
	}

	// Return empty array if range is invalid
	if start > stop {
		return protocol.MakeEmptyMultiBulkReply()
	}

	// Collect elements in range
	stop++ // Make it inclusive
	rangeLen := stop - start
	result := make([][]byte, rangeLen)
	for i := 0; i < int(rangeLen); i++ {
		value := list.GetAt(int(start) + i)
		if value == nil {
			value = []byte{}
		}
		result[i] = value
	}

	return protocol.MakeMultiBulkReply(result)
}
