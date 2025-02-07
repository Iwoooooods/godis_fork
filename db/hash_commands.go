package db

import (
	"godis/interfaces"
	"godis/redis/protocol"
	"sync"
)

// ConcurrentHash is a thread-safe hash structure
type ConcurrentHash struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewConcurrentHash() *ConcurrentHash {
	return &ConcurrentHash{
		data: make(map[string][]byte),
	}
}

// getAsHash returns the hash in key, or creates a new one if it doesn't exist
// Returns (hash, nil) if the operation is successful
// Returns (nil, error) if:
// - key exists but is not a hash
func getAsHash(db *Redis, key string) (*ConcurrentHash, *protocol.StandardErrReply) {
	entity, exists := db.data.Get(key)
	if !exists {
		hash := NewConcurrentHash()
		db.data.Put(key, &DataEntity{
			Type:  TypeHash,
			Value: hash,
		})
		return hash, nil
	}

	dataEntity, ok := entity.(*DataEntity)
	if !ok {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if dataEntity.Type != TypeHash {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return dataEntity.Value.(*ConcurrentHash), nil
}

// HSet sets field in the hash stored at key to value
// Returns the number of fields that were added (not updated)
func HSet(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 3 || len(args)%2 != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hset' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)
	hash, errReply := getAsHash(redis, key)
	if errReply != nil {
		return errReply
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	var added int64
	for i := 1; i < len(args); i += 2 {
		field := string(args[i])
		value := args[i+1]
		_, exists := hash.data[field]
		hash.data[field] = value
		if !exists {
			added++
		}
	}

	return protocol.MakeIntReply(added)
}

// HGet returns the value associated with field in the hash stored at key
func HGet(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hget' command")
	}

	key := string(args[0])
	field := string(args[1])

	redis, _ := db.(*Redis)
	hash, errReply := getAsHash(redis, key)
	if errReply != nil {
		return errReply
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	value, exists := hash.data[field]
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(value)
}

// HDel removes the specified fields from the hash stored at key
func HDel(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hdel' command")
	}

	key := string(args[0])
	fields := args[1:]

	redis, _ := db.(*Redis)
	hash, errReply := getAsHash(redis, key)
	if errReply != nil {
		return errReply
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	var deleted int64
	for _, field := range fields {
		if _, exists := hash.data[string(field)]; exists {
			delete(hash.data, string(field))
			deleted++
		}
	}

	return protocol.MakeIntReply(deleted)
}

// HGetAll returns all fields and values of the hash stored at key
func HGetAll(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hgetall' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)
	hash, errReply := getAsHash(redis, key)
	if errReply != nil {
		return errReply
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	result := make([][]byte, 0, len(hash.data)*2)
	for field, value := range hash.data {
		result = append(result, []byte(field), value)
	}

	return protocol.MakeMultiBulkReply(result)
}

// HExists returns if field is an existing field in the hash stored at key
func HExists(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hexists' command")
	}

	key := string(args[0])
	field := string(args[1])

	redis, _ := db.(*Redis)
	hash, errReply := getAsHash(redis, key)
	if errReply != nil {
		return errReply
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	_, exists := hash.data[field]
	if exists {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// HLen returns the number of fields in the hash stored at key
func HLen(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hlen' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)
	hash, errReply := getAsHash(redis, key)
	if errReply != nil {
		return errReply
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	return protocol.MakeIntReply(int64(len(hash.data)))
}
