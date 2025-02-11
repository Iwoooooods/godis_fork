package db

import (
	"godis/ds/set"
	"godis/interfaces"
	"godis/redis/protocol"
)

func getAsSet(db *Redis, key string) (*set.ConcurrentSet, *protocol.StandardErrReply) {
	entity, exists := db.data.Get(key)
	if !exists {
		newSet := set.NewSet()
		db.data.Put(key, &DataEntity{
			Type:  TypeSet,
			Value: newSet,
		})
		return newSet, nil
	}

	dataEntity, ok := entity.(*DataEntity)
	if !ok || dataEntity.Type != TypeSet {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return dataEntity.Value.(*set.ConcurrentSet), nil
}

func SAdd(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sadd' command")
	}

	key := string(args[0])
	members := args[1:]
	redis, _ := db.(*Redis)
	s, errReply := getAsSet(redis, key)
	if errReply != nil {
		return errReply
	}

	strMembers := make([]string, len(members))
	for i, m := range members {
		strMembers[i] = string(m)
	}
	added := s.Add(strMembers...)

	return protocol.MakeIntReply(int64(added))
}

func SRem(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'srem' command")
	}

	key := string(args[0])
	members := args[1:]
	redis, _ := db.(*Redis)
	s, errReply := getAsSet(redis, key)
	if errReply != nil {
		return errReply
	}

	strMembers := make([]string, len(members))
	for i, m := range members {
		strMembers[i] = string(m)
	}
	removed := s.Remove(strMembers...)
	return protocol.MakeIntReply(int64(removed))
}

func SIsMember(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sismember' command")
	}

	key := string(args[0])
	member := string(args[1])
	redis, _ := db.(*Redis)
	s, errReply := getAsSet(redis, key)
	if errReply != nil {
		return errReply
	}

	exists := s.Contains(member)
	if exists {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

func SMembers(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'smembers' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)
	s, errReply := getAsSet(redis, key)
	if errReply != nil {
		return errReply
	}

	members := s.Members()
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m)
	}
	return protocol.MakeMultiBulkReply(result)
}

func SCard(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'scard' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)
	s, errReply := getAsSet(redis, key)
	if errReply != nil {
		return errReply
	}

	cardinality := s.Cardinality()
	return protocol.MakeIntReply(int64(cardinality))
}

func SInter(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sinter' command")
	}

	redis, _ := db.(*Redis)
	sets := make([]*set.ConcurrentSet, 0, len(args))

	for _, arg := range args {
		key := string(arg)
		s, errReply := getAsSet(redis, key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, s)
	}

	if len(sets) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	result := sets[0]
	for _, s := range sets[1:] {
		result = result.Intersect(s)
	}

	members := result.Members()
	reply := make([][]byte, len(members))
	for i, m := range members {
		reply[i] = []byte(m)
	}
	return protocol.MakeMultiBulkReply(reply)
}

func SUnion(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sunion' command")
	}

	redis, _ := db.(*Redis)
	sets := make([]*set.ConcurrentSet, 0, len(args))

	for _, arg := range args {
		key := string(arg)
		s, errReply := getAsSet(redis, key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, s)
	}

	if len(sets) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	result := sets[0]
	for _, s := range sets[1:] {
		result = result.Union(s)
	}

	members := result.Members()
	reply := make([][]byte, len(members))
	for i, m := range members {
		reply[i] = []byte(m)
	}
	return protocol.MakeMultiBulkReply(reply)
}

func SDiff(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sdiff' command")
	}

	redis, _ := db.(*Redis)
	sets := make([]*set.ConcurrentSet, 0, len(args))

	for _, arg := range args {
		key := string(arg)
		s, errReply := getAsSet(redis, key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, s)
	}

	if len(sets) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	result := sets[0]
	for _, s := range sets[1:] {
		result = result.Diff(s)
	}

	members := result.Members()
	reply := make([][]byte, len(members))
	for i, m := range members {
		reply[i] = []byte(m)
	}
	return protocol.MakeMultiBulkReply(reply)
}
