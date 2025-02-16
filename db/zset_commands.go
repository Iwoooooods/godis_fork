package db

import (
	"godis/ds/zset"
	"godis/interfaces"
	"godis/redis/protocol"
	"log"
	"strconv"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
}

func getAsZSet(db *Redis, key string) (*zset.SortedSet, *protocol.StandardErrReply) {
	entity, exists := db.data.Get(key)
	if !exists {
		newZSet := zset.NewSortedSet()
		db.data.Put(key, &DataEntity{
			Type:  TypeZset,
			Value: newZSet,
		})
		return newZSet, nil
	}

	dataEntity, ok := entity.(*DataEntity)
	if !ok || dataEntity.Type != TypeZset {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return dataEntity.Value.(*zset.SortedSet), nil
}

// ZAdd adds the specified members with scores to the sorted set stored at key.
// It returns the number of elements added to the sorted sets, not including elements already present for which the score was updated.
func ZAdd(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 3 || len(args)%2 == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zadd' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)
	zSet, errReply := getAsZSet(redis, key)
	if errReply != nil {
		return errReply
	}

	var added int64 = 0
	for i := 1; i < len(args)-1; i += 2 {
		scoreStr := string(args[i])
		member := string(args[i+1])
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		if zSet.Add(member, score) {
			added++
		}
	}
	return protocol.MakeIntReply(added)
}

func ZRemove(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrem' command")
	}

	count := 0
	key := string(args[0])
	redis, _ := db.(*Redis)
	zSet, errReply := getAsZSet(redis, key)
	if errReply != nil {
		return errReply
	}

	for _, member := range args[1:] {
		if zSet.Remove(string(member)) {
			count++
		}
	}

	return protocol.MakeIntReply(int64(count))
}

// ZRange returns a range of members in the sorted set stored at key, by index or score
// if with score, should add a BYSCORE flag
// ZRange is 0-based
func ZRange(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrange' command")
	}

	key := string(args[0])
	var start zset.Border
	var stop zset.Border
	byScore := false
	var err error

	if len(args) == 4 {
		if string(args[3]) != "BYSCORE" {
			return protocol.MakeErrReply("ERR Unknown flag")
		}
		byScore = true
		start, err = zset.ParseFloatBorder(args[1])
		if err != nil {
			log.Print("err: ", err.Error())
			return protocol.MakeErrReply("ERR Error score format")
		}
		stop, err = zset.ParseFloatBorder(args[2])
		if err != nil {
			log.Print("err: ", err.Error())
			return protocol.MakeErrReply("ERR Error score format")
		}
	} else {
		start, err = zset.ParseIntBorder(args[1])
		if err != nil {
			log.Print("err: ", err.Error())
			return protocol.MakeErrReply("ERR Error rank format")
		}
		stop, err = zset.ParseIntBorder(args[2])
		if err != nil {
			log.Print("err: ", err.Error())
			return protocol.MakeErrReply("ERR Error rank format")
		}
	}

	redis, _ := db.(*Redis)
	zSet, errReply := getAsZSet(redis, key)
	if errReply != nil {
		return errReply
	}

	if zSet.Len() == 0 {
		log.Printf("zset %s is empty", key)
		return protocol.MakeEmptyMultiBulkReply()
	}

	members := zSet.Range(start, stop, byScore)
	result := make([][]byte, 0, len(members))
	for _, member := range members {
		scoreStr := strconv.FormatFloat(member.Score, 'g', 10, 64) // Format float to string
		result = append(result, []byte(member.Member), []byte(scoreStr))
	}
	return protocol.MakeMultiBulkReply(result)
}

// // ZRevRange returns a range of members in the sorted set stored at key, by index, in reverse order.
// // The indexes start and stop are zero-based, with 0 being the first element, 1 being the next element and so on.
// // They are inclusive ranges, so both start and stop are included in the returned elements.
// func ZRevRange(db interfaces.DB, args [][]byte) protocol.Reply {
// 	if len(args) != 3 {
// 		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrevrange' command")
// 	}

// 	key := string(args[0])
// 	startStr := string(args[1])
// 	stopStr := string(args[2])

// 	start, err := strconv.ParseInt(startStr, 10, 64)
// 	if err != nil {
// 		return protocol.MakeErrReply("ERR value is not an integer or out of range")
// 	}
// 	stop, err := strconv.ParseInt(stopStr, 10, 64)
// 	if err != nil {
// 		return protocol.MakeErrReply("ERR value is not an integer or out of range")
// 	}

// 	redis, _ := db.(*Redis)
// 	zSet, errReply := getAsZSet(redis, key)
// 	if errReply != nil {
// 		return errReply
// 	}

// 	if zSet.Len() == 0 {
// 		return protocol.MakeEmptyMultiBulkReply()
// 	}

// 	members := zSet.RevRange(start, stop)
// 	result := make([][]byte, 0, len(members))
// 	for _, member := range members {
// 		scoreStr := strconv.FormatFloat(member.Score, 'g', 10, 64)
// 		result = append(result, []byte(member.Member), []byte(scoreStr))
// 	}
// 	return protocol.MakeMultiBulkReply(result)
// }

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func ZCard(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zcard' command")
	}

	key := string(args[0])
	redis, _ := db.(*Redis)
	zSet, errReply := getAsZSet(redis, key)
	if errReply != nil {
		return errReply
	}

	return protocol.MakeIntReply(int64(zSet.Len()))
}

// ZScore returns the score of member in the sorted set at key.
// If member does not exist in the sorted set, or key does not exist, nil is returned.
func ZScore(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zscore' command")
	}

	key := string(args[0])
	member := string(args[1])

	redis, _ := db.(*Redis)
	zSet, errReply := getAsZSet(redis, key)
	if errReply != nil {
		return errReply
	}

	score, ok := zSet.Score(member)
	if !ok {
		log.Printf("member %s not found in zset %s", member, key)
		return protocol.MakeNullBulkReply()
	}
	scoreStr := strconv.FormatFloat(score, 'g', 10, 64)
	return protocol.MakeBulkReply([]byte(scoreStr))
}

// ZRank returns the rank of member in the sorted set stored at key, with scores ordered from low to high.
// Rank is 0-based.
func ZRank(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrank' command")
	}

	key := string(args[0])
	member := string(args[1])

	redis, _ := db.(*Redis)
	zSet, errReply := getAsZSet(redis, key)
	if errReply != nil {
		return errReply
	}

	_, ok := zSet.Score(member)
	if !ok {
		return protocol.MakeEmptyMultiBulkReply()
	}
	rank := zSet.GetRank(member, false)
	if rank == 0 {
		return protocol.MakeNullBulkReply() // Member not found
	}
	return protocol.MakeIntReply(rank)
}

// ZRevRank returns the rank of member in the sorted set stored at key, with scores ordered from high to low.
// Rank is 0-based.
func ZRevRank(db interfaces.DB, args [][]byte) protocol.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrevrank' command")
	}

	key := string(args[0])
	member := string(args[1])

	redis, _ := db.(*Redis)
	zSet, errReply := getAsZSet(redis, key)
	if errReply != nil {
		return errReply
	}

	_, ok := zSet.Score(member)
	if !ok {
		return protocol.MakeEmptyMultiBulkReply()
	}
	rank := zSet.GetRank(member, false)
	revRank := zSet.Len() - rank - 1
	if revRank == 0 {
		return protocol.MakeNullBulkReply() // Member not found
	}
	return protocol.MakeIntReply(revRank)
}
