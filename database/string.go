package database

import (
	"redisgo/interface/database"
	"redisgo/interface/redis"
	"redisgo/lib/utils"
	"redisgo/redis/reply"
	"strconv"
)

func (db *DB) getAsString(key string) ([]byte, reply.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

// execGet returns string value bound to the given key
func execGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return &reply.NullBulkReply{}
	}
	return reply.MakeBulkReply(bytes)
}

// execSet sets string value to given key
func execSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]

	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine2("set", args...))
	return &reply.OKReply{}
}

// execMSet sets multi key-value in database
func execMSet(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return reply.MakeSyntaxErrReply()
	}

	size := len(args) / 2
	keys := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[i*2])
		values[i] = args[i*2+1]
	}

	for i, key := range keys {
		value := values[i]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}

	db.addAof(utils.ToCmdLine2("mset", args...))
	return &reply.OKReply{}
}

// execMGet get multi key-value from database
func execMGet(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	result := make([][]byte, len(args))
	for i, key := range keys {
		bytes, err := db.getAsString(key)
		if err != nil {
			_, isWrongType := err.(*reply.WrongTypeErrReply)
			if isWrongType {
				result[i] = nil
				continue
			} else {
				return err
			}
		}
		result[i] = bytes
	}
	return reply.MakeMultiBulkReply(result)
}

// execIncr increments the integer value of a key by one
func execIncr(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes != nil { // 存在则+1
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+1, 10)),
		})
		db.addAof(utils.ToCmdLine2("incr", args...))
		return reply.MakeIntReply(val + 1)
	}
	// 不存在就新增
	db.PutEntity(key, &database.DataEntity{
		Data: []byte("1"),
	})
	db.addAof(utils.ToCmdLine2("incr", args...))
	return reply.MakeIntReply(1)
}

// execIncrBy increments the integer value of a key by given value
func execIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	delta, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val+delta, 10)),
		})
		db.addAof(utils.ToCmdLine2("incrby", args...))
		return reply.MakeIntReply(val + delta)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: args[1],
	})
	db.addAof(utils.ToCmdLine2("incrby", args...))
	return reply.MakeIntReply(delta)
}

// execDecr decrements the integer value of a key by one
func execDecr(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-1, 10)),
		})
		db.addAof(utils.ToCmdLine2("decr", args...))
		return reply.MakeIntReply(val - 1)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: []byte("-1"),
	})
	db.addAof(utils.ToCmdLine2("decr", args...))
	return reply.MakeIntReply(-1)
}

// execDecrBy decrements the integer value of a key by onedecrement
func execDecrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	delta, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range")
	}
	bytes, errReply := db.getAsString(key)
	if errReply != nil {
		return errReply
	}
	if bytes != nil {
		val, err := strconv.ParseInt(string(bytes), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range")
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(val-delta, 10)),
		})
		db.addAof(utils.ToCmdLine2("decrby", args...))
		return reply.MakeIntReply(val - delta)
	}
	db.PutEntity(key, &database.DataEntity{
		Data: []byte(strconv.FormatInt(-delta, 10)),
	})
	db.addAof(utils.ToCmdLine2("decrby", args...))
	return reply.MakeIntReply(-delta)
}

// execSetNX sets string if not exists
func execSetNX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	entity := &database.DataEntity{Data: args[1]}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine2("setnx", args...))
	return reply.MakeIntReply(int64(result))
}

// execGetSet sets value of a string-type key and returns its old value
func execGetSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]

	entity, exists := db.GetEntity(key)
	db.PutEntity(key, &database.DataEntity{Data: value})
	if !exists {
		return reply.MakeNullBulkReply()
	}
	old := entity.Data.([]byte)
	db.addAof(utils.ToCmdLine2("getset", args...))
	return reply.MakeBulkReply(old)
}

// execStrlen returns len of string value bound to the given key
func execStrlen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	entity, exist := db.GetEntity(key)
	if !exist {
		return reply.MakeNullBulkReply()
	}
	length := int64(len(entity.Data.([]byte)))
	return reply.MakeIntReply(length)
}

func init() {
	RegisterCommand("get", execGet, 2)
	RegisterCommand("set", execSet, 3)
	RegisterCommand("mget", execMGet, -2)
	RegisterCommand("mset", execMSet, -3)
	RegisterCommand("SetNX", execSetNX, 3)
	RegisterCommand("GetSet", execGetSet, 3)
	RegisterCommand("StrLen", execStrlen, 2)
	RegisterCommand("incr", execIncr, 2)
	RegisterCommand("incrby", execIncrBy, 3)
	RegisterCommand("decr", execDecr, 2)
	RegisterCommand("decrby", execDecrBy, 3)
}
