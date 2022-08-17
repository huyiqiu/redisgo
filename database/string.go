package database

import (
	"redisgo/interface/database"
	"redisgo/interface/redis"
	"redisgo/lib/utils"
	"redisgo/redis/reply"
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
	RegisterCommand("Get", execGet, 2)
	RegisterCommand("Set", execSet, 3)
	RegisterCommand("SetNX", execSetNX, 3)
	RegisterCommand("GetSet", execGetSet, 3)
	RegisterCommand("StrLen", execStrlen, 2)
}