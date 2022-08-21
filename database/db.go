package database

import (
	"redisgo/datastruct/dict"
	"redisgo/interface/database"
	"redisgo/interface/redis"
	"redisgo/redis/reply"
	"strings"
)

// DB stores data and execute user's commands
type DB struct {
	index int
	data  dict.Dict
	addAof func(CmdLine)
}

// CmdLine is alias for [][]byte, represents a command line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

type CmdLine = [][]byte

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data: dict.MakeSyncDict(),
		addAof: func(line CmdLine){},
	}
	return db
}

func (db *DB) Exec(c redis.Connection, cmdLine CmdLine) redis.Reply {
	// SET GET PING...
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok { //命令表中没有
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])

}

// 校验参数个数
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return arity == argNum
	}
	return argNum >= -arity // 变长参数个数使用负数表示
}

/* ---- data Access ----- */

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	val, exists := db.data.Get(key)
	if !exists {
		return nil, false
	}
	entity, _ := val.(*database.DataEntity)
	return entity, true
}

// PutEntity puts a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

// PutIfAbsent inserts an DataEntity only if the key not exists
// SETNX
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// PutIfExists edits an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// Remove removes the given key from db
func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

// Removes removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted ++
		}
	}
	return deleted
}

// Flush cleans the database
func (db *DB) Flush() {
	db.data.Clear()
}