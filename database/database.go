package database

import (
	"redisgo/aof"
	"redisgo/config"
	"redisgo/interface/redis"
	"redisgo/lib/logger"
	"redisgo/redis/reply"
	"strconv"
	"strings"
)

type StandaloneDatabase struct { // 核心
	dbSet      []*DB
	aofHandler *aof.AofHandler
}

func NewStandaloneDataBase() *StandaloneDatabase {
	database := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}

	database.dbSet = make([]*DB, config.Properties.Databases)
	// 初始化DB
	for i := range database.dbSet {
		db := makeDB()
		db.index = i
		database.dbSet[i] = db
	}

	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(database)
		if err != nil {
			panic(err)
		}
		database.aofHandler = aofHandler
		for _, db := range database.dbSet {
			singleDB := db // 局部变量，避免闭包
			singleDB.addAof = func(cmdline CmdLine) {
				database.aofHandler.AddAof(singleDB.index, cmdline)
			}
		}
	}
	return database
}

func (database *StandaloneDatabase) Exec(client redis.Connection, args [][]byte) redis.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	cmdName := strings.ToLower(string(args[0]))
	if cmdName == "select" {
		if len(args) != 2 {
			return reply.MakeArgNumErrReply(cmdName)
		}
		return execSelect(client, database, args[1:])
	}

	i := client.GetDBIndex()
	db := database.dbSet[i]
	return db.Exec(client, args)
}

func (database *StandaloneDatabase) Close() {

}

func (database *StandaloneDatabase) AfterClientClose(c redis.Connection) {

}

// 执行select命令
func execSelect(c redis.Connection, database *StandaloneDatabase, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(database.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
