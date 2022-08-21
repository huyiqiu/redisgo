package aof

import (
	"io"
	"os"
	"redisgo/config"
	"redisgo/interface/database"
	"redisgo/lib/logger"
	"redisgo/lib/utils"
	"redisgo/redis/connection"
	"redisgo/redis/parser"
	"redisgo/redis/reply"
	"strconv"
)

type CmdLine = [][]byte

const aofBufferSize = 1 << 16

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// AofHandler receives msgs from channel and write to AOF file
type AofHandler struct {
	database    database.Database
	aofFile     *os.File
	aofFilename string
	currentDB   int
	aofChan     chan *payload
}

// NewAOFHandler creates a new aof.AofHandler
func NewAOFHandler(database database.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename =  config.Properties.AppendFilename
	handler.database = database
	//Load
	handler.LoadAof()
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofBufferSize)
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// AddAof sends command to aof goroutine through channel
func (handler *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// handlerAof listen aof channel and write into file
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB {
			args := utils.ToCmdLine("select", strconv.Itoa(p.dbIndex)) // 将命令转换成resp协议的byte字节组
			data := reply.MakeMultiBulkReply(args).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}

		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
	}
}

// LoadAof reads aof files
func (handler *AofHandler) LoadAof() {
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error(err)
		return 
	}
	defer file.Close()
	payloads := parser.ParseStream(file)
	// only used for save dbIndex
	fakeConn := &connection.Connection{}
	logger.Info("LoadAof...")
	for p := range payloads {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(p.Err)
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		data, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("multibulk needed")
			continue
		}

		_ = handler.database.Exec(fakeConn, data.Args)
	
	}
}