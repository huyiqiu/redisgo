package parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"redisgo/interface/redis"
	"redisgo/lib/logger"
	"redisgo/redis/reply"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data redis.Reply
	Err  error
}

type readState struct { //读入状态（小写）
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
	// readingRepl       bool
}

func (s *readState) finished() bool { // 是多参数 且 参数都被读入
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *Payload { // <-chan作为返回值时为只读类型，即无法向返回的管道中写数据，类似于一个只开了出口的管子
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch

}

// ParseBytes从[]byte读取数据并返回所有回复
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne从[]byte读取数据并返回第一个负载
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}


func parse0(reader io.Reader, ch chan<- *Payload) { // chan<-作为参数，表示只能向管道内写数据，类似于一个只开了入口的管子
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()

	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte

	for {
		// read line
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			if ioErr {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			// protocol err
			ch <- &Payload{Err: err}
			state = readState{}
			continue
		}

		// parse line
		if !state.readingMultiLine {
			// 接收新回复
			if msg[0] == '*' {
				// 数组
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error：" + string(msg)),
					}
					state = readState{} // reset
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{} // reset
					continue
				}
			} else if msg[0] == '$' {
				// Bulk Strings
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error:" + string(msg)),
					}
					state = readState{} // reset
					continue
				}
				if state.bulkLen == -1 { // 不存在，即NULL
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{} // reset
					continue
				}
			} else {
				// single line protocol
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error:" + string(msg)),
				}
				state = readState{}
				continue
			}
			if state.finished() {
				var result redis.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) { // bool 表示是否有io错误
	var msg []byte
	var err error

	if state.bulkLen == 0 { // 1. \r\n切分
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' { // 倒数第二个不为'/r'则错误
			return nil, false, errors.New("protocol error:" + string(msg))
		}
	} else {
		msg = make([]byte, state.bulkLen + 2)    //
		_, err2 := io.ReadFull(bufReader, msg) //将buffer中的消息都读到msg中
		if err != nil {
			return nil, true, err2
		}

		if len(msg) == 0 || 
			msg[len(msg)-2] != '\r' || 
			msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error:" + string(msg))
		}
		state.bulkLen = 0
	}

	return msg, false, nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error { //数组
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// first line of multi bulk protocol
		state.msgType = msg[0]
		state.readingMultiLine = true                // 多行参数
		state.expectedArgsCount = int(expectedLine)  // 参数个数
		state.args = make([][]byte, 0, expectedLine) // 为读状态的参数初始化一个expectedLine行的二维切片
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)

	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}

	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1 // 一个参数
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}

}

func parseSingleLineReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result redis.Reply
	switch msg[0] {
	case '+': // status protocal
		result = reply.MakeStatusReply(str[1:])
	case '-': // err
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error:" + string(msg))
		}
		result = reply.MakeIntReply(val)
	default:
		//parse as text protocol
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = reply.MakeMultiBulkReply(args)
	}
	return result, nil
}

// read the non-first lines of multi bulk protocol or bulk protocol
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error:" + string(msg))
		}
		if state.bulkLen <= 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
