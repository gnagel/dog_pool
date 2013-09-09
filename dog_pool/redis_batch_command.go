package dog_pool

import "bytes"
import "fmt"
import "strconv"
import "strings"
import "github.com/fzzy/radix/redis"

//
// Queued Redis Command & Reply
//
type RedisBatchCommand struct {
	cmd   string "Command we are executing"
	args  [][]byte
	reply *redis.Reply
}

//
// Redis Client Interface "proxy" methods:
//

// Cmd calls the given Redis command.
func (p *RedisBatchCommand) RedisCmd(connection RedisClientInterface) *redis.Reply {
	p.RedisAppend(connection)
	p.RedisGetReply(connection)
	return p.Reply()
}

// Append adds the given call to the pipeline queue.
// Use GetReply() to read the reply.
func (p *RedisBatchCommand) RedisAppend(connection RedisClientInterface) {
	connection.Append(p.cmd, p.args)
}

// GetReply returns the reply for the next request in the pipeline queue.
// Error reply with PipelineQueueEmptyError is returned,
// if the pipeline queue is empty.
func (p *RedisBatchCommand) RedisGetReply(connection RedisClientInterface) *redis.Reply {
	p.reply = connection.GetReply()
	return p.reply
}

//
// Accessors:
//
func (p *RedisBatchCommand) GetCmd() string {
	return string(p.cmd)
}

func (p *RedisBatchCommand) GetArgs() []string {
	output := make([]string, len(p.args))
	for i, arg := range p.args {
		output[i] = string(arg)
	}
	return output
}

func (p *RedisBatchCommand) Reply() *redis.Reply {
	return p.reply
}

func (p *RedisBatchCommand) String() string {
	return fmt.Sprintf("%s %s --> %#v", p.GetCmd(), strings.Join(p.GetArgs(), " "), p.reply)
}

//
// Helpers:
//
func (p *RedisBatchCommand) WriteArg(arg []byte) {
	p.args = append(p.args, arg)
}

func (p *RedisBatchCommand) WriteBoolArg(arg bool) {
	var value string
	switch arg {
	case true:
		value = "1"
	default:
		value = "0"
	}
	p.WriteArg([]byte(value))
}

func (p *RedisBatchCommand) WriteStringArg(arg string) {
	p.WriteArg([]byte(arg))
}
func (p *RedisBatchCommand) WriteStringArgs(args []string) {
	for _, arg := range args {
		p.WriteArg([]byte(arg))
	}
}

func (p *RedisBatchCommand) WriteIntArg(arg int64) {
	value := strconv.FormatInt(arg, 10)
	p.WriteArg([]byte(value))
}
func (p *RedisBatchCommand) WriteIntArgs(args []int64) {
	for _, arg := range args {
		value := strconv.FormatInt(arg, 10)
		p.WriteArg([]byte(value))
	}
}

func (p *RedisBatchCommand) WriteFloatArg(arg float64) {
	value := fmt.Sprintf("%f", arg)
	p.WriteArg([]byte(value))
}
func (p *RedisBatchCommand) WriteFloatArgs(args []float64) {
	for _, arg := range args {
		value := fmt.Sprintf("%f", arg)
		p.WriteArg([]byte(value))
	}
}

//
// Is XXX operations?
//
func (p *RedisBatchCommand) IsBitop() bool {
	return p.cmd == cmd_bitop
}

func (p *RedisBatchCommand) IsBitopAnd() bool {
	return p.IsBitop() && bytes.Equal(p.args[0], cmd_bitop_and)
}

func (p *RedisBatchCommand) IsBitopOr() bool {
	return p.IsBitop() && bytes.Equal(p.args[0], cmd_bitop_or)
}

func (p *RedisBatchCommand) IsBitopNot() bool {
	return p.IsBitop() && bytes.Equal(p.args[0], cmd_bitop_not)
}
