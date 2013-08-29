package dog_pool

import "fmt"
import "strings"
import "github.com/fzzy/radix/redis"

const BITOP = "BITOP"
const BIT_AND = "AND"
const BIT_OR = "OR"
const BIT_NOT = "NOT"

const BITCOUNT = "BITCOUNT"

type RedisBatchCommand struct {
	Cmd   string
	Args  []string
	Reply *redis.Reply
}

func (p *RedisBatchCommand) String() string {
	return fmt.Sprintf("%s %s --> %#v", p.Cmd, strings.Join(p.Args, " "), p.Reply)
}

func MakeBitopAnd(dest string, sources []string) *RedisBatchCommand {
	return makeBitopCommand(BIT_AND, dest, sources)
}

func MakeBitopOr(dest string, sources []string) *RedisBatchCommand {
	return makeBitopCommand(BIT_OR, dest, sources)
}

func MakeBitopNot(dest string, sources []string) *RedisBatchCommand {
	return makeBitopCommand(BIT_NOT, dest, sources)
}

func MakeGetBit(key string, index int64) *RedisBatchCommand {
	output := &RedisBatchCommand{}
	output.Cmd = "GETBIT"
	output.Args = []string{key, fmt.Sprintf("%d", index)}
	return output
}


func MakeSetBit(key string, index int64, state bool) *RedisBatchCommand {
	state_str := "1"
	if (!state) {
		state_str ="0"
	}
	
	output := &RedisBatchCommand{}
	output.Cmd = "SETBIT"
	output.Args = []string{key, fmt.Sprintf("%d", index), state_str}
	return output
}


func MakeBitCount(key string) *RedisBatchCommand {
	output := &RedisBatchCommand{}
	output.Cmd = BITCOUNT
	output.Args = []string{key}
	return output
}

func MakeGet(key string) *RedisBatchCommand {
	output := &RedisBatchCommand{}
	output.Cmd = "GET"
	output.Args = []string{key}
	return output
}

func MakeDelete(keys []string) *RedisBatchCommand {
	output := &RedisBatchCommand{}
	output.Cmd = "DEL"
	output.Args = keys
	return output
}

func SelectBitopDestKeys(commands []*RedisBatchCommand) []string {
	output := []string{}
	for _, command := range commands {
		if command.Cmd == BITOP {
			output = append(output, command.Args[1])
		}
	}
	return output
}

func makeBitopCommand(operation, dest string, sources []string) *RedisBatchCommand {
	output := &RedisBatchCommand{}
	output.Cmd = BITOP
	output.Args = make([]string, 2+len(sources))
	output.Args[0] = operation
	output.Args[1] = dest
	copy(output.Args[2:], sources)
	return output
}
