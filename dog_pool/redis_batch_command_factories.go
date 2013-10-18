package dog_pool

import "time"

var cmd_bitop = "BITOP"
var cmd_bitop_and = []byte("AND")
var cmd_bitop_not = []byte("NOT")
var cmd_bitop_or = []byte("OR")
var cmd_getbit = "GETBIT"
var cmd_setbit = "SETBIT"
var cmd_bitcount = "BITCOUNT"

var cmd_exists = "EXISTS"
var cmd_expire = "EXPIRE"
var cmd_ttl = "TTL"
var cmd_mget = "MGET"
var cmd_get = "GET"
var cmd_del = "DEL"
var cmd_set = "SET"
var cmd_hexists = "HEXISTS"
var cmd_hmget = "HMGET"
var cmd_hget = "HGET"
var cmd_hdel = "HDEL"
var cmd_hset = "HSET"
var cmd_hincrby = "HINCRBY"

//
// Factory Methods:
//

// Basic factory method
func MakeRedisBatchCommand(cmd string) *RedisBatchCommand {
	return &RedisBatchCommand{cmd, [][]byte{}, nil}
}

// EXISTS <KEY>
func MakeRedisBatchCommandExists(key string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_exists,
		args:  make([][]byte, 1)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	return output
}

// EXPIRE <KEY> <SECONDS>
func MakeRedisBatchCommandExpireIn(key string, expire_in time.Duration) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_expire,
		args:  make([][]byte, 2)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteIntArg(int64(expire_in.Seconds()))
	return output
}

// EXPIRE <KEY> <SECONDS>
func MakeRedisBatchCommandGetExpiresIn(key string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_ttl,
		args:  make([][]byte, 1)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	return output
}

// MGET <KEY> <KEY> <KEY>...
func MakeRedisBatchCommandMget(keys ...string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_mget,
		args:  make([][]byte, len(keys))[0:0],
		reply: nil,
	}
	output.WriteStringArgs(keys)
	return output
}

// GET <KEY>
func MakeRedisBatchCommandGet(key string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_get,
		args:  make([][]byte, 1)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	return output
}

// SET <KEY> <VALUE>
func MakeRedisBatchCommandSet(key string, value []byte) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_set,
		args:  make([][]byte, 2)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteArg(value)
	return output
}

// DEL <KEY> <KEY> <KEY> ....
func MakeRedisBatchCommandDelete(keys ...string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_del,
		args:  make([][]byte, len(keys))[0:0],
		reply: nil,
	}
	output.WriteStringArgs(keys)
	return output
}

// HEXISTS <KEY> <FIELD>
func MakeRedisBatchCommandHashExists(key, field string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_hexists,
		args:  make([][]byte, 2)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteStringArg(field)
	return output
}

// HMGET <KEY> <FIELD> <FIELD> <FIELD>...
func MakeRedisBatchCommandHashMget(key string, fields ...string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_hmget,
		args:  make([][]byte, 1+len(fields))[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteStringArgs(fields)
	return output
}

// HGET <KEY> <FIELD>
func MakeRedisBatchCommandHashGet(key, field string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_hget,
		args:  make([][]byte, 2)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteStringArg(field)
	return output
}

// HSET <KEY> <FIELD> <VALUE>
func MakeRedisBatchCommandHashSet(key, field string, value []byte) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_hset,
		args:  make([][]byte, 3)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteStringArg(field)
	output.WriteArg(value)
	return output
}

// HDEL <KEY> <KEY> <KEY> ....
func MakeRedisBatchCommandHashDelete(key string, fields ...string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_hdel,
		args:  make([][]byte, 1+len(fields))[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteStringArgs(fields)
	return output
}

// HDEL <KEY> <KEY> <KEY> ....
func MakeRedisBatchCommandHashIncrementBy(key, field string, delta int64) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_hincrby,
		args:  make([][]byte, 2)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteStringArg(field)
	output.WriteIntArg(delta)
	return output
}

// BITOP AND <DEST> <SRC KEYS> ...
func MakeRedisBatchCommandBitopAnd(dest string, sources ...string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_bitop,
		args:  make([][]byte, 2+len(sources))[0:0],
		reply: nil,
	}
	output.WriteArg(cmd_bitop_and)
	output.WriteStringArg(dest)
	output.WriteStringArgs(sources)
	return output
}

// BITOP NOT <DEST> <SRC>
func MakeRedisBatchCommandBitopNot(dest string, source string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_bitop,
		args:  make([][]byte, 3)[0:0],
		reply: nil,
	}
	output.WriteArg(cmd_bitop_not)
	output.WriteStringArg(dest)
	output.WriteStringArg(source)
	return output
}

// BITOP OR <DEST> <SRC KEYS> ...
func MakeRedisBatchCommandBitopOr(dest string, sources ...string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_bitop,
		args:  make([][]byte, 2+len(sources))[0:0],
		reply: nil,
	}
	output.WriteArg(cmd_bitop_or)
	output.WriteStringArg(dest)
	output.WriteStringArgs(sources)
	return output
}

// GETBIT <KEY> <INDEX>
func MakeRedisBatchCommandGetBit(key string, index int64) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_getbit,
		args:  make([][]byte, 2)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteIntArg(index)
	return output
}

// SETBIT <KEY> <INDEX> <STATE>
func MakeRedisBatchCommandSetBit(key string, index int64, state bool) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_setbit,
		args:  make([][]byte, 3)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	output.WriteIntArg(index)
	output.WriteBoolArg(state)
	return output
}

// BITCOUNT <KEY>
func MakeRedisBatchCommandBitCount(key string) *RedisBatchCommand {
	output := &RedisBatchCommand{
		cmd:   cmd_bitcount,
		args:  make([][]byte, 1)[0:0],
		reply: nil,
	}
	output.WriteStringArg(key)
	return output
}
