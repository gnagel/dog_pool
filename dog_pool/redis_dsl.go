package dog_pool

import "fmt"
import "github.com/RUNDSP/radix/redis"

//
// Short hand commands for common operations
//
type RedisDsl struct {
	RedisClientInterface
}

//
// ==================================================
//
// Common Redis EXISTS "X" Operations:
//
// ==================================================
//

// Does the Key Exist?
func (p RedisDsl) KEY_EXISTS(key string) (bool, error) {
	if len(key) == 0 {
		return false, fmt.Errorf("Empty key")
	}

	return ReplyToBool(p.Cmd("EXISTS", key))
}

// Do the Keys Exist?
func (p RedisDsl) KEYS_EXIST(keys ...string) ([]bool, error) {
	for _, key := range keys {
		p.Append("EXISTS", key)
	}

	output := make([]bool, len(keys))
	for i := range keys {
		b, err := ReplyToBool(p.GetReply())
		if nil != err {
			return nil, err
		}

		output[i] = b
	}

	return output, nil
}

// Does the Key Exist?
func (p RedisDsl) HASH_FIELD_EXISTS(key, field string) (bool, error) {
	if len(key) == 0 {
		return false, fmt.Errorf("Empty key")
	}
	if len(field) == 0 {
		return false, fmt.Errorf("Empty field")
	}

	return ReplyToBool(p.Cmd("HEXISTS", key, field))
}

// Do the Keys Exist?
func (p RedisDsl) HASH_FIELDS_EXIST(key string, fields ...string) ([]bool, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("Empty key")
	}
	for i, field := range fields {
		if len(field) == 0 {
			return nil, fmt.Errorf("Empty field[%d]", i)
		}
		p.Append("HEXISTS", key, field)
	}

	output := make([]bool, len(fields))
	for i := range fields {
		b, err := ReplyToBool(p.GetReply())
		if nil != err {
			return nil, err
		}

		output[i] = b
	}

	return output, nil
}

//
// ==================================================
//
// Common Redis INCREMENT/DECREMENT "X" Operations:
//
// ==================================================
//

// Increment the key by 1
func (p RedisDsl) INCR(key string) (int64, error) {
	return p.Cmd("INCR", key).Int64()
}

// Increment the key by an integer amount
func (p RedisDsl) INCRBY(key string, amount int64) (int64, error) {
	return p.Cmd("INCRBY", key, amount).Int64()
}

// Increment the key by an float amount
func (p RedisDsl) INCRBYFLOAT(key string, amount float64) (float64, error) {
	return p.Cmd("INCRBYFLOAT", key, amount).Float64()
}

// Decrement the key by 1
func (p RedisDsl) DECR(key string) (int64, error) {
	return p.Cmd("DECR", key).Int64()
}

// Decrement the key by an integer amount
func (p RedisDsl) DECRBY(key string, amount int64) (int64, error) {
	return p.Cmd("DECRBY", key, amount).Int64()
}

// Decrement the key by an float amount
func (p RedisDsl) DECRBYFLOAT(key string, amount float64) (float64, error) {
	return p.INCRBYFLOAT(key, -1.0*amount)
}

// Increment the hash key's field by 1
func (p RedisDsl) HASH_INCR(key, field string) (int64, error) {
	return p.HASH_INCRBY(key, field, 1)
}

// Increment the hash key's field by an integer amount
func (p RedisDsl) HASH_INCRBY(key, field string, amount int64) (int64, error) {
	return p.Cmd("HINCRBY", key, field, amount).Int64()
}

// Increment the hash key's field by an float amount
func (p RedisDsl) HASH_INCRBYFLOAT(key, field string, amount float64) (float64, error) {
	return p.Cmd("HINCRBYFLOAT", key, field, amount).Float64()
}

// Decrement the hash key's field by 1
func (p RedisDsl) HASH_DECR(key, field string) (int64, error) {
	return p.HASH_INCRBY(key, field, -1)
}

// Decrement the hash key's field by an integer amount
func (p RedisDsl) HASH_DECRBY(key, field string, amount int64) (int64, error) {
	return p.HASH_INCRBY(key, field, -1*amount)
}

// Decrement the hash key's field by an float amount
func (p RedisDsl) HASH_DECRBYFLOAT(key, field string, amount float64) (float64, error) {
	return p.HASH_INCRBYFLOAT(key, field, -1*amount)
}

//
// ==================================================
//
// Common Redis GET "X" Operations:
//
// ==================================================
//

// Get the key's value
func (p RedisDsl) GET(key string) *redis.Reply {
	return p.Cmd("GET", key)
}

// Get the keys' values
func (p RedisDsl) MGET(keys ...string) *redis.Reply {
	return p.Cmd("MGET", keys)
}

// Get the hash key's field's value
func (p RedisDsl) HASH_GET(key, field string) *redis.Reply {
	return p.Cmd("HGET", key, field)
}

// Get the hash key's fields' values
func (p RedisDsl) HASH_MGET(key string, fields ...string) *redis.Reply {
	return p.Cmd("HMGET", key, fields)
}

// Get the hash key's fields' values
func (p RedisDsl) HASH_GETALL(key string) *redis.Reply {
	return p.Cmd("HGETALL", key)
}

// Get the hash field's value from several parallel hash keys
func (p RedisDsl) HASHES_GET(keys []string, field string) []*redis.Reply {
	for _, key := range keys {
		p.Append("HGET", key, field)
	}

	output := make([]*redis.Reply, len(keys))
	for i := range keys {
		output[i] = p.GetReply()
	}

	return output
}

// Get the hash fields' values from several parallel hash keys
func (p RedisDsl) HASHES_MGET(keys []string, fields ...string) []*redis.Reply {
	for _, key := range keys {
		p.Append("HMGET", key, fields)
	}

	output := make([]*redis.Reply, len(keys))
	for i := range keys {
		output[i] = p.GetReply()
	}

	return output
}

// Get the hash fields' values from several parallel hash keys
func (p RedisDsl) HASHES_GETALL(keys []string) []*redis.Reply {
	for _, key := range keys {
		p.Append("HGETALL", key)
	}

	output := make([]*redis.Reply, len(keys))
	for i := range keys {
		output[i] = p.GetReply()
	}

	return output
}

// Get the key's bit state
func (p RedisDsl) GETBIT(key string, position int64) (bool, error) {
	if len(key) == 0 {
		return false, fmt.Errorf("Empty key")
	}

	return ReplyToBool(p.Cmd("GETBIT", key, position))
}

// Get the key's bit state
func (p RedisDsl) GETBITS(key string, positions ...int64) ([]bool, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("Empty key")
	}

	for i, position := range positions {
		if position < 0 {
			return nil, fmt.Errorf("Empty negative bit position[%d]=%d", i, position)
		}

		p.Append("GETBIT", key, position)
	}

	output := make([]bool, len(positions))
	for i := range positions {
		b, err := ReplyToBool(p.GetReply())
		if nil != err {
			return nil, err
		}

		output[i] = b
	}

	return output, nil
}

// Decode the key's bit states
func (p RedisDsl) GETBITS_TURNED_ON(key string) ([]int64, error) {
	reply := p.GET(key)
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	case redis.NilReply == reply.Type:
		return []int64{}, nil
	default:
		as_bytes, err := reply.Bytes()
		if nil != err {
			return nil, err
		}
		return MapBitmapToIndices(as_bytes), nil
	}
}

//
// ==================================================
//
// Short Hand Conversion for the above GET/MGET operations:
//
// ==================================================
//

// Get the key's string value
func (p RedisDsl) GET_STRING(key string) (*string, error) {
	return ReplyToStringPtr(p.GET(key))
}

// Get the keys' string values
func (p RedisDsl) MGET_STRINGS(keys ...string) ([]*string, error) {
	return ReplyToStringPtrs(p.MGET(keys...))
}

// Get the key's int64 value
func (p RedisDsl) GET_INT64(key string) (*int64, error) {
	return ReplyToInt64Ptr(p.GET(key))
}

// Get the keys' int64 values
func (p RedisDsl) MGET_INT64S(keys ...string) ([]*int64, error) {
	return ReplyToInt64Ptrs(p.MGET(keys...))
}

// Get the key's float64 value
func (p RedisDsl) GET_FLOAT64(key string) (*float64, error) {
	return ReplyToFloat64Ptr(p.GET(key))
}

// Get the keys' float64 values
func (p RedisDsl) MGET_FLOAT64S(keys ...string) ([]*float64, error) {
	return ReplyToFloat64Ptrs(p.MGET(keys...))
}

//
// ==================================================
//
// Short Hand Conversion for the above HASH_GET/HASH_MGET operations:
//
// ==================================================
//

// Get the key's string value
func (p RedisDsl) HASH_GET_STRING(key, field string) (*string, error) {
	return ReplyToStringPtr(p.HASH_GET(key, field))
}

// Get the keys' string values
func (p RedisDsl) HASH_MGET_STRINGS(key string, fields ...string) ([]*string, error) {
	return ReplyToStringPtrs(p.HASH_MGET(key, fields...))
}

// Get the key's int64 value
func (p RedisDsl) HASH_GET_INT64(key, field string) (*int64, error) {
	return ReplyToInt64Ptr(p.HASH_GET(key, field))
}

// Get the keys' int64 values
func (p RedisDsl) HASH_MGET_INT64S(key string, fields ...string) ([]*int64, error) {
	return ReplyToInt64Ptrs(p.HASH_MGET(key, fields...))
}

// Get the key's float64 value
func (p RedisDsl) HASH_GET_FLOAT64(key, field string) (*float64, error) {
	return ReplyToFloat64Ptr(p.HASH_GET(key, field))
}

// Get the keys' float64 values
func (p RedisDsl) HASH_MGET_FLOAT64S(key string, fields ...string) ([]*float64, error) {
	return ReplyToFloat64Ptrs(p.HASH_MGET(key, fields...))
}

//
// ==================================================
//
// Short Hand Conversion for the above HASHES_GET/HASHES_MGET operations:
//
// ==================================================
//

// Get the key's string value
func (p RedisDsl) HASHES_GET_STRING(keys []string, field string) ([]*string, error) {
	replys := p.HASHES_GET(keys, field)

	output := make([]*string, len(keys))
	for i, reply := range replys {
		ptr, err := ReplyToStringPtr(reply)
		if nil != err {
			return nil, err
		}
		output[i] = ptr
	}

	return output, nil
}

// Get the keys' string values
func (p RedisDsl) HASHES_MGET_STRINGS(keys []string, fields ...string) ([][]*string, error) {
	replys := p.HASHES_MGET(keys, fields...)

	output := make([][]*string, len(keys))
	for i, reply := range replys {
		ptrs, err := ReplyToStringPtrs(reply)
		if nil != err {
			return nil, err
		}
		output[i] = ptrs
	}

	return output, nil
}

// Get the keys' string values
func (p RedisDsl) HASHES_GETALL_STRINGS(keys []string) ([][]*string, error) {
	replys := p.HASHES_GETALL(keys)

	output := make([][]*string, len(keys))
	for i, reply := range replys {
		ptrs, err := ReplyToStringPtrs(reply)
		if nil != err {
			return nil, err
		}
		output[i] = ptrs
	}

	return output, nil
}

// Get the key's int64 value
func (p RedisDsl) HASHES_GET_INT64(keys []string, field string) ([]*int64, error) {
	replys := p.HASHES_GET(keys, field)

	output := make([]*int64, len(keys))
	for i, reply := range replys {
		ptr, err := ReplyToInt64Ptr(reply)
		if nil != err {
			return nil, err
		}
		output[i] = ptr
	}

	return output, nil
}

// Get the keys' int64 values
func (p RedisDsl) HASHES_MGET_INT64S(keys []string, fields ...string) ([][]*int64, error) {
	replys := p.HASHES_MGET(keys, fields...)

	output := make([][]*int64, len(keys))
	for i, reply := range replys {
		ptrs, err := ReplyToInt64Ptrs(reply)
		if nil != err {
			return nil, err
		}
		output[i] = ptrs
	}

	return output, nil
}

// Get the keys' int64 values
func (p RedisDsl) HASHES_GETALL_INT64S(keys []string) ([][]*int64, error) {
	replys := p.HASHES_GETALL(keys)

	output := make([][]*int64, len(keys))
	for i, reply := range replys {
		ptrs, err := ReplyToInt64Ptrs(reply)
		if nil != err {
			return nil, err
		}
		output[i] = ptrs
	}

	return output, nil
}

// Get the key's float64 value
func (p RedisDsl) HASHES_GET_FLOAT64(keys []string, field string) ([]*float64, error) {
	replys := p.HASHES_GET(keys, field)

	output := make([]*float64, len(keys))
	for i, reply := range replys {
		ptr, err := ReplyToFloat64Ptr(reply)
		if nil != err {
			return nil, err
		}
		output[i] = ptr
	}

	return output, nil
}

// Get the keys' float64 values
func (p RedisDsl) HASHES_MGET_FLOAT64S(keys []string, fields ...string) ([][]*float64, error) {
	replys := p.HASHES_MGET(keys, fields...)

	output := make([][]*float64, len(keys))
	for i, reply := range replys {
		ptrs, err := ReplyToFloat64Ptrs(reply)
		if nil != err {
			return nil, err
		}
		output[i] = ptrs
	}

	return output, nil
}

// Get the keys' float64 values
func (p RedisDsl) HASHES_GETALL_FLOAT64S(keys []string) ([][]*float64, error) {
	replys := p.HASHES_GETALL(keys)

	output := make([][]*float64, len(keys))
	for i, reply := range replys {
		ptrs, err := ReplyToFloat64Ptrs(reply)
		if nil != err {
			return nil, err
		}
		output[i] = ptrs
	}

	return output, nil
}
