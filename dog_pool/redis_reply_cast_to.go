package dog_pool

import "fmt"
import "github.com/RUNDSP/radix/redis"

//
// Return the bool in the Redis Reply
//
// Redis/Casting Error --> error
// Nil Reply           --> false
// All other cases     --> true or false
//
func ReplyToBool(reply *redis.Reply) (bool, error) {
	switch {
	case nil != reply.Err:
		return false, reply.Err
	case redis.NilReply == reply.Type:
		return false, nil
	default:
		return reply.Bool()
	}
}

//
// Return the int64 pointer in the Redis Reply
//
// Redis/Casting Error --> error
// Cache Miss          --> nil ptr
// Cache Hit           --> valid ptr
//
func ReplyToInt64Ptr(reply *redis.Reply) (*int64, error) {
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	case redis.NilReply == reply.Type:
		return nil, nil
	default:
		value, err := reply.Int64()
		if nil != err {
			return nil, err
		}
		return &value, nil
	}
}

//
// Return the int64 pointers in the Redis Reply
//
// Redis/Casting Error --> error
// Cache Miss          --> nil ptr
// Cache Hit           --> valid ptr
//
func ReplyToInt64Ptrs(reply *redis.Reply) ([]*int64, error) {
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	case redis.MultiReply != reply.Type:
		return nil, fmt.Errorf("Reply type is not MultiReply, %#v", reply)
	default:
		output := make([]*int64, len(reply.Elems))
		for i, reply_elem := range reply.Elems {
			ptr, err := ReplyToInt64Ptr(reply_elem)
			if nil != err {
				return nil, err
			}
			output[i] = ptr
		}
		return output, nil
	}
}

//
// Return the float64 pointer in the Redis Reply
//
// Redis/Casting Error --> error
// Cache Miss          --> nil ptr
// Cache Hit           --> valid ptr
//
func ReplyToFloat64Ptr(reply *redis.Reply) (*float64, error) {
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	case redis.NilReply == reply.Type:
		return nil, nil
	default:
		value, err := reply.Float64()
		if nil != err {
			return nil, err
		}
		return &value, nil
	}
}

//
// Return the float64 pointers in the Redis Reply
//
// Redis/Casting Error --> error
// Cache Miss          --> nil ptr
// Cache Hit           --> valid ptr
//
func ReplyToFloat64Ptrs(reply *redis.Reply) ([]*float64, error) {
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	case redis.MultiReply != reply.Type:
		return nil, fmt.Errorf("Reply type is not MultiReply, %#v", reply)
	default:
		output := make([]*float64, len(reply.Elems))
		for i, reply_elem := range reply.Elems {
			ptr, err := ReplyToFloat64Ptr(reply_elem)
			if nil != err {
				return nil, err
			}
			output[i] = ptr
		}
		return output, nil
	}
}

//
// Return the string pointer in the Redis Reply
//
// Redis/Casting Error --> error
// Cache Miss          --> nil ptr
// Cache Hit           --> valid ptr
//
func ReplyToStringPtr(reply *redis.Reply) (*string, error) {
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	case redis.NilReply == reply.Type:
		return nil, nil
	case redis.IntegerReply == reply.Type:
		i64, err := reply.Int64()
		if nil != err {
			return nil, err
		}
		value := fmt.Sprintf("%d", i64)
		return &value, nil
	default:
		value, err := reply.Str()
		if nil != err {
			return nil, err
		}
		return &value, nil
	}
}

//
// Return the string pointers in the Redis Reply
//
// Redis/Casting Error --> error
// Cache Miss          --> nil ptr
// Cache Hit           --> valid ptr
//
func ReplyToStringPtrs(reply *redis.Reply) ([]*string, error) {
	switch {
	case nil != reply.Err:
		return nil, reply.Err
	case redis.MultiReply != reply.Type:
		return nil, fmt.Errorf("Reply type is not MultiReply, %#v", reply)
	default:
		output := make([]*string, len(reply.Elems))
		for i, reply_elem := range reply.Elems {
			ptr, err := ReplyToStringPtr(reply_elem)
			if nil != err {
				return nil, err
			}
			output[i] = ptr
		}
		return output, nil
	}
}
