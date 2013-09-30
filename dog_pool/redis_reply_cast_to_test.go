package dog_pool

import "strings"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"
import "github.com/RUNDSP/radix/redis"

func TestReplyToSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(ReplyToSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func ReplyToSpecs(c gospec.Context) {

	c.Specify("[ReplyToBool] returns boolean or error", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Cache Miss
		reply := server.Connection().Cmd("EXISTS", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.IntegerReply)

		value, value_err := ReplyToBool(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, false)

		// Cache Hit
		server.Connection().Cmd("SET", "Bob", "George")
		reply = server.Connection().Cmd("EXISTS", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.IntegerReply)

		value, value_err = ReplyToBool(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, true)

		// Parsing Error
		reply = server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)

		value, value_err = ReplyToBool(reply)
		c.Expect(value_err, gospec.Satisfies, nil != value_err)
		c.Expect(value, gospec.Equals, false)
	})

	c.Specify("[ReplyToInt64Ptr] returns *int64 or error", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Cache Miss
		reply := server.Connection().Cmd("GET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.NilReply)

		value, value_err := ReplyToInt64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil == value)

		// Cache Hit --> String
		server.Connection().Cmd("SET", "Bob", "123")
		reply = server.Connection().Cmd("GET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToInt64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value, gospec.Satisfies, int64(123) == *value)

		// Cache Hit --> Integer
		reply = server.Connection().Cmd("INCRBY", "Bob", 1000)
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.IntegerReply)

		value, value_err = ReplyToInt64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(*value, gospec.Equals, int64(1123))

		// Cache Hit --> Integer
		reply = server.Connection().Cmd("DECRBY", "Bob", 1000)
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.IntegerReply)

		value, value_err = ReplyToInt64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(*value, gospec.Equals, int64(123))

		// Cache Hit --> Integer
		reply = server.Connection().Cmd("INCR", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.IntegerReply)

		value, value_err = ReplyToInt64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(*value, gospec.Equals, int64(124))

		// Cache Hit --> Integer
		reply = server.Connection().Cmd("DECR", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.IntegerReply)

		value, value_err = ReplyToInt64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(*value, gospec.Equals, int64(123))

		// Parsing Error
		reply = server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)

		value, value_err = ReplyToInt64Ptr(reply)
		c.Expect(value_err, gospec.Satisfies, nil != value_err)
		c.Expect(value, gospec.Satisfies, nil == value)
	})

	c.Specify("[ReplyToInt64Ptrs] returns []*int64 or error", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Invalid Reply Type

		// Cache Miss
		reply := server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)

		value, value_err := ReplyToInt64Ptrs(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, 1 == len(value))
		c.Expect(value[0], gospec.Satisfies, nil == value[0])

		// Cache Hit --> String
		server.Connection().Cmd("SET", "Bob", "123")
		reply = server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)
		c.Expect(len(reply.Elems), gospec.Equals, 1)
		c.Expect(reply.Elems[0].Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToInt64Ptrs(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, 1 == len(value))
		c.Expect(value[0], gospec.Satisfies, nil != value[0])
		c.Expect(value[0], gospec.Satisfies, int64(123) == *value[0])

		// Parsing Error
		reply = server.Connection().Cmd("GET", "Missing")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.NilReply)

		value, value_err = ReplyToInt64Ptrs(reply)
		c.Expect(value_err, gospec.Satisfies, strings.HasPrefix(value_err.Error(), "Reply type is not MultiReply, "))
		c.Expect(value, gospec.Satisfies, 0 == len(value))

		// Parsing Error
		reply = server.Connection().Cmd("GET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToInt64Ptrs(reply)
		c.Expect(value_err, gospec.Satisfies, strings.HasPrefix(value_err.Error(), "Reply type is not MultiReply, "))
		c.Expect(value, gospec.Satisfies, 0 == len(value))
	})

	c.Specify("[ReplyToFloat64Ptr] returns *float64 or error", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Cache Miss
		reply := server.Connection().Cmd("GET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.NilReply)

		value, value_err := ReplyToFloat64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil == value)

		// Cache Hit --> String
		server.Connection().Cmd("SET", "Bob", "123.456")
		reply = server.Connection().Cmd("GET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToFloat64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value, gospec.Satisfies, float64(123.456) == *value)

		// Cache Hit --> Float
		reply = server.Connection().Cmd("INCRBYFLOAT", "Bob", 1000.0)
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToFloat64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(*value, gospec.Equals, float64(1123.456))

		// Cache Hit --> Integer
		server.Connection().Cmd("SET", "Bob", 123)
		reply = server.Connection().Cmd("INCR", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.IntegerReply)

		value, value_err = ReplyToFloat64Ptr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(*value, gospec.Equals, float64(123+1))

		// Parsing Error
		reply = server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)

		value, value_err = ReplyToFloat64Ptr(reply)
		c.Expect(value_err, gospec.Satisfies, nil != value_err)
		c.Expect(value, gospec.Satisfies, nil == value)
	})

	c.Specify("[ReplyToFloat64Ptrs] returns []*float64 or error", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Invalid Reply Type

		// Cache Miss
		reply := server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)

		value, value_err := ReplyToFloat64Ptrs(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, 1 == len(value))
		c.Expect(value[0], gospec.Satisfies, nil == value[0])

		// Cache Hit --> String
		server.Connection().Cmd("SET", "Bob", "123.456")
		reply = server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)
		c.Expect(len(reply.Elems), gospec.Equals, 1)
		c.Expect(reply.Elems[0].Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToFloat64Ptrs(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, 1 == len(value))
		c.Expect(value[0], gospec.Satisfies, nil != value[0])
		c.Expect(value[0], gospec.Satisfies, float64(123.456) == *value[0])

		// Parsing Error
		reply = server.Connection().Cmd("GET", "Missing")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.NilReply)

		value, value_err = ReplyToFloat64Ptrs(reply)
		c.Expect(value_err, gospec.Satisfies, strings.HasPrefix(value_err.Error(), "Reply type is not MultiReply, "))
		c.Expect(value, gospec.Satisfies, 0 == len(value))

		// Parsing Error
		reply = server.Connection().Cmd("GET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToFloat64Ptrs(reply)
		c.Expect(value_err, gospec.Satisfies, strings.HasPrefix(value_err.Error(), "Reply type is not MultiReply, "))
		c.Expect(value, gospec.Satisfies, 0 == len(value))
	})

	c.Specify("[ReplyToStringPtr] returns *string or error", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Cache Miss
		reply := server.Connection().Cmd("GET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.NilReply)

		value, value_err := ReplyToStringPtr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil == value)

		// Cache Hit --> String
		server.Connection().Cmd("SET", "Bob", "123.456")
		reply = server.Connection().Cmd("GET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToStringPtr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value, gospec.Satisfies, string("123.456") == *value)

		// Cache Hit --> Float
		reply = server.Connection().Cmd("INCRBYFLOAT", "Bob", 1000.0)
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToStringPtr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(*value, gospec.Equals, string("1123.45599999999999996"))

		// Cache Hit --> Integer
		server.Connection().Cmd("SET", "Bob", 123)
		reply = server.Connection().Cmd("INCR", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.IntegerReply)

		value, value_err = ReplyToStringPtr(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(*value, gospec.Equals, string("124"))

		// Parsing Error
		reply = server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)

		value, value_err = ReplyToStringPtr(reply)
		c.Expect(value_err, gospec.Satisfies, nil != value_err)
		c.Expect(value, gospec.Satisfies, nil == value)
	})

	c.Specify("[ReplyToStringPtrs] returns []*string or error", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Invalid Reply Type

		// Cache Miss
		reply := server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)

		value, value_err := ReplyToStringPtrs(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, 1 == len(value))
		c.Expect(value[0], gospec.Satisfies, nil == value[0])

		// Cache Hit --> String
		server.Connection().Cmd("SET", "Bob", "123.456")
		reply = server.Connection().Cmd("MGET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)
		c.Expect(len(reply.Elems), gospec.Equals, 1)
		c.Expect(reply.Elems[0].Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToStringPtrs(reply)
		c.Expect(value_err, gospec.Equals, nil)
		c.Expect(value, gospec.Satisfies, 1 == len(value))
		c.Expect(value[0], gospec.Satisfies, nil != value[0])
		c.Expect(value[0], gospec.Satisfies, string("123.456") == *value[0])

		// Parsing Error
		reply = server.Connection().Cmd("GET", "Missing")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.NilReply)

		value, value_err = ReplyToStringPtrs(reply)
		c.Expect(value_err, gospec.Satisfies, strings.HasPrefix(value_err.Error(), "Reply type is not MultiReply, "))
		c.Expect(value, gospec.Satisfies, 0 == len(value))

		// Parsing Error
		reply = server.Connection().Cmd("GET", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply, gospec.Satisfies, nil != reply)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		value, value_err = ReplyToStringPtrs(reply)
		c.Expect(value_err, gospec.Satisfies, strings.HasPrefix(value_err.Error(), "Reply type is not MultiReply, "))
		c.Expect(value, gospec.Satisfies, 0 == len(value))
	})

}
