package dog_pool

import "fmt"
import "math"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"
import "github.com/RUNDSP/radix/redis"

func TestRedisDslSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisDslSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisDslSpecs(c gospec.Context) {

	//
	// ==================================================
	//
	// Common Redis EXISTS "X" Operations:
	//
	// ==================================================
	//

	c.Specify("[RedisDsl][KEY_EXISTS]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.KEY_EXISTS("Miss")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, false)

		server.Connection().Cmd("SET", "Bob", "123")
		value, err = dsl.KEY_EXISTS("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, true)

		value, err = dsl.KEY_EXISTS("")
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(value, gospec.Equals, false)
	})

	c.Specify("[RedisDsl][KEYS_EXIST]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.KEYS_EXIST("Miss")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(value), gospec.Equals, 1)
		c.Expect(value[0], gospec.Equals, false)

		server.Connection().Cmd("SET", "Bob", "123")
		value, err = dsl.KEYS_EXIST("Bob", "Miss")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(value), gospec.Equals, 2)
		c.Expect(value[0], gospec.Equals, true)
		c.Expect(value[1], gospec.Equals, false)
	})
	//
	// ==================================================
	//
	// Common Redis INCREMENT/DECREMENT "X" Operations:
	//
	// ==================================================
	//

	c.Specify("[RedisDsl][INCR]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.INCR("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(1))

		server.Connection().Cmd("SET", "Bob", "123")
		value, err = dsl.INCR("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(123+1))
	})

	c.Specify("[RedisDsl][INCRBY]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.INCRBY("Bob", 1000)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(1000))

		server.Connection().Cmd("SET", "Bob", "123")
		value, err = dsl.INCRBY("Bob", 1000)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(1123))
	})

	c.Specify("[RedisDsl][INCRBYFLOAT]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.INCRBYFLOAT("Bob", 1000.0)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, float64(1000))

		server.Connection().Cmd("SET", "Bob", "123.456")
		value, err = dsl.INCRBYFLOAT("Bob", 1000.0)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, float64(1123.456))
	})

	c.Specify("[RedisDsl][DECR]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.DECR("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(-1))

		server.Connection().Cmd("SET", "Bob", "123")
		value, err = dsl.DECR("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(123-1))
	})

	c.Specify("[RedisDsl][DECRBY]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.DECRBY("Bob", 1000)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(-1000))

		server.Connection().Cmd("SET", "Bob", "123")
		value, err = dsl.DECRBY("Bob", 1000.0)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(123-1000.0))
	})

	c.Specify("[RedisDsl][DECRBYFLOAT]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.DECRBYFLOAT("Bob", 1000.0)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, float64(-1000))

		server.Connection().Cmd("SET", "Bob", "123.456")
		value, err = dsl.DECRBYFLOAT("Bob", 1000.0)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, float64(123.456-1000.0))
	})

	c.Specify("[RedisDsl][HASH_INCR]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.HASH_INCR("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(1))

		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123")
		value, err = dsl.HASH_INCR("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(123+1))
	})

	c.Specify("[RedisDsl][HASH_INCRBY]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.HASH_INCRBY("Hash Name", "Bob", 1000)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(1000))

		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123")
		value, err = dsl.HASH_INCRBY("Hash Name", "Bob", 1000)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(123+1000))
	})

	c.Specify("[RedisDsl][HASH_INCRBYFLOAT]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.HASH_INCRBYFLOAT("Hash Name", "Bob", 1000.0)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, float64(1000))

		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123.456")
		value, err = dsl.HASH_INCRBYFLOAT("Hash Name", "Bob", 1000.0)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, float64(123.456+1000))
	})

	c.Specify("[RedisDsl][HASH_DECR]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.HASH_DECR("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(-1))

		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123")
		value, err = dsl.HASH_DECR("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(123-1))
	})

	c.Specify("[RedisDsl][HASH_DECRBY]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.HASH_DECRBY("Hash Name", "Bob", 1000)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(-1000))

		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123")
		value, err = dsl.HASH_DECRBY("Hash Name", "Bob", 1000)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, int64(123-1000))
	})

	c.Specify("[RedisDsl][HASH_DECRBYFLOAT]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}
		value, err := dsl.HASH_DECRBYFLOAT("Hash Name", "Bob", 1000.0)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, float64(-1000))

		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123.456")
		value, err = dsl.HASH_DECRBYFLOAT("Hash Name", "Bob", 1000.0)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(value, gospec.Equals, float64(123.456-1000))
	})

	//
	// ==================================================
	//
	// Common Redis GET "X" Operations:
	// Short Hand Conversion for the above GET/MGET operations:
	//
	// ==================================================
	//

	c.Specify("[RedisDsl][GET]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		reply := dsl.GET("Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply.Type, gospec.Equals, redis.NilReply)

		// Cache Hit String:
		server.Connection().Cmd("SET", "Bob", "Gary")
		reply = dsl.GET("Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		ptr, err := ReplyToStringPtr(reply)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, "Gary")
	})

	c.Specify("[RedisDsl][GET_STRING]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptr, err := dsl.GET_STRING("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		// Cache Hit:
		server.Connection().Cmd("SET", "Bob", "Gary")
		ptr, err = dsl.GET_STRING("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, "Gary")
	})

	c.Specify("[RedisDsl][GET_INT64]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptr, err := dsl.GET_INT64("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		// Cache Hit:
		server.Connection().Cmd("SET", "Bob", "123")
		ptr, err = dsl.GET_INT64("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, int64(123))
	})

	c.Specify("[RedisDsl][GET_FLOAT64]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptr, err := dsl.GET_FLOAT64("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		// Cache Hit:
		server.Connection().Cmd("SET", "Bob", "123.456")
		ptr, err = dsl.GET_FLOAT64("Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, float64(123.456))
	})

	c.Specify("[RedisDsl][MGET]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		reply := dsl.MGET("Bob", "Gary")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)
		c.Expect(len(reply.Elems), gospec.Equals, 2)
		c.Expect(reply.Elems[0].Type, gospec.Equals, redis.NilReply)
		c.Expect(reply.Elems[1].Type, gospec.Equals, redis.NilReply)

		// Cache Hit String:
		server.Connection().Cmd("SET", "Bob", "Hit Bob")
		reply = dsl.MGET("Bob", "Gary")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(len(reply.Elems), gospec.Equals, 2)
		c.Expect(reply.Elems[0].Type, gospec.Equals, redis.BulkReply)
		c.Expect(reply.Elems[1].Type, gospec.Equals, redis.NilReply)

		ptrs, err := ReplyToStringPtrs(reply)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])
		c.Expect(*ptrs[0], gospec.Equals, "Hit Bob")

		// Reverse the order and verify
		reply = dsl.MGET("Gary", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(len(reply.Elems), gospec.Equals, 2)
		c.Expect(reply.Elems[0].Type, gospec.Equals, redis.NilReply)
		c.Expect(reply.Elems[1].Type, gospec.Equals, redis.BulkReply)

		ptrs, err = ReplyToStringPtrs(reply)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(*ptrs[1], gospec.Equals, "Hit Bob")
	})

	c.Specify("[RedisDsl][MGET_STRINGS]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptrs, err := dsl.MGET_STRINGS("Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])

		// Cache Hit:
		server.Connection().Cmd("SET", "Bob", "Hit Bob")
		server.Connection().Cmd("SET", "Gary", "Hit Gary")
		ptrs, err = dsl.MGET_STRINGS("Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(*ptrs[0], gospec.Equals, "Hit Bob")
		c.Expect(*ptrs[1], gospec.Equals, "Hit Gary")
	})

	c.Specify("[RedisDsl][MGET_INT64S]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptrs, err := dsl.MGET_INT64S("Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])

		// Cache Hit:
		server.Connection().Cmd("SET", "Bob", "123")
		server.Connection().Cmd("SET", "Gary", "456")
		ptrs, err = dsl.MGET_INT64S("Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(*ptrs[0], gospec.Equals, int64(123))
		c.Expect(*ptrs[1], gospec.Equals, int64(456))
	})

	c.Specify("[RedisDsl][MGET_FLOAT64S]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptrs, err := dsl.MGET_FLOAT64S("Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])

		// Cache Hit:
		server.Connection().Cmd("SET", "Bob", "123.456")
		server.Connection().Cmd("SET", "Gary", "456.789")
		ptrs, err = dsl.MGET_FLOAT64S("Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(*ptrs[0], gospec.Equals, float64(123.456))
		c.Expect(*ptrs[1], gospec.Equals, float64(456.789))
	})

	//
	// ==================================================
	//
	// Common Redis GET "X" Operations:
	// Short Hand Conversion for the above HASH_GET/HASH_MGET operations:
	//
	// ==================================================
	//

	c.Specify("[RedisDsl][HASH_GET]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		reply := dsl.HASH_GET("Hash Name", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply.Type, gospec.Equals, redis.NilReply)

		// Cache Hit String:
		server.Connection().Cmd("HSET", "Hash Name", "Bob", "Gary")
		reply = dsl.HASH_GET("Hash Name", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply.Type, gospec.Equals, redis.BulkReply)

		ptr, err := ReplyToStringPtr(reply)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, "Gary")
	})

	c.Specify("[RedisDsl][HASH_GET_STRING]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptr, err := dsl.HASH_GET_STRING("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		// Cache Hit:
		server.Connection().Cmd("HSET", "Hash Name", "Bob", "Gary")
		ptr, err = dsl.HASH_GET_STRING("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, "Gary")
	})

	c.Specify("[RedisDsl][HASH_GET_INT64]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptr, err := dsl.HASH_GET_INT64("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		// Cache Hit:
		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123")
		ptr, err = dsl.HASH_GET_INT64("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, int64(123))
	})

	c.Specify("[RedisDsl][HASH_GET_FLOAT64]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptr, err := dsl.HASH_GET_FLOAT64("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		// Cache Hit:
		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123.456")
		ptr, err = dsl.HASH_GET_FLOAT64("Hash Name", "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, float64(123.456))
	})

	c.Specify("[RedisDsl][HASH_MGET]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		reply := dsl.HASH_MGET("Hash Name", "Bob", "Gary")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(reply.Type, gospec.Equals, redis.MultiReply)
		c.Expect(len(reply.Elems), gospec.Equals, 2)
		c.Expect(reply.Elems[0].Type, gospec.Equals, redis.NilReply)
		c.Expect(reply.Elems[1].Type, gospec.Equals, redis.NilReply)

		// Cache Hit String:
		server.Connection().Cmd("HSET", "Hash Name", "Bob", "Hit Bob")
		reply = dsl.HASH_MGET("Hash Name", "Bob", "Gary")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(len(reply.Elems), gospec.Equals, 2)
		c.Expect(reply.Elems[0].Type, gospec.Equals, redis.BulkReply)
		c.Expect(reply.Elems[1].Type, gospec.Equals, redis.NilReply)

		ptrs, err := ReplyToStringPtrs(reply)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])
		c.Expect(*ptrs[0], gospec.Equals, "Hit Bob")

		// Reverse the order and verify
		reply = dsl.HASH_MGET("Hash Name", "Gary", "Bob")
		c.Expect(reply.Err, gospec.Equals, nil)
		c.Expect(len(reply.Elems), gospec.Equals, 2)
		c.Expect(reply.Elems[0].Type, gospec.Equals, redis.NilReply)
		c.Expect(reply.Elems[1].Type, gospec.Equals, redis.BulkReply)

		ptrs, err = ReplyToStringPtrs(reply)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(*ptrs[1], gospec.Equals, "Hit Bob")
	})

	c.Specify("[RedisDsl][HASH_MGET_STRINGS]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptrs, err := dsl.HASH_MGET_STRINGS("Hash Name", "Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])

		// Cache Hit:
		server.Connection().Cmd("HSET", "Hash Name", "Bob", "Hit Bob")
		server.Connection().Cmd("HSET", "Hash Name", "Gary", "Hit Gary")
		ptrs, err = dsl.HASH_MGET_STRINGS("Hash Name", "Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(*ptrs[0], gospec.Equals, "Hit Bob")
		c.Expect(*ptrs[1], gospec.Equals, "Hit Gary")
	})

	c.Specify("[RedisDsl][HASH_MGET_INT64S]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptrs, err := dsl.HASH_MGET_INT64S("Hash Name", "Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])

		// Cache Hit:
		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123")
		server.Connection().Cmd("HSET", "Hash Name", "Gary", "456")
		ptrs, err = dsl.HASH_MGET_INT64S("Hash Name", "Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(*ptrs[0], gospec.Equals, int64(123))
		c.Expect(*ptrs[1], gospec.Equals, int64(456))
	})

	c.Specify("[RedisDsl][HASH_MGET_FLOAT64S]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptrs, err := dsl.HASH_MGET_FLOAT64S("Hash Name", "Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])

		// Cache Hit:
		server.Connection().Cmd("HSET", "Hash Name", "Bob", "123.456")
		server.Connection().Cmd("HSET", "Hash Name", "Gary", "456.789")
		ptrs, err = dsl.HASH_MGET_FLOAT64S("Hash Name", "Bob", "Gary")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 2)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(*ptrs[0], gospec.Equals, float64(123.456))
		c.Expect(*ptrs[1], gospec.Equals, float64(456.789))
	})

	//
	// ==================================================
	//
	// Common Redis GET "X" Operations:
	// Short Hand Conversion for the above HASHES_GET/HASHES_MGET operations:
	//
	// ==================================================
	//

	c.Specify("[RedisDsl][HASHES_GET]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		replys := dsl.HASHES_GET([]string{"Hash A", "Hash B", "Hash C"}, "Bob")
		c.Expect(len(replys), gospec.Equals, 3)
		c.Expect(replys[0].Err, gospec.Equals, nil)
		c.Expect(replys[0].Type, gospec.Equals, redis.NilReply)
		c.Expect(replys[1].Err, gospec.Equals, nil)
		c.Expect(replys[1].Type, gospec.Equals, redis.NilReply)
		c.Expect(replys[2].Err, gospec.Equals, nil)
		c.Expect(replys[2].Type, gospec.Equals, redis.NilReply)

		// Cache Hit String:
		server.Connection().Cmd("HSET", "Hash A", "Bob", "Hit Bob A")
		server.Connection().Cmd("HSET", "Hash B", "Bob", "Hit Bob B")
		server.Connection().Cmd("HSET", "Hash C", "Bob", "Hit Bob C")
		replys = dsl.HASHES_GET([]string{"Hash A", "Hash B", "Hash C"}, "Bob")
		c.Expect(len(replys), gospec.Equals, 3)
		c.Expect(replys[0].Err, gospec.Equals, nil)
		c.Expect(replys[0].Type, gospec.Equals, redis.BulkReply)
		c.Expect(replys[1].Err, gospec.Equals, nil)
		c.Expect(replys[1].Type, gospec.Equals, redis.BulkReply)
		c.Expect(replys[2].Err, gospec.Equals, nil)
		c.Expect(replys[2].Type, gospec.Equals, redis.BulkReply)

		ptr, err := ReplyToStringPtr(replys[0])
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, "Hit Bob A")

		ptr, err = ReplyToStringPtr(replys[1])
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, "Hit Bob B")

		ptr, err = ReplyToStringPtr(replys[2])
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
		c.Expect(*ptr, gospec.Equals, "Hit Bob C")
	})

	c.Specify("[RedisDsl][HASHES_GET_STRING]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptrs, err := dsl.HASHES_GET_STRING([]string{"Hash A", "Hash B", "Hash C"}, "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 3)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])
		c.Expect(ptrs[2], gospec.Satisfies, nil == ptrs[2])

		// Cache Hit:
		server.Connection().Cmd("HSET", "Hash A", "Bob", "Hit Bob A")
		server.Connection().Cmd("HSET", "Hash B", "Bob", "Hit Bob B")
		server.Connection().Cmd("HSET", "Hash C", "Bob", "Hit Bob C")
		ptrs, err = dsl.HASHES_GET_STRING([]string{"Hash A", "Hash B", "Hash C"}, "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 3)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(ptrs[2], gospec.Satisfies, nil != ptrs[2])
		c.Expect(*ptrs[0], gospec.Equals, "Hit Bob A")
		c.Expect(*ptrs[1], gospec.Equals, "Hit Bob B")
		c.Expect(*ptrs[2], gospec.Equals, "Hit Bob C")
	})

	c.Specify("[RedisDsl][HASHES_GET_INT64]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptrs, err := dsl.HASHES_GET_INT64([]string{"Hash A", "Hash B", "Hash C"}, "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 3)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])
		c.Expect(ptrs[2], gospec.Satisfies, nil == ptrs[2])

		// Cache Hit:
		server.Connection().Cmd("HSET", "Hash A", "Bob", "123")
		server.Connection().Cmd("HSET", "Hash B", "Bob", "456")
		server.Connection().Cmd("HSET", "Hash C", "Bob", "789")
		ptrs, err = dsl.HASHES_GET_INT64([]string{"Hash A", "Hash B", "Hash C"}, "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 3)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(ptrs[2], gospec.Satisfies, nil != ptrs[2])
		c.Expect(*ptrs[0], gospec.Equals, int64(123))
		c.Expect(*ptrs[1], gospec.Equals, int64(456))
		c.Expect(*ptrs[2], gospec.Equals, int64(789))
	})

	c.Specify("[RedisDsl][HASHES_GET_FLOAT64]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		// Cache Miss:
		ptrs, err := dsl.HASHES_GET_FLOAT64([]string{"Hash A", "Hash B", "Hash C"}, "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 3)
		c.Expect(ptrs[0], gospec.Satisfies, nil == ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil == ptrs[1])
		c.Expect(ptrs[2], gospec.Satisfies, nil == ptrs[2])

		// Cache Hit:
		server.Connection().Cmd("HSET", "Hash A", "Bob", "123.1")
		server.Connection().Cmd("HSET", "Hash B", "Bob", "456.2")
		server.Connection().Cmd("HSET", "Hash C", "Bob", "789.3")
		ptrs, err = dsl.HASHES_GET_FLOAT64([]string{"Hash A", "Hash B", "Hash C"}, "Bob")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(len(ptrs), gospec.Equals, 3)
		c.Expect(ptrs[0], gospec.Satisfies, nil != ptrs[0])
		c.Expect(ptrs[1], gospec.Satisfies, nil != ptrs[1])
		c.Expect(ptrs[2], gospec.Satisfies, nil != ptrs[2])
		c.Expect(*ptrs[0], gospec.Equals, float64(123.1))
		c.Expect(*ptrs[1], gospec.Equals, float64(456.2))
		c.Expect(*ptrs[2], gospec.Equals, float64(789.3))
	})

	c.Specify("[RedisDsl][HASHES_MGET]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		hash_names := []string{"Hash A", "Hash B", "Hash Miss"}
		hash_fields := []string{"Bob", "Gary", "George", "Fred"}

		// Cache Miss:
		replys := dsl.HASHES_MGET(hash_names, hash_fields...)
		c.Expect(len(replys), gospec.Equals, 3)
		for _, reply := range replys {
			c.Expect(reply.Err, gospec.Equals, nil)
			c.Expect(reply.Type, gospec.Equals, redis.MultiReply)
			c.Expect(len(reply.Elems), gospec.Equals, 4)
			for _, elem := range reply.Elems {
				c.Expect(elem.Err, gospec.Equals, nil)
				c.Expect(elem.Type, gospec.Equals, redis.NilReply)
			}
		}

		// Cache Hit String:
		server.Connection().Cmd("HMSET", "Hash A", "Bob", "Hit Bob A", "Gary", "Hit Gary A", "George", "Hit George A", "Fred", "Hit Fred A")
		server.Connection().Cmd("HMSET", "Hash B", "Bob", "Hit Bob B", "Gary", "Hit Gary B", "George", "Hit George B", "Fred", "Hit Fred B")
		server.Connection().Cmd("HMSET", "Hash C", "Bob", "Hit Bob C", "Gary", "Hit Gary C", "George", "Hit George C", "Fred", "Hit Fred C")
		replys = dsl.HASHES_MGET([]string{"Hash A", "Hash B", "Hash C"}, hash_fields...)
		c.Expect(len(replys), gospec.Equals, 3)
		for hash_i, reply := range replys {
			c.Expect(reply.Err, gospec.Equals, nil)
			c.Expect(reply.Type, gospec.Equals, redis.MultiReply)
			c.Expect(len(reply.Elems), gospec.Equals, 4)
			for field_i, elem := range reply.Elems {
				c.Expect(elem.Err, gospec.Equals, nil)
				c.Expect(elem.Type, gospec.Equals, redis.BulkReply)

				ptr, err := ReplyToStringPtr(elem)
				c.Expect(err, gospec.Equals, nil)
				c.Expect(ptr, gospec.Satisfies, nil != ptr)

				c.Expect(*ptr, gospec.Equals, fmt.Sprintf("Hit %s %s", hash_fields[field_i], []string{"A", "B", "C"}[hash_i]))
			}
		}

	})

	c.Specify("[RedisDsl][HASHES_MGET_STRINGS]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		hash_names := []string{"Hash A", "Hash B", "Hash Miss"}
		hash_fields := []string{"Bob", "Gary", "George", "Fred"}

		// Cache Miss:
		ptrs_s, ptrs_s_err := dsl.HASHES_MGET_STRINGS(hash_names, hash_fields...)
		c.Expect(ptrs_s_err, gospec.Equals, nil)
		c.Expect(len(ptrs_s), gospec.Equals, 3)
		for _, ptrs := range ptrs_s {
			c.Expect(len(ptrs), gospec.Equals, 4)
			for _, ptr := range ptrs {
				c.Expect(ptr, gospec.Satisfies, nil == ptr)
			}
		}

		// Cache Hit String:
		server.Connection().Cmd("HMSET", "Hash A", "Bob", "Hit Bob A", "Gary", "Hit Gary A", "George", "Hit George A", "Fred", "Hit Fred A")
		server.Connection().Cmd("HMSET", "Hash B", "Bob", "Hit Bob B", "Gary", "Hit Gary B", "George", "Hit George B", "Fred", "Hit Fred B")
		server.Connection().Cmd("HMSET", "Hash C", "Bob", "Hit Bob C", "Gary", "Hit Gary C", "George", "Hit George C", "Fred", "Hit Fred C")
		ptrs_s, ptrs_s_err = dsl.HASHES_MGET_STRINGS([]string{"Hash A", "Hash B", "Hash C"}, hash_fields...)
		c.Expect(ptrs_s_err, gospec.Equals, nil)
		c.Expect(len(ptrs_s), gospec.Equals, 3)
		for hash_i, ptrs := range ptrs_s {
			c.Expect(len(ptrs), gospec.Equals, 4)
			for field_i, ptr := range ptrs {
				c.Expect(ptr, gospec.Satisfies, nil != ptr)
				c.Expect(*ptr, gospec.Equals, fmt.Sprintf("Hit %s %s", hash_fields[field_i], []string{"A", "B", "C"}[hash_i]))
			}
		}
	})

	c.Specify("[RedisDsl][HASHES_MGET_INT64S]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		hash_names := []string{"Hash A", "Hash B", "Hash Miss"}
		hash_fields := []string{"Bob", "Gary", "George", "Fred"}

		// Cache Miss:
		ptrs_s, ptrs_s_err := dsl.HASHES_MGET_INT64S(hash_names, hash_fields...)
		c.Expect(ptrs_s_err, gospec.Equals, nil)
		c.Expect(len(ptrs_s), gospec.Equals, 3)
		for _, ptrs := range ptrs_s {
			c.Expect(len(ptrs), gospec.Equals, 4)
			for _, ptr := range ptrs {
				c.Expect(ptr, gospec.Satisfies, nil == ptr)
			}
		}

		// Cache Hit String:
		server.Connection().Cmd("HMSET", "Hash A", "Bob", 123, "Gary", 456, "George", 789, "Fred", 555)
		server.Connection().Cmd("HMSET", "Hash B", "Bob", 1230, "Gary", 4560, "George", 7890, "Fred", 5550)
		server.Connection().Cmd("HMSET", "Hash C", "Bob", 12300, "Gary", 45600, "George", 78900, "Fred", 55500)
		ptrs_s, ptrs_s_err = dsl.HASHES_MGET_INT64S([]string{"Hash A", "Hash B", "Hash C"}, hash_fields...)
		c.Expect(ptrs_s_err, gospec.Equals, nil)
		c.Expect(len(ptrs_s), gospec.Equals, 3)
		for hash_i, ptrs := range ptrs_s {
			c.Expect(len(ptrs), gospec.Equals, 4)
			for field_i, ptr := range ptrs {
				c.Expect(ptr, gospec.Satisfies, nil != ptr)
				c.Expect(*ptr, gospec.Equals, int64(math.Pow10(hash_i))*[]int64{123, 456, 789, 555}[field_i])
			}
		}
	})

	c.Specify("[RedisDsl][HASHES_MGET_FLOAT64S]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		dsl := RedisDsl{server.Connection()}

		hash_names := []string{"Hash A", "Hash B", "Hash Miss"}
		hash_fields := []string{"Bob", "Gary", "George", "Fred"}

		// Cache Miss:
		ptrs_s, ptrs_s_err := dsl.HASHES_MGET_FLOAT64S(hash_names, hash_fields...)
		c.Expect(ptrs_s_err, gospec.Equals, nil)
		c.Expect(len(ptrs_s), gospec.Equals, 3)
		for _, ptrs := range ptrs_s {
			c.Expect(len(ptrs), gospec.Equals, 4)
			for _, ptr := range ptrs {
				c.Expect(ptr, gospec.Satisfies, nil == ptr)
			}
		}

		// Cache Hit String:
		server.Connection().Cmd("HMSET", "Hash A", "Bob", 123.1, "Gary", 456.2, "George", 789.3, "Fred", 555.4)
		server.Connection().Cmd("HMSET", "Hash B", "Bob", 1231, "Gary", 4562, "George", 7893, "Fred", 5554)
		server.Connection().Cmd("HMSET", "Hash C", "Bob", 12310, "Gary", 45620, "George", 78930, "Fred", 55540)
		ptrs_s, ptrs_s_err = dsl.HASHES_MGET_FLOAT64S([]string{"Hash A", "Hash B", "Hash C"}, hash_fields...)
		c.Expect(ptrs_s_err, gospec.Equals, nil)
		c.Expect(len(ptrs_s), gospec.Equals, 3)
		for hash_i, ptrs := range ptrs_s {
			c.Expect(len(ptrs), gospec.Equals, 4)
			for field_i, ptr := range ptrs {
				c.Expect(ptr, gospec.Satisfies, nil != ptr)
				c.Expect(*ptr, gospec.Equals, math.Pow10(hash_i)*[]float64{123.1, 456.2, 789.3, 555.4}[field_i])
			}
		}

	})

}
