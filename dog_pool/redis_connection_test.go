package dog_pool

import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"
import dog_pool_utils "./utils"

//
// NOTE: Use differient ports for each test!
//       gospec runs the specs in parallel!
//
func TestRedisConnectionSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.AddSpec(RedisConnectionSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisConnectionSpecs(c gospec.Context) {
	var redis_connection_logger = log4go.NewDefaultLogger(log4go.CRITICAL)

	c.Specify("[RedisConnection] New connection is not open", func() {
		connection := RedisConnection{Url: "127.0.0.1:6990", Logger: &redis_connection_logger}
		defer connection.Close()

		open := connection.IsOpen()
		closed := connection.IsClosed()

		// Should be opposite of each other:
		c.Expect(open, gospec.Equals, false)
		c.Expect(closed, gospec.Equals, true)
		c.Expect(closed, gospec.Satisfies, open != closed)
	})

	c.Specify("[RedisConnection] Opening connection to Invalid Host/Port has errors", func() {
		connection := RedisConnection{Url: "127.0.0.1:6991", Logger: &redis_connection_logger}
		defer connection.Close()

		// The server is not running ...
		// This should return an error
		err := connection.Open()
		c.Expect(err, gospec.Satisfies, err != nil)

		closed := connection.IsClosed()
		c.Expect(closed, gospec.Equals, true)
	})

	c.Specify("[RedisConnection] Opening connection to Valid Host/Port has no errors", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := dog_pool_utils.StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		c.Expect(server.Connection().Open(), gospec.Equals, nil)
		c.Expect(server.Connection().IsOpen(), gospec.Equals, true)
		c.Expect(server.Connection().IsClosed(), gospec.Equals, false)
	})

	c.Specify("[RedisConnection] Ping (-->Cmd-->Append+GetReply) (re-)opens the connection automatically", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := dog_pool_utils.StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Starts off closed ...
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		// Ping the server
		// Should now be open
		c.Expect(server.Connection().Ping(), gospec.Equals, nil)
		c.Expect(server.Connection().IsOpen(), gospec.Equals, true)
		c.Expect(server.Connection().IsClosed(), gospec.Equals, false)

		// Close the connection
		c.Expect(server.Connection().Close(), gospec.Equals, nil)
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		// Ping the server again
		// Should now be open again
		c.Expect(server.Connection().Ping(), gospec.Equals, nil)
		c.Expect(server.Connection().IsOpen(), gospec.Equals, true)
		c.Expect(server.Connection().IsClosed(), gospec.Equals, false)
	})

	// c.Specify("[RedisConnection] BatchCommands batches commands w/o Multi Exec or transactions", func() {
	// 	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	// 	server, err := dog_pool_utils.StartRedisServer(&logger)
	// 	if nil != err {
	// 		panic(err)
	// 	}
	// 	defer server.Close()
	//
	// 	// Starts off closed ...
	// 	c.Expect(server.Connection().IsClosed(), gospec.Equals, true)
	//
	// 	commands := make([]*RedisBatchCommand, 4)
	// 	commands[0] = &RedisBatchCommand{Cmd: "SET", Args: []string{"BOB", "1"}}
	// 	commands[1] = &RedisBatchCommand{Cmd: "GET", Args: []string{"BOB"}}
	// 	commands[2] = &RedisBatchCommand{Cmd: "DEL", Args: []string{"BOB"}}
	// 	commands[3] = &RedisBatchCommand{Cmd: "GET", Args: []string{"MISS"}}
	//
	// 	// Execute the commands
	// 	// Should now be open
	// 	c.Expect(server.Connection().BatchCommands(commands), gospec.Equals, nil)
	// 	c.Expect(server.Connection().IsOpen(), gospec.Equals, true)
	// })

}

func Benchmark_RedisConnection_Get(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool_utils.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	server.Connection().Cmd("SET", "BOB", "Hello")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Cmd("GET", "BOB")
	}
}

func Benchmark_RedisConnection_Set(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool_utils.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Cmd("SET", "BOB", "Hello")
	}
}

func Benchmark_RedisConnection_Del_CacheMiss(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool_utils.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Cmd("DEL", "BOB", "Hello", "World", "GARY", "THE", "SNAIL")
	}
}

func Benchmark_RedisConnection_SetGet(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool_utils.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Cmd("SET", "BOB", "Hello")
		server.Connection().Cmd("GET", "BOB")
	}
}

func Benchmark_RedisConnection_BitOp_Or(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool_utils.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	for i := 0; i < 1024; i++ {
		server.Connection().Cmd("SETBIT", "ALL", i, true)
		server.Connection().Cmd("SETBIT", "BOB", i, i%2 == 0)
		server.Connection().Cmd("SETBIT", "Not-BOB", i, i%2 == 1)
		server.Connection().Cmd("SETBIT", "GARY", i, i%4 == 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Cmd("BITOP", "OR", "ALL", "BOB", "Not-BOB", "GARY", "Cache-Miss")
	}
}

func Benchmark_RedisConnection_Bit_Get(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := dog_pool_utils.StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	for i := 0; i < 1024; i++ {
		server.Connection().Cmd("SETBIT", "ALL", i, true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Cmd("GET", "ALL")
	}
}
