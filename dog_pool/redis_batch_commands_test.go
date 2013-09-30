package dog_pool

import "fmt"
import "time"
import "github.com/RUNDSP/radix/redis"
import "github.com/alecthomas/log4go"

import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisBatchCommandsSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisBatchCommandsSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisBatchCommandsSpecs(c gospec.Context) {

	c.Specify("[RedisBatchCommands] PING Test", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommand("PING")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		str, _ := commands[0].Reply().Str()
		c.Expect(commands[0].Reply(), gospec.Satisfies, str == "PONG")
	})

	c.Specify("[RedisBatchCommands] Value Exists", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SET", "Bob", "123")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 2)
		commands[0] = MakeRedisBatchCommandExists("Bob")
		commands[1] = MakeRedisBatchCommandExists("George")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 1)

		ok, _ = commands[1].Reply().Int()
		c.Expect(commands[1].Reply(), gospec.Satisfies, ok == 0)
	})

	c.Specify("[RedisBatchCommands] Expire value", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SET", "Bob", "123")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandExpireIn("Bob", time.Duration(1)*time.Second)

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 1)

		// Sleep for 1.5 seconds
		time.Sleep(time.Duration(1500) * time.Millisecond)

		// Value has expired!
		reply := server.Connection().Cmd("GET", "Bob")
		c.Expect(reply, gospec.Satisfies, reply.Type == redis.NilReply)
	})

	c.Specify("[RedisBatchCommands] Delete values", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SET", "Bob", "123")
		server.Connection().Cmd("SET", "Gary", "456")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandDelete("Bob", "Gary", "George")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		// Deleted 2 keys:
		count, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, count == 2)
	})

	c.Specify("[RedisBatchCommands] Mget values", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SET", "Bob", "123")
		server.Connection().Cmd("SET", "Gary", "456")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandMget("Bob", "Gary", "George")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		// Cache Hit on 2x; Cache Miss on 1x
		bytes_array, _ := commands[0].Reply().ListBytes()
		c.Expect(commands[0].Reply(), gospec.Satisfies, len(bytes_array) == 3)
		c.Expect(commands[0].Reply(), gospec.Satisfies, string(bytes_array[0]) == "123")
		c.Expect(commands[0].Reply(), gospec.Satisfies, string(bytes_array[1]) == "456")
		c.Expect(commands[0].Reply(), gospec.Satisfies, len(bytes_array[2]) == 0)
	})

	c.Specify("[RedisBatchCommands] Get values", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SET", "Bob", "123")
		server.Connection().Cmd("SET", "Gary", "456")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 3)
		commands[0] = MakeRedisBatchCommandGet("Bob")
		commands[1] = MakeRedisBatchCommandGet("Gary")
		commands[2] = MakeRedisBatchCommandGet("George")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		// Cache Hit on 2x; Cache Miss on 1x
		str, _ := commands[0].Reply().Str()
		c.Expect(commands[0].Reply(), gospec.Satisfies, str == "123")

		str, _ = commands[1].Reply().Str()
		c.Expect(commands[1].Reply(), gospec.Satisfies, str == "456")

		str, _ = commands[2].Reply().Str()
		c.Expect(commands[2].Reply(), gospec.Satisfies, str == "")
	})

	c.Specify("[RedisBatchCommands] Set value", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandSet("Bob", []byte("123"))

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Str()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == "OK")
	})

	c.Specify("[RedisBatchCommands] Hash Value Exists", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("HSET", "Bob", "123", "")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 2)
		commands[0] = MakeRedisBatchCommandHashExists("Bob", "123")
		commands[1] = MakeRedisBatchCommandHashExists("Bob", "George")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 1)

		ok, _ = commands[1].Reply().Int()
		c.Expect(commands[1].Reply(), gospec.Satisfies, ok == 0)
	})

	c.Specify("[RedisBatchCommands] Hash Delete values", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("HSET", "Bob", "A", "123")

		str, _ := server.Connection().Cmd("HGET", "Bob", "A").Str()
		c.Expect(str, gospec.Equals, "123")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandHashDelete("Bob", "A")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		// Deleted 1 keys:
		count, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, count == 1)
	})

	c.Specify("[RedisBatchCommands] Hash Mget values", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("HSET", "A", "Bob", "123")
		server.Connection().Cmd("HSET", "A", "Gary", "456")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandHashMget("A", "Bob", "Gary", "George")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		// Cache Hit on 2x; Cache Miss on 1x
		bytes_array, _ := commands[0].Reply().ListBytes()
		c.Expect(commands[0].Reply(), gospec.Satisfies, len(bytes_array) == 3)
		c.Expect(commands[0].Reply(), gospec.Satisfies, string(bytes_array[0]) == "123")
		c.Expect(commands[0].Reply(), gospec.Satisfies, string(bytes_array[1]) == "456")
		c.Expect(commands[0].Reply(), gospec.Satisfies, len(bytes_array[2]) == 0)
	})

	c.Specify("[RedisBatchCommands] Hash Get values", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("HSET", "Bob", "A", "123")
		server.Connection().Cmd("HSET", "Gary", "A", "456")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 3)
		commands[0] = MakeRedisBatchCommandHashGet("Bob", "A")
		commands[1] = MakeRedisBatchCommandHashGet("Gary", "A")
		commands[2] = MakeRedisBatchCommandHashGet("George", "A")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		// Cache Hit on 2x; Cache Miss on 1x
		str, _ := commands[0].Reply().Str()
		c.Expect(commands[0].Reply(), gospec.Satisfies, str == "123")

		str, _ = commands[1].Reply().Str()
		c.Expect(commands[1].Reply(), gospec.Satisfies, str == "456")

		str, _ = commands[2].Reply().Str()
		c.Expect(commands[2].Reply(), gospec.Satisfies, str == "")
	})

	c.Specify("[RedisBatchCommands] Hash Set value", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandHashSet("Bob", "A", []byte("123"))

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 1)
	})

	c.Specify("[RedisBatchCommands] Hash IncrementBy value", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandHashIncrementBy("Bob", "A", 123)

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		// Returns the new hash value
		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 123)
	})

	c.Specify("[MakeRedisBatchCommand][Bitop][And] Makes command", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SETBIT", "Bob", "123", "1")
		server.Connection().Cmd("SETBIT", "Gary", "456", "1")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandBitopAnd("DEST", "Bob", "Gary")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 58)
	})

	c.Specify("[MakeRedisBatchCommand][Bitop][Or] Makes command", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SETBIT", "Bob", "123", "1")
		server.Connection().Cmd("SETBIT", "Gary", "456", "1")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandBitopOr("DEST", "Bob", "Gary")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 58)
	})

	c.Specify("[MakeRedisBatchCommand][Bitop][Not] Makes command", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SETBIT", "Bob", "123", "1")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandBitopNot("DEST", "Bob")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 16)
	})

	c.Specify("[MakeRedisBatchCommand][BitCount] Makes command", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SETBIT", "Bob", "123", "1")
		server.Connection().Cmd("SETBIT", "Bob", "456", "1")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandBitCount("Bob")

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 2)
	})

	c.Specify("[MakeRedisBatchCommand][BitCount] Makes command", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		server.Connection().Cmd("SETBIT", "Bob", "123", "1")

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 2)
		commands[0] = MakeRedisBatchCommandGetBit("Bob", 123)
		commands[1] = MakeRedisBatchCommandGetBit("Bob", 456)

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 1)

		ok, _ = commands[1].Reply().Int()
		c.Expect(commands[1].Reply(), gospec.Satisfies, ok == 0)
	})

	c.Specify("[MakeRedisBatchCommand][SetBit] Makes command", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		var commands RedisBatchCommands
		commands = make([]*RedisBatchCommand, 1)
		commands[0] = MakeRedisBatchCommandSetBit("Bob", 123, true)

		err = commands.ExecuteBatch(server.Connection())
		c.Expect(err, gospec.Equals, nil)

		ok, _ := commands[0].Reply().Int()
		c.Expect(commands[0].Reply(), gospec.Satisfies, ok == 0)
	})

}

//
//
// ==================================================
//
// We are comparing Benchmark_BitOp_ComplementSet_... for BatchCommands and RedisConnection
//
// Any serious performance degredation should be examined
//
// ==================================================
//

//
// Benchmark Bit Operation A & !B on 10x keys
//
func Benchmark_BitOp_ComplementSet_BatchCommands(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	keys_all := [][]string{make([]string, 10), make([]string, 10)}
	keys_bob := [][]string{make([]string, 10), make([]string, 10)}
	keys_not_bob := [][]string{make([]string, 10), make([]string, 10)}
	keys_gary := [][]string{make([]string, 10), make([]string, 10)}
	keys_not_gary := [][]string{make([]string, 10), make([]string, 10)}
	for i := 0; i < 10; i++ {
		for j := 0; j <= 1; j++ {
			keys_all[j][i] = fmt.Sprintf("ALL:%d:%d", j, i)
			keys_bob[j][i] = fmt.Sprintf("BOB:%d:%d", j, i)
			keys_not_bob[j][i] = fmt.Sprintf("Not-BOB:%d:%d", j, i)
			keys_gary[j][i] = fmt.Sprintf("GARY:%d:%d", j, i)
			keys_not_gary[j][i] = fmt.Sprintf("Not-GARY:%d:%d", j, i)
		}
	}

	for i := 0; i < 1024; i++ {
		for j := 0; j < 10; j++ {
			for k := 0; k <= 1; k++ {
				server.Connection().Cmd("SETBIT", keys_all[k][j], i, true)
				server.Connection().Cmd("SETBIT", keys_bob[k][j], i, i%2 == 0)
				server.Connection().Cmd("SETBIT", keys_not_bob[k][j], i, i%2 == 1)
				server.Connection().Cmd("SETBIT", keys_gary[k][j], i, i%4 == 0)
				server.Connection().Cmd("SETBIT", keys_not_gary[k][j], i, i%4 == 0)
			}
		}
	}

	cmds := RedisBatchCommands{
		MakeRedisBatchCommandBitopAnd("ALL:0", keys_all[0]...),
		MakeRedisBatchCommandBitopAnd("ALL:1", keys_all[1]...),
		MakeRedisBatchCommandBitopAnd("BOB:0", keys_bob[0]...),
		MakeRedisBatchCommandBitopAnd("BOB:1", keys_bob[1]...),
		MakeRedisBatchCommandBitopAnd("Not-BOB:0", keys_not_bob[0]...),
		MakeRedisBatchCommandBitopAnd("Not-BOB:1", keys_not_bob[1]...),
		MakeRedisBatchCommandBitopAnd("GARY:0", keys_gary[0]...),
		MakeRedisBatchCommandBitopAnd("GARY:1", keys_gary[1]...),
		MakeRedisBatchCommandBitopAnd("Not-GARY:0", keys_not_gary[0]...),
		MakeRedisBatchCommandBitopOr("Not-GARY:1", keys_not_gary[1]...),
		MakeRedisBatchCommandBitopNot("Not-GARY:1", "Not-GARY:1"),
		MakeRedisBatchCommandBitopAnd("Complement", "Not-GARY:1", "ALL:0", "ALL:1", "BOB:0", "BOB:1", "Not-BOB:0", "Not-BOB:1", "GARY:0", "GARY:1", "Not-GARY:0"),
	}
	for _, key := range []string{"Complement", "Not-GARY:1", "ALL:0", "ALL:1", "BOB:0", "BOB:1", "Not-BOB:0", "Not-BOB:1", "GARY:0", "GARY:1", "Not-GARY:0"} {
		cmds = append(cmds, MakeRedisBatchCommandBitCount(key))
	}
	cmds = append(cmds, MakeRedisBatchCommandGet("Complement"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmds.ExecuteBatch(server.Connection())
	}
}
