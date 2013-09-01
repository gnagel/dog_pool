package dog_pool

import "fmt"
import "strings"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"

//
// NOTE: Use differient ports for each test!
//       gospec runs the specs in parallel!
//
func TestRedisConnectionSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisConnectionSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisConnectionSpecs(c gospec.Context) {
	var redis_connection_logger = log4go.NewDefaultLogger(log4go.CRITICAL)

	c.Specify("[RedisConnection] Clone a connection", func() {
		connection := &RedisConnection{Url: "127.0.0.1:6990", Id: "Bob", Logger: &redis_connection_logger}
		defer connection.Close()
		c.Expect(connection.IsOpen(), gospec.Equals, false)

		connection2 := connection.Clone()
		defer connection2.Close()
		c.Expect(connection2.IsOpen(), gospec.Equals, false)

		// Should be differient pointers
		c.Expect(connection2, gospec.Satisfies, connection != connection2)
	})

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
		server, err := StartRedisServer(&logger)
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
		server, err := StartRedisServer(&logger)
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

}

func Benchmark_RedisConnection_Get(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
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
	server, err := StartRedisServer(&logger)
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
	server, err := StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Cmd("DEL", "BOB", "Hello", "World", "GARY", "THE", "SNAIL")
	}
}

// Pre-format the commands as a single string, and compare the results to above
func Benchmark_RedisConnection_Del_CacheMiss_Str(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Cmd("DEL BOB Hello World GARY THE SNAIL")
	}
}

func Benchmark_RedisConnection_SetGet(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
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

func Benchmark_RedisConnection_Bit_Get(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
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

func Benchmark_RedisConnection_BitOp_And(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
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
		server.Connection().Cmd("BITOP", "AND", "ALL", "BOB", "Not-BOB", "GARY", "Cache-Miss")
	}
}

// Pre-format the commands as a single string, and compare the results to above
func Benchmark_RedisConnection_BitOp_And_Str(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
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
		server.Connection().Cmd("BITOP AND ALL BOB Not-BOB GARY Cache-Miss")
	}
}

func Benchmark_RedisConnection_BitOp_Or(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
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

// Pre-format the commands as a single string, and compare the results to above
func Benchmark_RedisConnection_BitOp_Or_Str(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
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
		server.Connection().Cmd("BITOP OR ALL BOB Not-BOB GARY Cache-Miss")
	}
}

func Benchmark_RedisConnection_BitOp_Not(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
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
		server.Connection().Cmd("BITOP", "NOT", "ALL", "BOB", "Not-BOB", "GARY", "Cache-Miss")
	}
}

//
// Benchmark Bit Operation A & !B on 10x keys
//
func Benchmark_RedisConnection_BitOp_ComplementSet(b *testing.B) {
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer := []string{}
		buffer = []string{"AND", "ALL:0"}
		buffer = append(buffer, keys_all[0]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"AND", "ALL:0"}
		buffer = append(buffer, keys_all[0]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"AND", "ALL:1"}
		buffer = append(buffer, keys_all[1]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"AND", "BOB:0"}
		buffer = append(buffer, keys_bob[0]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"AND", "BOB:1"}
		buffer = append(buffer, keys_bob[1]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"AND", "Not-BOB:0"}
		buffer = append(buffer, keys_not_bob[0]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"AND", "Not-BOB:1"}
		buffer = append(buffer, keys_not_bob[1]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"AND", "GARY:0"}
		buffer = append(buffer, keys_gary[0]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"AND", "GARY:1"}
		buffer = append(buffer, keys_gary[1]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"AND", "Not-GARY:0"}
		buffer = append(buffer, keys_not_gary[0]...)
		server.Connection().Cmd("BITOP", buffer)
		buffer = []string{"OR", "Not-GARY:1"}
		buffer = append(buffer, keys_not_gary[1]...)
		server.Connection().Cmd("BITOP", buffer)
		server.Connection().Cmd("BITOP", "NOT", "Not-GARY:1", "Not-GARY:1")
		server.Connection().Cmd("BITOP", "AND", "Complement", "Not-GARY:1", "ALL:0", "ALL:1", "BOB:0", "BOB:1", "Not-BOB:0", "Not-BOB:1", "GARY:0", "GARY:1", "Not-GARY:0")

		for _, key := range []string{"Complement", "Not-GARY:1", "ALL:0", "ALL:1", "BOB:0", "BOB:1", "Not-BOB:0", "Not-BOB:1", "GARY:0", "GARY:1", "Not-GARY:0"} {
			server.Connection().Cmd("BITCOUNT", key)
		}
		server.Connection().Cmd("GET", "Complement")
	}
}

// Pre-format the commands as a single string, and compare the results to above
func Benchmark_RedisConnection_BitOp_ComplementSet_Str(b *testing.B) {
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

	cmds := [11]string{}
	cmds[0] = fmt.Sprintf("BITOP AND ALL:0 %s", strings.Join(keys_all[0], " "))
	cmds[1] = fmt.Sprintf("BITOP AND ALL:1 %s", strings.Join(keys_all[1], " "))
	cmds[2] = fmt.Sprintf("BITOP AND BOB:0 %s", strings.Join(keys_bob[0], " "))
	cmds[3] = fmt.Sprintf("BITOP AND BOB:1 %s", strings.Join(keys_bob[1], " "))
	cmds[4] = fmt.Sprintf("BITOP AND Not-BOB:0 %s", strings.Join(keys_not_bob[0], " "))
	cmds[5] = fmt.Sprintf("BITOP AND Not-BOB:1 %s", strings.Join(keys_not_bob[1], " "))
	cmds[6] = fmt.Sprintf("BITOP AND GARY:0 %s", strings.Join(keys_gary[0], " "))
	cmds[7] = fmt.Sprintf("BITOP AND GARY:1 %s", strings.Join(keys_gary[1], " "))
	cmds[8] = fmt.Sprintf("BITOP AND Not-GARY:0 %s", strings.Join(keys_not_gary[0], " "))
	cmds[9] = fmt.Sprintf("BITOP AND Not-GARY:0 %s", strings.Join(keys_not_gary[0], " "))
	cmds[10] = fmt.Sprintf("BITOP OR Not-GARY:1 %s", strings.Join(keys_not_gary[1], " "))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, cmd := range cmds {
			server.Connection().Cmd(cmd)
		}
		server.Connection().Cmd("BITOP NOT Not-GARY:1 Not-GARY:1")
		server.Connection().Cmd("BITOP AND Complement Not-GARY:1 ALL:0 ALL:1 BOB:0 BOB:1 Not-BOB:0 Not-BOB:1 GARY:0 GARY:1 Not-GARY:0")

		server.Connection().Cmd("BITCOUNT Complement")
		server.Connection().Cmd("BITCOUNT Not-GARY:1")
		server.Connection().Cmd("BITCOUNT ALL:0")
		server.Connection().Cmd("BITCOUNT ALL:1")
		server.Connection().Cmd("BITCOUNT BOB:0")
		server.Connection().Cmd("BITCOUNT BOB:1")
		server.Connection().Cmd("BITCOUNT Not-BOB:0")
		server.Connection().Cmd("BITCOUNT Not-BOB:1")
		server.Connection().Cmd("BITCOUNT GARY:0")
		server.Connection().Cmd("BITCOUNT GARY:1")
		server.Connection().Cmd("BITCOUNT Not-GARY:0")

		server.Connection().Cmd("GET Complement")
	}
}
