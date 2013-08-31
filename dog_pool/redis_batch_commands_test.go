package dog_pool

import "fmt"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"

//
// NOTE: Use differient ports for each test!
//       gospec runs the specs in parallel!
//
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
	c.Specify("[RedisBatchCommands] BatchCommands batches commands w/o Multi Exec or transactions", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Starts off closed ...
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		commands := make([]*RedisBatchCommand, 4)
		commands[0] = &RedisBatchCommand{Cmd: "SET", Args: []string{"BOB", "1"}}
		commands[1] = &RedisBatchCommand{Cmd: "GET", Args: []string{"BOB"}}
		commands[2] = &RedisBatchCommand{Cmd: "DEL", Args: []string{"BOB"}}
		commands[3] = &RedisBatchCommand{Cmd: "GET", Args: []string{"MISS"}}

		// Cast the commands slice to a RedisBatchCommands object
		var batch RedisBatchCommands
		batch = commands

		// Execute the commands
		c.Expect(batch.ExecuteBatch(server.Connection()), gospec.Equals, nil)
	})

}

//
//
// ==================================================
//
// We are looking for significant degredation as we increase the number of bit operations
//
// ==================================================
//

//
// Benchmark Bit Operation NOT on 5x keys
//
func Benchmark_BatchCommands_BitOp_And_5x1(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartRedisServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	keys_all := [10]string{}
	keys_bob := [10]string{}
	keys_not_bob := [10]string{}
	keys_gary := [10]string{}
	keys_not_gary := [10]string{}
	for i := 0; i < 10; i++ {
		keys_all[i] = fmt.Sprintf("ALL:%d", i)
		keys_bob[i] = fmt.Sprintf("BOB:%d", i)
		keys_not_bob[i] = fmt.Sprintf("Not-BOB:%d", i)
		keys_gary[i] = fmt.Sprintf("GARY:%d", i)
		keys_not_gary[i] = fmt.Sprintf("Not-GARY:%d", i)
	}

	for i := 0; i < 1024; i++ {
		for j := 0; j < 10; j++ {
			server.Connection().Cmd("SETBIT", keys_all[j], i, true)
			server.Connection().Cmd("SETBIT", keys_bob[j], i, i%2 == 0)
			server.Connection().Cmd("SETBIT", keys_not_bob[j], i, i%2 == 1)
			server.Connection().Cmd("SETBIT", keys_gary[j], i, i%4 == 0)
			server.Connection().Cmd("SETBIT", keys_not_gary[j], i, i%4 == 0)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmds := RedisBatchCommands{
			MakeBitopAnd("ALL", keys_all[:]),
			MakeBitopAnd("BOB", keys_bob[:]),
			MakeBitopAnd("Not-BOB", keys_not_bob[:]),
			MakeBitopAnd("GARY", keys_gary[:]),
			MakeBitopAnd("Not-GARY", keys_not_gary[:]),
		}

		cmds.ExecuteBatch(server.Connection())
	}
}

//
// Benchmark Bit Operation NOT on 10x keys
//
func Benchmark_BatchCommands_BitOp_And_10x(b *testing.B) {
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
		cmds := RedisBatchCommands{
			MakeBitopAnd("ALL:0", keys_all[0][:]),
			MakeBitopAnd("ALL:1", keys_all[1][:]),
			MakeBitopAnd("BOB:0", keys_bob[0][:]),
			MakeBitopAnd("BOB:1", keys_bob[1][:]),
			MakeBitopAnd("Not-BOB:0", keys_not_bob[0][:]),
			MakeBitopAnd("Not-BOB:1", keys_not_bob[1][:]),
			MakeBitopAnd("GARY:0", keys_gary[0][:]),
			MakeBitopAnd("GARY:1", keys_gary[1][:]),
			MakeBitopAnd("Not-GARY:0", keys_not_gary[0][:]),
			MakeBitopAnd("Not-GARY:1", keys_not_gary[1][:]),
		}

		cmds.ExecuteBatch(server.Connection())
	}
}

//
// Benchmark Bit Operation NOT on 10x keys
//
func Benchmark_BatchCommands_BitOp_AndGet_10x(b *testing.B) {
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
		cmds := RedisBatchCommands{
			MakeBitopAnd("ALL:0", keys_all[0][:]),
			MakeBitopAnd("ALL:1", keys_all[1][:]),
			MakeBitopAnd("BOB:0", keys_bob[0][:]),
			MakeBitopAnd("BOB:1", keys_bob[1][:]),
			MakeBitopAnd("Not-BOB:0", keys_not_bob[0][:]),
			MakeBitopAnd("Not-BOB:1", keys_not_bob[1][:]),
			MakeBitopAnd("GARY:0", keys_gary[0][:]),
			MakeBitopAnd("GARY:1", keys_gary[1][:]),
			MakeBitopAnd("Not-GARY:0", keys_not_gary[0][:]),
			MakeBitopAnd("Not-GARY:1", keys_not_gary[1][:]),
		}
		for i, max := 0, len(cmds); i < max; i++ {
			cmds = append(cmds, MakeGet(cmds[i].Args[0]))
		}

		cmds.ExecuteBatch(server.Connection())
	}
}

//
// Benchmark Bit Operation A & !B on 10x keys
//
func Benchmark_BatchCommands_BitOp_ComplementSet(b *testing.B) {
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
		cmds := RedisBatchCommands{
			MakeBitopAnd("ALL:0", keys_all[0][:]),
			MakeBitopAnd("ALL:1", keys_all[1][:]),
			MakeBitopAnd("BOB:0", keys_bob[0][:]),
			MakeBitopAnd("BOB:1", keys_bob[1][:]),
			MakeBitopAnd("Not-BOB:0", keys_not_bob[0][:]),
			MakeBitopAnd("Not-BOB:1", keys_not_bob[1][:]),
			MakeBitopAnd("GARY:0", keys_gary[0][:]),
			MakeBitopAnd("GARY:1", keys_gary[1][:]),
			MakeBitopAnd("Not-GARY:0", keys_not_gary[0][:]),
			MakeBitopOr("Not-GARY:1", keys_not_gary[1][:]),
			MakeBitopNot("Not-GARY:1", "Not-GARY:1"),
			MakeBitopAnd("Complement", []string{"Not-GARY:1", "ALL:0", "ALL:1", "BOB:0", "BOB:1", "Not-BOB:0", "Not-BOB:1", "GARY:0", "GARY:1", "Not-GARY:0"}),
		}

		for _, key := range []string{"Complement", "Not-GARY:1", "ALL:0", "ALL:1", "BOB:0", "BOB:1", "Not-BOB:0", "Not-BOB:1", "GARY:0", "GARY:1", "Not-GARY:0"} {
			cmds = append(cmds, MakeBitCount(key))
		}
		cmds = append(cmds, MakeGet("Complement"))

		cmds.ExecuteBatch(server.Connection())
	}
}
