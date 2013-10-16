package dog_pool

import "fmt"
import "time"
import "runtime"
import "github.com/alecthomas/log4go"

import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisBatchQueueWorkerSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisBatchQueueWorkerSpecs)
	gospec.MainGoTest(r, t)
}

func RedisBatchQueueWorkerSpecs(c gospec.Context) {

	c.Specify("[RedisBatchQueueWorker][Make]", func() {
		ptr, err := makeRedisBatchQueueWorker(nil, nil, 0, nil)
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(err.Error(), gospec.Equals, "[redisBatchQueueWorker][Make] Nil logger!")
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		logger := &log4go.Logger{}
		ptr, err = makeRedisBatchQueueWorker(logger, nil, 0, nil)
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(err.Error(), gospec.Equals, "[redisBatchQueueWorker][Make] Nil redis connection!")
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		connection := &RedisConnection{}
		ptr, err = makeRedisBatchQueueWorker(logger, connection, 0, nil)
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(err.Error(), gospec.Equals, "[redisBatchQueueWorker][Make] Nil queue!")
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		queue := make(chan *RedisBatchCommand)
		defer close(queue)

		ptr, err = makeRedisBatchQueueWorker(logger, connection, 0, queue)
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(err.Error(), gospec.Equals, "[redisBatchQueueWorker][Make] BatchSize must be greater than 0!")
		c.Expect(ptr, gospec.Satisfies, nil == ptr)

		batch_size := uint(1)
		ptr, err = makeRedisBatchQueueWorker(logger, connection, batch_size, queue)
		c.Expect(err, gospec.Satisfies, nil == err)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)
	})

	c.Specify("[RedisBatchQueueWorker][runCommands]", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		queue := make(chan *RedisBatchCommand)
		defer close(queue)

		worker, worker_err := makeRedisBatchQueueWorker(&logger, server.Connection(), 1, queue)
		c.Expect(worker_err, gospec.Equals, nil)
		c.Expect(worker, gospec.Satisfies, nil != worker)

		cmds := []*RedisBatchCommand{
			MakeRedisBatchCommandSet("A", []byte("Bob")),
			MakeRedisBatchCommandSet("B", []byte("Gary")),
		}
		worker.runCommands(cmds)

		strs, strs_err := RedisDsl{server.Connection()}.MGET_STRINGS("A", "B")
		c.Expect(strs_err, gospec.Equals, nil)
		c.Expect(strs, gospec.Satisfies, 2 == len(strs))
		c.Expect(*strs[0], gospec.Equals, "Bob")
		c.Expect(*strs[1], gospec.Equals, "Gary")
	})

	c.Specify("[RedisBatchQueueWorker][mustPopCommand]", func() {
		prev := runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)

		logger := &log4go.Logger{}
		connection := &RedisConnection{}
		batch_size := uint(1)
		queue := make(chan *RedisBatchCommand, 2)
		defer close(queue)

		ptr, err := makeRedisBatchQueueWorker(logger, connection, batch_size, queue)
		c.Expect(err, gospec.Satisfies, nil == err)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)

		counter := make(chan int, 2)
		defer close(counter)

		// Pop the command
		go func() {
			fmt.Println("Waiting for command ...")
			cmd, ok := ptr.mustPopCommand()

			fmt.Println("Got command:", cmd, ok)
			counter <- 1

			fmt.Println("Forwarded command:", cmd, ok)
		}()

		time.Sleep(time.Millisecond)

		fmt.Println("Queue sizes:", len(queue), len(counter))
		c.Expect(len(counter), gospec.Equals, 0)

		queue <- &RedisBatchCommand{}
		fmt.Println("Queue sizes:", len(queue), len(counter))

		time.Sleep(10 * time.Millisecond)

		fmt.Println("Queue sizes:", len(queue), len(counter))
		c.Expect(len(counter), gospec.Equals, 1)

		<-counter

		fmt.Println("Queue sizes:", len(queue), len(counter))
	})

	c.Specify("[RedisBatchQueueWorker][mayPopCommand]", func() {
		prev := runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)

		logger := &log4go.Logger{}
		connection := &RedisConnection{}
		batch_size := uint(1)
		queue := make(chan *RedisBatchCommand, 2)
		defer close(queue)

		ptr, err := makeRedisBatchQueueWorker(logger, connection, batch_size, queue)
		c.Expect(err, gospec.Satisfies, nil == err)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)

		cmd, ok := ptr.mayPopCommand()
		c.Expect(cmd, gospec.Satisfies, nil == cmd)
		c.Expect(ok, gospec.Equals, true)
		c.Expect(len(queue), gospec.Equals, 0)

		queue <- &RedisBatchCommand{}
		c.Expect(len(queue), gospec.Equals, 1)

		cmd, ok = ptr.mayPopCommand()
		c.Expect(cmd, gospec.Satisfies, nil != cmd)
		c.Expect(ok, gospec.Equals, true)
		c.Expect(len(queue), gospec.Equals, 0)
	})

	c.Specify("[RedisBatchQueueWorker][popCommands]", func() {
		prev := runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)

		logger := &log4go.Logger{}
		connection := &RedisConnection{}
		batch_size := uint(10)
		queue := make(chan *RedisBatchCommand, int(batch_size*3))
		defer close(queue)

		ptr, err := makeRedisBatchQueueWorker(logger, connection, batch_size, queue)
		c.Expect(err, gospec.Satisfies, nil == err)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)

		counter := make(chan int, 2)
		defer close(counter)

		// Pop the commands
		pop := func() {
			cmds, _ := ptr.popCommands()
			counter <- len(cmds)
		}

		c.Expect(len(queue), gospec.Equals, 0)
		c.Expect(len(counter), gospec.Equals, 0)

		queue <- &RedisBatchCommand{}
		c.Expect(len(queue), gospec.Equals, 1)
		c.Expect(len(counter), gospec.Equals, 0)

		go pop()
		time.Sleep(time.Millisecond)
		c.Expect(len(queue), gospec.Equals, 0)
		c.Expect(len(counter), gospec.Equals, 1)

		count, ok := <-counter
		c.Expect(count, gospec.Equals, 1)
		c.Expect(ok, gospec.Equals, true)

		for i := 0; i < 15; i++ {
			queue <- &RedisBatchCommand{}
		}
		c.Expect(len(queue), gospec.Equals, 15)
		c.Expect(len(counter), gospec.Equals, 0)

		// Pops 1x batch_size commands
		go pop()
		time.Sleep(time.Millisecond)
		c.Expect(len(queue), gospec.Equals, 5)
		c.Expect(len(counter), gospec.Equals, 1)

		// Counts the correct number of commands
		count, ok = <-counter
		c.Expect(count, gospec.Equals, 10)
		c.Expect(ok, gospec.Equals, true)

		// Pops 1/2x batch_size commands
		go pop()
		time.Sleep(time.Millisecond)
		c.Expect(len(queue), gospec.Equals, 0)
		c.Expect(len(counter), gospec.Equals, 1)

		// Pop a partial batch:
		count, ok = <-counter
		c.Expect(count, gospec.Equals, 5)
		c.Expect(ok, gospec.Equals, true)
	})

	c.Specify("[RedisBatchQueueWorker][Run]", func() {
		prev := runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)

		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		batch_size := uint(10)
		queue := make(chan *RedisBatchCommand, int(batch_size*3))
		// defer close(queue)

		ptr, err := makeRedisBatchQueueWorker(&logger, server.Connection(), batch_size, queue)
		c.Expect(err, gospec.Satisfies, nil == err)
		c.Expect(ptr, gospec.Satisfies, nil != ptr)

		// Runs the task until the queue is closed
		go ptr.Run()

		queue <- MakeRedisBatchCommandHashIncrementBy("Hash", "Field A", 1)
		queue <- MakeRedisBatchCommandHashIncrementBy("Hash", "Field B", 10)
		queue <- MakeRedisBatchCommandHashIncrementBy("Hash", "Field C", 100)

		time.Sleep(50 * time.Millisecond)

		ints, ints_err := RedisDsl{server.Connection()}.HASH_MGET_INT64S("Hash", "Field A", "Field B", "Field C")
		c.Expect(ints_err, gospec.Equals, nil)
		c.Expect(len(ints), gospec.Equals, 3)
		c.Expect(*ints[0], gospec.Equals, int64(1))
		c.Expect(*ints[1], gospec.Equals, int64(10))
		c.Expect(*ints[2], gospec.Equals, int64(100))

		queue <- MakeRedisBatchCommandHashDelete("Hash", "Field A", "Field B", "Field C")
		time.Sleep(time.Millisecond)
		close(queue)

		time.Sleep(50 * time.Millisecond)

		ints, ints_err = RedisDsl{server.Connection()}.HASH_MGET_INT64S("Hash", "Field A", "Field B", "Field C")
		c.Expect(ints_err, gospec.Equals, nil)
		c.Expect(len(ints), gospec.Equals, 3)
		c.Expect(ints[0], gospec.Satisfies, nil == ints[0])
		c.Expect(ints[1], gospec.Satisfies, nil == ints[1])
		c.Expect(ints[2], gospec.Satisfies, nil == ints[2])
	})

}
