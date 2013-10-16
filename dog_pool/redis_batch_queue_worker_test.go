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

	c.Specify("[RedisBatchQueueWorker][mayPopCommand]", func() {})

	c.Specify("[RedisBatchQueueWorker][popCommands]", func() {})

	c.Specify("[RedisBatchQueueWorker][Run]", func() {})

}
