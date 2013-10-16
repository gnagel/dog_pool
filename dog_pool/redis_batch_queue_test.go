package dog_pool

import "time"
import "runtime"
import "github.com/alecthomas/log4go"

import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisBatchQueueSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisBatchQueueSpecs)
	gospec.MainGoTest(r, t)
}

func RedisBatchQueueSpecs(c gospec.Context) {

	c.Specify("[RedisBatchQueue][Cap]", func() {
		ptr := &RedisBatchQueue{
			queue: nil,
		}
		c.Expect(ptr.Cap(), gospec.Equals, -1)

		ptr.queue = make(chan *RedisBatchCommand, 10)
		defer close(ptr.queue)
		c.Expect(ptr.Cap(), gospec.Equals, 10)
	})

	c.Specify("[RedisBatchQueue][Len]", func() {
		ptr := &RedisBatchQueue{
			queue: nil,
		}
		c.Expect(ptr.Len(), gospec.Equals, -1)

		ptr.queue = make(chan *RedisBatchCommand, 10)
		defer close(ptr.queue)
		c.Expect(ptr.Len(), gospec.Equals, 0)

		ptr.queue <- &RedisBatchCommand{}
		c.Expect(ptr.Len(), gospec.Equals, 1)
	})

	c.Specify("[RedisBatchQueue][String]", func() {
		ptr := &RedisBatchQueue{
			QueueSize:        10,
			WorkersSize:      1,
			WorkersBatchSize: 5,
			queue:            make(chan *RedisBatchCommand, 10),
		}
		defer close(ptr.queue)
		c.Expect(ptr.String(), gospec.Equals, "RedisBatchQueue { Connection=<nil>, QueueSize=10, WorkersSize=1, WorkersBatchSize=5, Queue.Cap=10, Queue.Len=0 }")
	})

	c.Specify("[RedisBatchQueue][Open]", func() {
		ptr := &RedisBatchQueue{}
		defer ptr.Close()

		err := ptr.Open()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(ptr.queue, gospec.Satisfies, nil == ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 == len(ptr.workers))
		c.Expect(err.Error(), gospec.Equals, "[RedisBatchQueue][Open] Nil Logger!")

		ptr.Logger = &log4go.Logger{}
		ptr.queue = make(chan *RedisBatchCommand, 10)

		err = ptr.Open()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(ptr.queue, gospec.Satisfies, nil != ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 == len(ptr.workers))
		c.Expect(err.Error(), gospec.Equals, "[RedisBatchQueue][Open] Queue is already open!")

		close(ptr.queue)
		ptr.queue = nil
		err = ptr.Open()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(ptr.queue, gospec.Satisfies, nil == ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 == len(ptr.workers))
		c.Expect(err.Error(), gospec.Equals, "[RedisBatchQueue][Open] Nil redis connection!")

		ptr.Connection = &RedisConnection{}
		err = ptr.Open()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(ptr.queue, gospec.Satisfies, nil == ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 == len(ptr.workers))
		c.Expect(err.Error(), gospec.Equals, "[RedisBatchQueue][Open] QueueSize[0] must be > 0!")

		ptr.QueueSize = 10
		err = ptr.Open()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(ptr.queue, gospec.Satisfies, nil == ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 == len(ptr.workers))
		c.Expect(err.Error(), gospec.Equals, "[RedisBatchQueue][Open] WorkersSize[0] must be > 0!")

		ptr.WorkersSize = 20
		err = ptr.Open()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(ptr.queue, gospec.Satisfies, nil == ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 == len(ptr.workers))
		c.Expect(err.Error(), gospec.Equals, "[RedisBatchQueue][Open] WorkersBatchSize[0] must be > 0!")

		ptr.WorkersBatchSize = 5
		err = ptr.Open()
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(ptr.queue, gospec.Satisfies, nil == ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 == len(ptr.workers))
		c.Expect(err.Error(), gospec.Equals, "[RedisBatchQueue][Open] QueueSize[10] must be > WorkersSize[20]!")

		ptr.WorkersSize = 10
		err = ptr.Open()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(ptr.queue, gospec.Satisfies, nil != ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 < len(ptr.workers))
	})

	c.Specify("[RedisBatchQueue][Close]", func() {
		ptr := &RedisBatchQueue{}

		err := ptr.Close()
		c.Expect(err, gospec.Satisfies, nil == err)
		c.Expect(ptr.queue, gospec.Satisfies, nil == ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 == len(ptr.workers))

		ptr.queue = make(chan *RedisBatchCommand, 10)
		ptr.workers = []*redisBatchQueueWorker{nil, nil, nil, nil, nil}
		err = ptr.Close()
		c.Expect(err, gospec.Satisfies, nil == err)
		c.Expect(ptr.queue, gospec.Satisfies, nil == ptr.queue)
		c.Expect(ptr.workers, gospec.Satisfies, 0 == len(ptr.workers))
	})

	c.Specify("[RedisBatchQueue][RunAsync]", func() {
		prev := runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)

		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartRedisServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		ptr := &RedisBatchQueue{
			Logger:           &logger,
			Connection:       server.Connection(),
			QueueSize:        10,
			WorkersSize:      5,
			WorkersBatchSize: 2,
		}

		err = ptr.RunAsync(nil)
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(err.Error(), gospec.Equals, "[RedisBatchQueue][RunAsync] Queue is closed!")

		err = ptr.Open()
		c.Expect(err, gospec.Satisfies, nil == err)

		err = ptr.RunAsync(nil)
		c.Expect(err, gospec.Satisfies, nil != err)
		c.Expect(err.Error(), gospec.Equals, "[RedisBatchQueue][RunAsync][0] Nil RedisBatchCommand!")

		err = ptr.RunAsync(
			MakeRedisBatchCommandHashIncrementBy("Hash", "Field A", 1),
			MakeRedisBatchCommandHashIncrementBy("Hash", "Field B", 10),
			MakeRedisBatchCommandHashIncrementBy("Hash", "Field C", 100),
		)
		c.Expect(err, gospec.Satisfies, nil == err)

		time.Sleep(50 * time.Millisecond)

		ints, ints_err := RedisDsl{server.Connection()}.HASH_MGET_INT64S("Hash", "Field A", "Field B", "Field C")
		c.Expect(ints_err, gospec.Equals, nil)
		c.Expect(len(ints), gospec.Equals, 3)
		c.Expect(*ints[0], gospec.Equals, int64(1))
		c.Expect(*ints[1], gospec.Equals, int64(10))
		c.Expect(*ints[2], gospec.Equals, int64(100))

		err = ptr.RunAsync(
			MakeRedisBatchCommandHashDelete("Hash", "Field A", "Field B", "Field C"),
		)
		c.Expect(err, gospec.Satisfies, nil == err)

		time.Sleep(time.Millisecond)
		ptr.Close()

		time.Sleep(50 * time.Millisecond)

		ints, ints_err = RedisDsl{server.Connection()}.HASH_MGET_INT64S("Hash", "Field A", "Field B", "Field C")
		c.Expect(ints_err, gospec.Equals, nil)
		c.Expect(len(ints), gospec.Equals, 3)
		c.Expect(ints[0], gospec.Satisfies, nil == ints[0])
		c.Expect(ints[1], gospec.Satisfies, nil == ints[1])
		c.Expect(ints[2], gospec.Satisfies, nil == ints[2])
	})

}
