package dog_pool

import "fmt"
import "github.com/alecthomas/log4go"

type RedisBatchQueue struct {
	Logger     *log4go.Logger   "Logger for logging updates, errors, etc"
	Connection *RedisConnection "Connection to Redis"

	QueueSize        uint "How big should the queue of pending commands be?"
	WorkersSize      uint "How many workers should we have?"
	WorkersBatchSize uint "How many RedisBatchCommand's should the worker try to process at once? 1, 5, 10, 100, ..."

	queue   chan *RedisBatchCommand  "Input only queue"
	workers []*redisBatchQueueWorker "Workers we are running in the background"
}

// Capacity of the queue
func (p *RedisBatchQueue) Cap() int {
	if nil != p.queue {
		return cap(p.queue)
	}
	return -1
}

// Length of the queue
func (p *RedisBatchQueue) Len() int {
	if nil != p.queue {
		return len(p.queue)
	}
	return -1
}

// Format as a string
func (p *RedisBatchQueue) String() string {
	return fmt.Sprintf("RedisBatchQueue { Connection=%v, QueueSize=%v, WorkersSize=%v, WorkersBatchSize=%v, Queue.Cap=%v, Queue.Len=%v }", p.Connection, p.QueueSize, p.WorkersSize, p.WorkersBatchSize, p.Cap(), p.Len)
}

// Open the queue
func (p *RedisBatchQueue) Open() error {
	switch {
	case nil != p.queue:
		return fmt.Errorf("[RedisBatchQueue][Open] Queue is already open!")
	case nil == p.Connection:
		return fmt.Errorf("[RedisBatchQueue][Open] Nil redis connection!")
	case 0 == p.QueueSize:
		return fmt.Errorf("[RedisBatchQueue][Open] QueueSize[%v] must be > 0!", p.QueueSize)
	case 0 == p.WorkersSize:
		return fmt.Errorf("[RedisBatchQueue][Open] WorkersSize[%v] must be > 0!", p.WorkersSize)
	case 0 == p.WorkersBatchSize:
		return fmt.Errorf("[RedisBatchQueue][Open] WorkersBatchSize[%v] must be > 0!", p.WorkersBatchSize)
	case p.QueueSize < p.WorkersSize:
		return fmt.Errorf("[RedisBatchQueue][Open] QueueSize[%v] must be > WorkersSize[%v]!", p.QueueSize, p.WorkersSize)
	}

	p.queue = make(chan *RedisBatchCommand, p.QueueSize)

	p.workers = make([]*redisBatchQueueWorker, p.WorkersSize)
	for i := range p.workers {
		ptr, err := makeRedisBatchQueueWorker(p.Logger, p.Connection, p.WorkersBatchSize, p.queue)
		if nil != err {
			// Close the queue and cleanup the pointers
			p.Close()

			// Return the error
			return err
		}

		// Save the handle to the workers
		p.workers[i] = ptr

		// Kick off the go routine:
		go ptr.Run()
	}

	return nil
}

// Close the queue
func (p *RedisBatchQueue) Close() error {
	// Close the queue
	if nil != p.queue {
		close(p.queue)
	}

	// Cleanup the pointers
	p.queue = nil
	p.workers = nil

	return nil
}

// Push the command(s) onto the queue
func (p *RedisBatchQueue) Push(cmds ...*RedisBatchCommand) error {
	if nil == p.queue {
		return fmt.Errorf("[RedisBatchQueue][Push] Queue is closed!")
	}

	for _, cmd := range cmds {
		p.queue <- cmd
	}
	return nil
}
