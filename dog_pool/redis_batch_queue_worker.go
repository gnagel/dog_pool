package dog_pool

import "fmt"
import "github.com/alecthomas/log4go"

// Worker for running Redis Commands serially in a go routine
type redisBatchQueueWorker struct {
	Logger       *log4go.Logger            "Logger for logging updates, errors, etc"
	Connection   *RedisConnection          "Connection to Redis"
	BatchSize    uint                      "Number of RedisBatchCommand's to process at once"
	CommandQueue <-chan *RedisBatchCommand "Output only queue"
}

// Make a new instance of redisBatchQueueWorker, or return an error
func makeRedisBatchQueueWorker(logger *log4go.Logger, connection *RedisConnection, batch_size uint, queue <-chan *RedisBatchCommand) (*redisBatchQueueWorker, error) {
	p := &redisBatchQueueWorker{
		Logger:       logger,
		Connection:   connection,
		CommandQueue: queue,
		BatchSize:    batch_size,
	}

	switch {
	case nil == p.Logger:
		return nil, fmt.Errorf("[redisBatchQueueWorker][Make] Nil logger!")
	case nil == p.Connection:
		return nil, fmt.Errorf("[redisBatchQueueWorker][Make] Nil redis connection!")
	case nil == p.CommandQueue:
		return nil, fmt.Errorf("[redisBatchQueueWorker][Make] Nil queue!")
	case 0 == p.BatchSize:
		return nil, fmt.Errorf("[redisBatchQueueWorker][Make] BatchSize must be greater than 0!")
	default:
		return p, nil
	}
}

//
// Poll the Queue for commands
//
func (p *redisBatchQueueWorker) Run() {
	for {
		// Pop any pending commands
		cmds, queue_is_open := p.popCommands()

		// Run any commands we have collected
		if len(cmds) > 0 {
			p.runCommands(cmds)
		}

		// The queue is closed, exit the go routine
		if !queue_is_open {
			return
		}
	}
}

//
// Pop a RedisBatchCommand from the queue, blocks until a command is available or the queue is closed:
//
// Returns:
//   ptr, true  --> Got a command, the queue is open
//   nil, false --> The queue is closed
func (p *redisBatchQueueWorker) mustPopCommand() (*RedisBatchCommand, bool) {
	select {
	// Will only execute once there is a command or the queue is closed:
	case cmd, queue_is_open := <-p.CommandQueue:
		return cmd, queue_is_open
	}

	panic("[redisBatchQueueWorker][mustPopCommand] Should never get here")
	return nil, false
}

//
// Pop a RedisBatchCommand from the queue if possible:
//
// Returns:
//   ptr, true  --> Got a command, the queue is open
//   nil, true  --> No commands left in the queue, the queue is open
//   nil, false --> The queue is closed
func (p *redisBatchQueueWorker) mayPopCommand() (*RedisBatchCommand, bool) {
	select {
	case cmd, queue_is_open := <-p.CommandQueue:
		// Will only execute once there is a command or the queue is closed:
		return cmd, queue_is_open
	default:
		// The queue is empty, continue ...
		return nil, true
	}
}

//
// Pop a collection of commands from the queue
//
// Returns:
//   ptrs, true  --> We recieve commands and the queue is open
//   ptrs, false --> We recieve commands and the queue is closed
//   [], true    --> The queue is empty and open
//   [], false   --> The queue is empty and closed
//
func (p *redisBatchQueueWorker) popCommands() (RedisBatchCommands, bool) {
	commands := make([]*RedisBatchCommand, p.BatchSize)[0:0]

	cmd, ok := p.mustPopCommand()
	switch {
	case !ok:
		// The queue is closed
		return nil, false

	case nil != cmd:
		// We got a command
		commands = append(commands, cmd)

	default:
		// nil == cmd
		panic("[redisBatchQueueWorker][popCommands] Should never get here")
		return nil, false
	}

	for i := uint(1); i < p.BatchSize; i++ {
		cmd, ok = p.mayPopCommand()
		switch {
		case !ok:
			// The queue is closed, return what we have and exit
			return commands, false

		case nil != cmd:
			// We got a command
			commands = append(commands, cmd)

		default:
			// nil == cmd

			// The queue is empty & open, return what we have
			return commands, true
		}
	}

	// We have filled up the commands buffer, exit now
	return commands, true
}

//
// Execute a batch of commands and log the results
//
func (p *redisBatchQueueWorker) runCommands(cmds RedisBatchCommands) {
	//  Execute the batch and log any high-level errors:
	if err := cmds.ExecuteBatch(p.Connection); nil != err {
		p.Logger.Critical("[redisBatchQueueWorker][Run] Error processing Redis Batch: err=%v", err)
	}

	// Iterate the commands and log the command + results:
	for i, cmd := range cmds {
		switch err := cmd.Reply().Err; {
		case nil != err:
			p.Logger.Critical("[redisBatchQueueWorker][Run][%v] Error processing Redis Command: err=%v, cmd=%v", i, err, cmd)
		default:
			p.Logger.Info("[redisBatchQueueWorker][Run][%v] Success processing Redis Command: cmd=%v", i, cmd)
		}
	}
}
