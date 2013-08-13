//
// Redis Client Interface
//
// Interface implemented by redis.Client and dog_pool.RedisConnection
//

package dog_pool

import "github.com/fzzy/radix/redis"

type RedisClientInterface interface {
	// Close closes the connection.
	Close() error

	// Cmd calls the given Redis command.
	Cmd(cmd string, args ...interface{}) *redis.Reply

	// Append adds the given call to the pipeline queue.
	// Use GetReply() to read the reply.
	Append(cmd string, args ...interface{})

	// GetReply returns the reply for the next request in the pipeline queue.
	// Error reply with PipelineQueueEmptyError is returned,
	// if the pipeline queue is empty.
	GetReply() *redis.Reply
}
