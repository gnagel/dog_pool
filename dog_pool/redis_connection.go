//
// Redis Connection Wrapper written in GO
//

package dog_pool

import "fmt"
import "strings"
import "errors"
import "time"
import "github.com/fzzy/radix/redis"
import "github.com/alecthomas/log4go"

//
// Constants for connecting to Redis & Logging
//
var ErrConnectionIsClosed = errors.New("Connection is closed, command aborted")

const timeout = time.Duration(10) * time.Second
const log_client_has_saved_connection = "[RedisConnection][Client] - %s saved available for Url = %s"
const log_ping_reply = "[RedisConnection][Ping] %s for Url = %s, Redis Reply = %#v"
const log_closed = "[RedisConnection][Close] - Closed connection to %s"
const log_get_reply = "[RedisConnection][GetReply] Redis Reply = %#v"
const log_get_reply_error = "[RedisConnection][GetReply] Response Error = %v"

//
// Connection Wrapper for Redis
//
type RedisConnection struct {
	Url string "Redis URL this factory will connect to"

	Id string "(optional) Identifier for distingushing between redis connections"

	Logger *log4go.Logger "Handle to the logger we are using"

	client *redis.Client "Connection to a Redis, may be nil"
}

//
//  ========================================
//
// RedisClientInterface -and- redis.Client implementation:
//
//  ========================================
//

//
// Close closes the connection.
//
func (p *RedisConnection) Close() (err error) {
	// Close the connection
	if nil != p.client {
		err = p.client.Close()
	}

	// Set the pointer to nil
	p.client = nil

	// Log the event
	p.Logger.Debug("[RedisConnection][Close][%s/%s] --> Closed!", p.Url, p.Id)

	return
}

//
// Cmd calls the given Redis command:
// - Calls Append(...)
// - Returns GetReply()
//
func (p *RedisConnection) Cmd(cmd string, args ...interface{}) *redis.Reply {
	p.Append(cmd, args...)
	return p.GetReply()
}

//
// Append adds the given call to the pipeline queue.
// Use GetReply() to read the reply.
//
func (p *RedisConnection) Append(cmd string, args ...interface{}) {
	args_to_s := func() string {
		return fmt.Sprintf(strings.Repeat("%v ", len(args)), args...)
	}

	// Wrap in a lambda to prevent evaulation, unless logging is enabled ...
	p.Logger.Trace(func() string {
		args_s := args_to_s()
		return fmt.Sprintf("[RedisConnection][Append][%s/%s] Redis Command = '%s %s'", p.Url, p.Id, cmd, args_s)
	})

	// If the connection is not open, then open it
	if !p.IsOpen() {
		// Did opening the connection fail?
		if err := p.Open(); nil != err {
			p.Logger.Warn(func() string {
				args_s := args_to_s()
				return fmt.Sprintf("[RedisConnection][Append][%s/%s] Redis Command = '%s %s' --> Error = %v", p.Url, p.Id, cmd, args_s, err)
			})
			return
		}
	}

	// Append the command
	p.client.Append(cmd, args...)
}

//
// GetReply returns the reply for the next request in the pipeline queue.
// Error reply with PipelineQueueEmptyError is returned,
// if the pipeline queue is empty.
//
func (p *RedisConnection) GetReply() *redis.Reply {
	// Connection is closed?
	if !p.IsOpen() {
		return &redis.Reply{Type: redis.ErrorReply, Err: ErrConnectionIsClosed}
	}

	// Get the reply from redis
	reply := p.client.GetReply()

	// If the connection
	if reply.Type == redis.ErrorReply {
		//* Common errors
		switch reply.Err.Error() {
		case redis.AuthError.Error():
			fallthrough
		case redis.LoadingError.Error():
			fallthrough
		case redis.ParseError.Error():
			fallthrough
		case redis.PipelineQueueEmptyError.Error():
			// Log the error & break
			p.Logger.Warn("[RedisConnection][GetReply][%s/%s] Reply Error = %v", p.Url, p.Id, reply.Err)
			break

		default:
			// All other errors are fatal!
			// Close the connection and log the error
			p.Logger.Critical("[RedisConnection][GetReply][%s/%s] Fatal Reply Error = %v", p.Url, p.Id, reply.Err)
			p.Close()
		}
	} else {
		// Log the response
		p.Logger.Trace("[RedisConnection][GetReply][%s/%s] Reply.Type = %d, Reply.Value = %v", p.Url, p.Id, reply.Type, reply.String())
	}

	// Return the reply from redis to the caller
	return reply
}

//
//  ========================================
//
// RedisConnection implementation:
//
//  ========================================
//

//
// [Depricated, use Append/GetReply above instead]
//
// Get a connection to Redis
//
func (p *RedisConnection) Client() (*redis.Client, error) {
	// Is a saved connection available?
	if p.IsOpen() {
		p.Logger.Trace(log_client_has_saved_connection, "Has", p.Url, p.Id)

		// Return the connection
		return p.client, nil
	} else {
		p.Logger.Warn(log_client_has_saved_connection, "No", p.Url, p.Id)
	}

	// Open a new connection to redis
	if err := p.Open(); nil != err {
		// Abort on errors
		return nil, err
	}

	// Return the new redis connection
	return p.client, nil
}

//
// Ping the server, opening the client connection if necessary
// Returns:
//   nil   --> Ping was successful!
//   error --> Ping was failure
//
func (p *RedisConnection) Ping() error {
	return p.Cmd("ping").Err
}

//
// Return true if the client connection exists
//
func (p *RedisConnection) IsOpen() bool {
	output := nil != p.client

	// Debug logging
	p.Logger.Trace("[RedisConnection][IsOpen][%s/%s] --> %v", p.Url, p.Id, output)

	return output
}

//
// Return true if the client connection exists
//
func (p *RedisConnection) IsClosed() bool {
	output := nil == p.client

	// Debug logging
	p.Logger.Trace("[RedisConnection][IsClosed][%s/%s] --> %v", p.Url, p.Id, output)

	return output
}

//
// Open a new connection to redis
//
func (p *RedisConnection) Open() error {
	// Open the TCP connection
	client, err := redis.DialTimeout("tcp", p.Url, timeout)

	// Check for errors
	if nil != err {
		// Log the event
		p.Logger.Error("[RedisConnection][Open][%s/%s] --> Error = %v", p.Url, p.Id, err)

		// Return the error
		return err
	}

	// Save the client pointer
	p.client = client

	// Log the event
	p.Logger.Info("[RedisConnection][Open][%s/%s] --> Opened!", p.Url, p.Id)

	// Return nil
	return nil
}
