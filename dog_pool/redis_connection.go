//
// Redis Connection Pool written in GO
//

package dog_pool

import "time"
import "github.com/fzzy/radix/redis"
import "github.com/alecthomas/log4go"

//
// Constants for connecting to Redis & Logging
//
const timeout = time.Duration(10) * time.Second
const log_client_has_saved_connection = "[RedisConnection][Client] - %s saved available for Url=%s"
const log_ping_reply = "[RedisConnection][Ping] %s for Url=%s, Redis Reply = %#v"
const log_open_failed = "[RedisConnection][Open] - Failed to connect to %s, error = %#v"
const log_open_success = "[RedisConnection][Open] - Opened new connection to %s"
const log_closed = "[RedisConnection][Close] - Closed connection to %s, error = %#v"

//
// Connection Wrapper for Redis
//
type RedisConnection struct {
	Url string "Redis URL this factory will connect to"

	Logger *log4go.Logger "Handle to the logger we are using"

	client *redis.Client "Connection to a Redis, may be nil"
}

//
// Get a connection to Redis
//
func (p *RedisConnection) Client() (*redis.Client, error) {
	// Is a saved connection available?
	if p.IsOpen() {
		p.Logger.Trace(log_client_has_saved_connection, "Has", p.Url)

		// Return the connection
		return p.client, nil
	} else {
		p.Logger.Warn(log_client_has_saved_connection, "No", p.Url)
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
	// Open the connection to Redis
	client, err := p.Client()
	if nil != err {
		// Return the error
		return err
	}

	// Ping the server & get the response
	client.Append("ping")
	reply := client.GetReply()

	// Connection error? Then tell the factory to invalidate the Redis connection
	if nil != reply.Err {
		p.Logger.Error(log_ping_reply, "Error", p.Url, reply)

		// Close the connection
		p.Close(reply.Err)

		// Return the error
		return reply.Err
	} else {
		p.Logger.Trace(log_ping_reply, "Success", p.Url, reply)
	}

	// Return nil on Success!
	return nil
}

//
// Return true if the client connection exists
//
func (p *RedisConnection) IsOpen() bool {
	return p.client != nil
}

//
// Return true if the client connection exists
//
func (p *RedisConnection) IsClosed() bool {
	return !p.IsOpen()
}

//
// Open a new connection to redis
//
func (p *RedisConnection) Open() (err error) {
	// Connect to Redis
	p.client, err = redis.DialTimeout("tcp", p.Url, timeout)

	// Error connecting?
	if nil != err {
		// Clear the connection pointer
		p.client = nil

		p.Logger.Error(log_open_failed, p.Url, err)
	} else {
		p.Logger.Trace(log_open_success, p.Url)
	}

	return
}

//
// Close the connection to redis
//
func (p *RedisConnection) Close(err error) {
	// Log the event
	p.Logger.Debug(log_closed, p.Url, err)

	// Close the connection
	if nil != p.client {
		p.client.Close()
	}

	// Set the pointer to nil
	p.client = nil
}
