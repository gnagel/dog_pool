//
// Redis Connection Wrapper written in GO
//

package dog_pool

import "bytes"
import "fmt"
import "strings"
import "time"
import "reflect"
import "strconv"
import "github.com/RUNDSP/radix/redis"
import "github.com/alecthomas/log4go"

//
// Connection Wrapper for Redis
//
type RedisConnection struct {
	Url string "Redis URL this factory will connect to"

	Id string "(optional) Identifier for distingushing between redis connections"

	Logger *log4go.Logger "Handle to the logger we are using"

	Timeout time.Duration "Connection Timeout"

	client *redis.Client "Connection to a Redis, may be nil"

	cmd_queue []string
}

func (p *RedisConnection) String() string {
	return fmt.Sprintf("RedisConnection { Id=%v, Url=%v, Timeout=%v }", p.Id, p.Url, p.Timeout)
}

//
// Lazily make a Redis Connection
//
func makeLazyRedisConnection(url string, id string, timeout time.Duration, logger *log4go.Logger) (*RedisConnection, error) {
	// Create a new factory instance
	p := &RedisConnection{Url: url, Id: id, Logger: logger, Timeout: timeout}

	// Return the factory
	return p, nil
}

//
// Agressively make a Redis Connection
//
func makeAgressiveRedisConnection(url string, id string, timeout time.Duration, logger *log4go.Logger) (*RedisConnection, error) {
	// Create a new factory instance
	p, _ := makeLazyRedisConnection(url, id, timeout, logger)

	// Ping the server
	if err := p.Ping(); nil != err {
		// Close the connection
		p.Close()

		// Return the error
		return nil, err
	}

	// Return the factory
	return p, nil
}

//
// Clone the connection and return a new instance of RedisConnection
//
func (p *RedisConnection) Clone() *RedisConnection {
	connection, _ := makeLazyRedisConnection(p.Url, p.Id, p.Timeout, p.Logger)
	return connection
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
	p.Logger.Info("[RedisConnection][Close][%s/%s] --> Closed!", p.Url, p.Id)

	return
}

//
// Cmd calls the given Redis command:
// - Calls Append(...)
// - Returns GetReply()
//
func (p *RedisConnection) Cmd(cmd string, args ...interface{}) *redis.Reply {
	stop_watch := MakeStopWatch(p, p.Logger, strings.Join([]string{"Cmd", cmd}, " ")).Start()
	defer stop_watch.LogDurationAt(log4go.TRACE)
	defer stop_watch.Stop()

	p.Append(cmd, args...)
	return p.GetReply()
}

//
// Append adds the given call to the pipeline queue.
// Use GetReply() to read the reply.
//
func (p *RedisConnection) Append(cmd string, args ...interface{}) {
	last_cmd := string(formatArgs(cmd, args))
	p.cmd_queue = append(p.cmd_queue, last_cmd)

	// Wrap in a lambda to prevent evaulation, unless logging is enabled ...
	p.Logger.Trace("[RedisConnection][Append][%s/%s] Redis Command = '%s'", p.Url, p.Id, last_cmd)

	// If the connection is not open, then open it
	if !p.IsOpen() {
		// Did opening the connection fail?
		if err := p.Open(); nil != err {
			p.Logger.Warn("[RedisConnection][Append][%s/%s] Redis Command = '%s' --> Error = %v", p.Url, p.Id, last_cmd, err)
			return
		}
	}

	// Append the command
	stop_watch := MakeStopWatchTags(p, p.Logger, []string{p.Url, p.Id, "Append", cmd}).Start()
	p.client.Append(cmd, args...)
	stop_watch.Stop().LogDurationAt(log4go.FINEST)
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
	stop_watch := MakeStopWatchTags(p, p.Logger, []string{p.Url, p.Id, "GetReply"}).Start()
	reply := p.client.GetReply()
	stop_watch.Stop().LogDurationAt(log4go.FINEST)

	var first_cmd string
	switch {
	case 1 == len(p.cmd_queue):
		first_cmd = p.cmd_queue[0]
		p.cmd_queue = nil
	case 1 < len(p.cmd_queue):
		first_cmd = p.cmd_queue[0]
		p.cmd_queue = p.cmd_queue[1:]
	}

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
			p.Logger.Warn("[RedisConnection][GetReply][%s/%s] Ignored Error from Redis, cmd=%v, Error = %v", p.Url, p.Id, first_cmd, reply.Err)
			break

		default:
			// All other errors are fatal!
			// Close the connection and log the error
			p.Logger.Error("[RedisConnection][GetReply][%s/%s] Fatal Error from Redis, cmd=%v, Error = %v", p.Url, p.Id, first_cmd, reply.Err)
			p.Close()
		}
	} else {
		// Log the response
		p.Logger.Info("[RedisConnection][GetReply][%s/%s] Redis Reply, Cmd=%v, Reply=%#v", p.Url, p.Id, first_cmd, reply)
	}

	// Return the reply from redis to the caller
	return reply
}

//
//  ========================================
//
// RedisConnection Utils:
//
//  ========================================
//

func (p *RedisConnection) KeysExist(keys ...string) ([]bool, error) {
	count := len(keys)

	commands := make([]*RedisBatchCommand, count)
	for i, key := range keys {
		commands[i] = MakeRedisBatchCommandExists(key)
	}

	err := RedisBatchCommands(commands).ExecuteBatch(p)
	if err != nil {
		return nil, err
	}

	exists := make([]bool, count)

	for i := range keys {
		reply := commands[i].Reply()
		if nil != reply.Err {
			return nil, reply.Err
		}

		ok, err := reply.Int()
		if err != nil {
			return nil, err
		}

		exists[i] = ok == 1
	}

	return exists, nil
}

func (p *RedisConnection) HashFieldsExist(key string, fields ...string) ([]bool, error) {
	count := len(fields)

	commands := make([]*RedisBatchCommand, count)
	for i, field := range fields {
		commands[i] = MakeRedisBatchCommandHashExists(key, field)
	}

	err := RedisBatchCommands(commands).ExecuteBatch(p)
	if err != nil {
		return nil, err
	}

	exists := make([]bool, count)
	for i := range fields {
		reply := commands[i].Reply()
		if nil != reply.Err {
			return nil, reply.Err
		}

		ok, err := reply.Int()
		if err != nil {
			return nil, err
		}

		exists[i] = ok == 1
	}

	return exists, nil
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
		p.Logger.Trace("[RedisConnection][Client][%s/%s] --> Found Opened Connection!", p.Url, p.Id)

		// Return the connection
		return p.client, nil
	} else {
		p.Logger.Warn("[RedisConnection][Client][%s/%s] --> Found Closed Connection!", p.Url, p.Id)
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
	// Set the default timeout
	if time.Duration(0) == p.Timeout {
		p.Timeout = time.Duration(10) * time.Second
	}

	// Open the TCP connection
	client, err := redis.DialTimeout("tcp", p.Url, p.Timeout)

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

// formatArg formats the given argument to a Redis-styled argument byte slice.
func formatArg(v interface{}) []byte {
	var b, bs []byte

	switch vt := v.(type) {
	case []byte:
		bs = vt
	case string:
		bs = []byte(vt)
	case bool:
		if vt {
			bs = []byte{'1'}
		} else {
			bs = []byte{'0'}
		}
	case nil:
		// empty byte slice
	case int:
		bs = []byte(strconv.Itoa(vt))
	case int8:
		bs = []byte(strconv.FormatInt(int64(vt), 10))
	case int16:
		bs = []byte(strconv.FormatInt(int64(vt), 10))
	case int32:
		bs = []byte(strconv.FormatInt(int64(vt), 10))
	case int64:
		bs = []byte(strconv.FormatInt(vt, 10))
	case uint:
		bs = []byte(strconv.FormatUint(uint64(vt), 10))
	case uint8:
		bs = []byte(strconv.FormatUint(uint64(vt), 10))
	case uint16:
		bs = []byte(strconv.FormatUint(uint64(vt), 10))
	case uint32:
		bs = []byte(strconv.FormatUint(uint64(vt), 10))
	case uint64:
		bs = []byte(strconv.FormatUint(vt, 10))
	default:
		// Fallback to reflect-based.
		switch reflect.TypeOf(vt).Kind() {
		case reflect.Slice:
			rv := reflect.ValueOf(vt)
			for i := 0; i < rv.Len(); i++ {
				bs = append(bs, formatArg(rv.Index(i).Interface())...)
			}

			return bs
		case reflect.Map:
			rv := reflect.ValueOf(vt)
			keys := rv.MapKeys()
			for _, k := range keys {
				bs = append(bs, formatArg(k.Interface())...)
				bs = append(bs, formatArg(rv.MapIndex(k).Interface())...)
			}

			return bs
		default:
			var buf bytes.Buffer

			fmt.Fprint(&buf, v)
			bs = buf.Bytes()
		}
	}

	var delim []byte = []byte{' '}

	// b = append(b, '$')
	// b = append(b, []byte(strconv.Itoa(len(bs)))...)
	// b = append(b, delim...)
	b = append(b, bs...)
	b = append(b, delim...)
	return b
}

// createRequest creates a request string from the given requests.
func formatArgs(cmd string, args ...interface{}) []byte {
	var total []byte

	var s []byte

	// Calculate number of arguments.
	argsLen := 1
	for _, arg := range args {
		kind := reflect.TypeOf(arg).Kind()
		switch kind {
		case reflect.Slice:
			argsLen += reflect.ValueOf(arg).Len()
		case reflect.Map:
			argsLen += reflect.ValueOf(arg).Len() * 2
		default:
			argsLen++
		}
	}

	var delim []byte = []byte{' '}

	// number of arguments
	// s = append(s, '*')
	// s = append(s, []byte(strconv.Itoa(argsLen))...)
	// s = append(s, delim...)

	// command
	// s = append(s, '$')
	// s = append(s, []byte(strconv.Itoa(len(cmd)))...)
	// s = append(s, delim...)
	s = append(s, []byte(cmd)...)
	s = append(s, delim...)

	// arguments
	for _, arg := range args {
		s = append(s, formatArg(arg)...)
	}

	total = append(total, s...)

	return total
}
