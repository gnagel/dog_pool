//
// Thrift Connection Wrapper written in GO
//

package dog_pool

import "fmt"
import "strings"
import "time"
import "github.com/alecthomas/log4go"
import "./thrift"

//
// Connection Wrapper for Thrift
//
type ThriftConnection struct {
	Url string "Thrift URL this factory will connect to"

	Id string "(optional) Identifier for distingushing between thrift connections"

	Logger *log4go.Logger "Handle to the logger we are using"

	Timeout time.Duration "Connection Timeout"

	client *thrift.HbaseClient "Connection to a Thrift, may be nil"
}

//
// Lazily make a Thrift Connection
//
func makeLazyThriftConnection(url string, id string, timeout time.Duration, logger *log4go.Logger) (*ThriftConnection, error) {
	// Create a new factory instance
	p := &ThriftConnection{Url: url, Id: id, Logger: logger, Timeout: timeout}

	// Return the factory
	return p, nil
}

//
// Agressively make a Thrift Connection
//
func makeAgressiveThriftConnection(url string, id string, timeout time.Duration, logger *log4go.Logger) (*ThriftConnection, error) {
	// Create a new factory instance
	p, _ := makeLazyThriftConnection(url, id, timeout, logger)

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
//  ========================================
//
// ThriftClientInterface -and- thrift.HbaseClient implementation:
//
//  ========================================
//

//
// Cmd calls the given Thrift command:
// - Calls Append(...)
// - Returns GetReply()
//
func (p *ThriftConnection) Cmd(cmd string, args ...interface{}) *thrift.Reply {
	p.Append(cmd, args...)
	return p.GetReply()
}

//
// Append adds the given call to the pipeline queue.
// Use GetReply() to read the reply.
//
func (p *ThriftConnection) Append(cmd string, args ...interface{}) {
	args_to_s := func() string {
		return fmt.Sprintf(strings.Repeat("%v ", len(args)), args...)
	}

	// Wrap in a lambda to prevent evaulation, unless logging is enabled ...
	p.Logger.Trace(func() string {
		args_s := args_to_s()
		return fmt.Sprintf("[ThriftConnection][Append][%s/%s] Thrift Command = '%s %s'", p.Url, p.Id, cmd, args_s)
	})

	// If the connection is not open, then open it
	if !p.IsOpen() {
		// Did opening the connection fail?
		if err := p.Open(); nil != err {
			p.Logger.Warn(func() string {
				args_s := args_to_s()
				return fmt.Sprintf("[ThriftConnection][Append][%s/%s] Thrift Command = '%s %s' --> Error = %v", p.Url, p.Id, cmd, args_s, err)
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
func (p *ThriftConnection) GetReply() *thrift.Reply {
	// Connection is closed?
	if !p.IsOpen() {
		return &thrift.Reply{Type: thrift.ErrorReply, Err: ErrConnectionIsClosed}
	}

	// Get the reply from thrift
	reply := p.client.GetReply()

	// If the connection
	if reply.Type == thrift.ErrorReply {
		//* Common errors
		switch reply.Err.Error() {
		case thrift.AuthError.Error():
			fallthrough
		case thrift.LoadingError.Error():
			fallthrough
		case thrift.ParseError.Error():
			fallthrough
		case thrift.PipelineQueueEmptyError.Error():
			// Log the error & break
			p.Logger.Warn("[ThriftConnection][GetReply][%s/%s] Ignored Error from Thrift, Error = %v", p.Url, p.Id, reply.Err)
			break

		default:
			// All other errors are fatal!
			// Close the connection and log the error
			p.Logger.Error("[ThriftConnection][GetReply][%s/%s] Fatal Error from Thrift, Error = %v", p.Url, p.Id, reply.Err)
			p.Close()
		}
	} else {
		// Log the response
		p.Logger.Trace("[ThriftConnection][GetReply][%s/%s] Thrift Reply Type = %d, Value = %v", p.Url, p.Id, reply.Type, reply.String())
	}

	// Return the reply from thrift to the caller
	return reply
}

//
//  ========================================
//
// ThriftConnection implementation:
//
//  ========================================
//

//
// Ping the server, opening the client connection if necessary
// Returns:
//   nil   --> Ping was successful!
//   error --> Ping was failure
//
func (p *ThriftConnection) Ping() error {
	return p.Cmd("ping").Err
}

//
// Return true if the client connection exists
//
func (p *ThriftConnection) IsOpen() bool {
	output := nil != p.client

	// Debug logging
	p.Logger.Trace("[ThriftConnection][IsOpen][%s/%s] --> %v", p.Url, p.Id, output)

	return output
}

//
// Return true if the client connection exists
//
func (p *ThriftConnection) IsClosed() bool {
	output := nil == p.client

	// Debug logging
	p.Logger.Trace("[ThriftConnection][IsClosed][%s/%s] --> %v", p.Url, p.Id, output)

	return output
}

//
// Open a new connection to thrift
//
func (p *ThriftConnection) Open() error {
	// Set the default timeout
	if time.Duration(0) == p.Timeout {
		p.Timeout = time.Duration(10) * time.Second
	}

	// Open the TCP connection
	client := &thrift.HbaseClient{}

	// Open the connection &
	// Check for errors
	if err := client.Open(p.Url, p.Timeout); nil != err {
		// Log the event
		p.Logger.Error("[ThriftConnection][Open][%s/%s] --> Error = %v", p.Url, p.Id, err)

		// Return the error
		return err
	}

	// Save the client pointer
	p.client = client

	// Log the event
	p.Logger.Info("[ThriftConnection][Open][%s/%s] --> Opened!", p.Url, p.Id)

	// Return nil
	return nil
}

//
// Close closes the connection.
//
func (p *ThriftConnection) Close() (err error) {
	// Close the connection
	if nil != p.client {
		err = p.client.Close()
	}

	// Set the pointer to nil
	p.client = nil

	// Log the event
	p.Logger.Info("[ThriftConnection][Close][%s/%s] --> Closed!", p.Url, p.Id)

	return
}
