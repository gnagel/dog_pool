//
// Memcached Connection Pool written in GO
//

package dog_pool

import "fmt"
import "errors"
import "time"
import "github.com/alecthomas/log4go"

//
// Memcached Connection Pool wrapper
//
type MemcachedConnectionPool struct {
	Mode    ConnectionMode         "How should we prepare the connection pool?"
	Size    int                    "(Max) Pool size"
	Urls    []string               "Memcached URLs to connect to"
	Logger  log4go.Logger          "Logger we are using in the connection pool"
	Timeout time.Duration          "Timeout to use for Memcached Connections"
	myPool  *ConnectionPoolWrapper "Connection Pool wrapper"
}

//
// Is the pool open?
//
func (p *MemcachedConnectionPool) IsOpen() bool {
	return nil != p.myPool
}

//
// Is the pool closed?
//
func (p *MemcachedConnectionPool) IsClosed() bool {
	return nil == p.myPool
}

//
// Length of the pool
// Returns -1 if the pool is not open
//
func (p *MemcachedConnectionPool) Len() int {
	if p.IsOpen() {
		return p.myPool.Len()
	}
	return -1
}

//
// Open the connection pool
//
func (p *MemcachedConnectionPool) Open() error {
	p.Close()

	// Default to 15s timeout
	if time.Duration(0) == p.Timeout {
		p.Timeout = time.Duration(15) * time.Second
	}

	// Lambda to iterate the urls
	nextUrl := loopStrings(p.Urls)

	// Lambda for creating the factories
	var initfn InitFunction
	switch p.Mode {
	case LAZY:
		// Create the factory
		// DON'T Connect to Memcached
		// DON'T Test the connection
		initfn = func() (interface{}, error) {
			values := nextUrl()
			return makeLazyMemcachedConnection(values[0], values[1], p.Timeout, &p.Logger)
		}
	case AGRESSIVE:
		// Create the factory
		// AND Connect to Memcached
		// AND Test the connection
		initfn = func() (interface{}, error) {
			values := nextUrl()
			return makeAgressiveMemcachedConnection(values[0], values[1], p.Timeout, &p.Logger)
		}
		// No mode specified!
	default:
		return errors.New(fmt.Sprintf("Invalid connection mode: %v", p.Mode))
	}

	// Create the new pool
	pool, err := MakeConnectionPoolWrapper(p.Size, initfn)

	// Error creating the pool?
	if nil != err {
		return err
	}

	// Save the pointer to the pool
	p.myPool = pool

	// Return nil
	return nil
}

//
// Close the connection pool
//
func (p *MemcachedConnectionPool) Close() {
	if p.IsClosed() {
		return
	}

	// If the pool is not nil,
	// Then close all the connections and release the pointer
	if nil != p.myPool {
		for i := 0; i < p.Size; i++ {
			// Pop a connection from the pool
			c, _ := p.Pop()

			// Close the connection
			if nil != c {
				c.Close()
			}
		}
	}

	// Release the connection pool
	p.myPool = nil
}

//
// Get a MemcachedConnection from the pool
//
func (p *MemcachedConnectionPool) Pop() (*MemcachedConnection, error) {
	// Pop a connection from the pool
	c := p.myPool.GetConnection()

	// Return the connection
	if c != nil {
		return c.(*MemcachedConnection), nil
	}

	// Return an error when all connections are exhausted
	return nil, ErrNoConnectionsAvailable
}

//
// Return a MemcachedConnection
//
func (p *MemcachedConnectionPool) Push(c *MemcachedConnection) {
	p.myPool.ReleaseConnection(c)
}
