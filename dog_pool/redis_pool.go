//
// Redis Connection Pool written in GO
//

package dog_pool

import "fmt"
import "errors"
import "github.com/alecthomas/log4go"

//
// What mode are we building the connection pool in?
//
type ConnectionMode int

//
// How should we populate the connection pool?
//
const (
	_ ConnectionMode = iota
	LAZY
	AGRESSIVE
)

//
// Redis Connection Pool wrapper
//
type RedisConnectionPool struct {
	Mode   ConnectionMode         "How should we prepare the connection pool?"
	Size   int                    "(Max) Pool size"
	Urls   []string               "Redis URLs to connect to"
	Logger log4go.Logger          "Logger we are using in the connection pool"
	myPool *ConnectionPoolWrapper "Connection Pool wrapper"
}

//
// Open the connection pool
//
func (p *RedisConnectionPool) Open() error {
	p.Close()

	// Lambda to iterate the urls
	nextUrl := loopStrings(p.Urls)

	// Lambda for creating the factories
	var initfn InitFunction
	switch p.Mode {
	case LAZY:
		// Create the factory
		// DON'T Connect to Redis
		// DON'T Test the connection
		initfn = func() (interface{}, error) {
			return makeLazyConnection(nextUrl(), &p.Logger)
		}
	case AGRESSIVE:
		// Create the factory
		// AND Connect to Redis
		// AND Test the connection
		initfn = func() (interface{}, error) {
			return makeAgressiveConnection(nextUrl(), &p.Logger)
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
func (p *RedisConnectionPool) Close() {
	// If the pool is not nil,
	// Then close all the connections and release the pointer
	if nil != p.myPool {
		for i := 0; i < p.Size; i++ {
			// Pop a connection from the pool
			c, _ := p.Pop()

			// Close the connection
			if nil != c {
				c.Close(nil)
			}
		}
	}

	// Release the connection pool
	p.myPool = nil
}

//
// Get a RedisConnection from the pool
//
func (p *RedisConnectionPool) Pop() (*RedisConnection, error) {
	// Pop a connection from the pool
	c := p.myPool.GetConnection()

	// Return the connection
	if c != nil {
		return c.(*RedisConnection), nil
	}

	// Return an error when all connections are exhausted
	return nil, errors.New("No RedisConnection available")
}

//
// Return a RedisConnection
//
func (p *RedisConnectionPool) Push(c *RedisConnection) {
	p.myPool.ReleaseConnection(c)
}
