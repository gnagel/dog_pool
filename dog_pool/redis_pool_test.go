package dog_pool

import "os/exec"
import "time"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"

func TestRedisPoolSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.AddSpec(RedisPoolSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisPoolSpecs(c gospec.Context) {
	var redis_pool_logger = log4go.NewDefaultLogger(log4go.CRITICAL)

	c.Specify("[RedisConnectionPool] New Pool is not open", func() {
		pool := RedisConnectionPool{Mode: AGRESSIVE, Size: 0, Urls: []string{}, Logger: redis_pool_logger}
		defer pool.Close()

		c.Expect(pool.IsOpen(), gospec.Equals, false)
		c.Expect(pool.IsClosed(), gospec.Equals, true)
		c.Expect(pool.IsOpen(), gospec.Satisfies, pool.IsOpen() != pool.IsClosed())
		c.Expect(pool.Len(), gospec.Equals, -1)
	})

	c.Specify("[RedisConnectionPool] Opening a Pool with Undefined Mode has errors", func() {
		pool := RedisConnectionPool{Mode: 0, Size: 0, Urls: []string{}, Logger: redis_pool_logger}
		defer pool.Close()

		// Should have an error
		err := pool.Open()
		c.Expect(err, gospec.Satisfies, err != nil)

		// Should be closed
		c.Expect(pool.IsClosed(), gospec.Equals, true)
		c.Expect(pool.Len(), gospec.Equals, -1)
	})

	c.Specify("[RedisConnectionPool] Size=0 pool is Empty", func() {
		pool := RedisConnectionPool{Mode: AGRESSIVE, Size: 0, Urls: []string{}, Logger: redis_pool_logger}
		defer pool.Close()

		// Shouldn't have any errors
		err := pool.Open()
		c.Expect(err, gospec.Equals, nil)

		// Should be open
		c.Expect(pool.IsOpen(), gospec.Equals, true)
		c.Expect(pool.IsClosed(), gospec.Equals, false)

		// Should be empty
		c.Expect(pool.Len(), gospec.Equals, 0)
	})

	c.Specify("[RedisConnectionPool] Pop from empty pool returns error", func() {
		pool := RedisConnectionPool{Mode: AGRESSIVE, Size: 0, Urls: []string{}, Logger: redis_pool_logger}
		defer pool.Close()

		// Shouldn't have any errors
		err := pool.Open()
		c.Expect(err, gospec.Equals, nil)

		// Should be open
		c.Expect(pool.IsOpen(), gospec.Equals, true)
		c.Expect(pool.IsClosed(), gospec.Equals, false)

		// Should be empty
		c.Expect(pool.Len(), gospec.Equals, 0)

		var connection *RedisConnection
		connection, err = pool.Pop()
		c.Expect(err, gospec.Equals, ErrNoConnectionsAvailable)
		c.Expect(connection, gospec.Satisfies, nil == connection)
	})

	c.Specify("[RedisConnectionPool] Opening connection to Invalid Host/Port has errors", func() {
		pool := RedisConnectionPool{Mode: AGRESSIVE, Size: 1, Urls: []string{"127.0.0.1:6991"}, Logger: redis_pool_logger}
		defer pool.Close()

		// Should have an error
		err := pool.Open()
		c.Expect(err, gospec.Satisfies, err != nil)

		// Should be closed
		c.Expect(pool.IsClosed(), gospec.Equals, true)
		c.Expect(pool.Len(), gospec.Equals, -1)
	})

	c.Specify("[RedisConnectionPool] Opening connection to Valid Host/Port has no errors", func() {
		pool := RedisConnectionPool{Mode: AGRESSIVE, Size: 1, Urls: []string{"127.0.0.1:6992"}, Logger: redis_pool_logger}
		defer pool.Close()

		// Start the server ...
		cmd := exec.Command("redis-server", "--port", "6992")
		err := cmd.Start()
		c.Expect(err, gospec.Equals, nil)
		if err != nil {
			// Abort on errors
			return
		}
		time.Sleep(time.Duration(1) * time.Second)
		defer cmd.Wait()
		defer cmd.Process.Kill()

		err = pool.Open()
		c.Expect(err, gospec.Equals, nil)

		c.Expect(pool.IsOpen(), gospec.Equals, true)
		c.Expect(pool.IsClosed(), gospec.Equals, false)
		c.Expect(pool.IsClosed(), gospec.Satisfies, pool.IsOpen() != pool.IsClosed())
	})

	c.Specify("[RedisConnectionPool] 10x AGRESSIVE Pool Pops 10x open connections", func() {
		pool := RedisConnectionPool{Mode: AGRESSIVE, Size: 10, Urls: []string{"127.0.0.1:6993"}, Logger: redis_pool_logger}
		defer pool.Close()

		// Start the server ...
		cmd := exec.Command("redis-server", "--port", "6993")
		err := cmd.Start()
		c.Expect(err, gospec.Equals, nil)
		if err != nil {
			// Abort on errors
			return
		}
		time.Sleep(time.Duration(1) * time.Second)
		defer cmd.Wait()
		defer cmd.Process.Kill()

		err = pool.Open()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(pool.IsOpen(), gospec.Equals, true)

		// Has 10x connections
		var connection *RedisConnection

		for count := 10; count > 0; count-- {
			// Count decrements when the connection is pop'd
			c.Expect(pool.Len(), gospec.Equals, count)
			connection, err = pool.Pop()
			c.Expect(pool.Len(), gospec.Equals, count-1)

			// Expecting an open connection
			c.Expect(err, gospec.Equals, nil)
			c.Expect(connection, gospec.Satisfies, connection != nil)
			c.Expect(connection.IsOpen(), gospec.Equals, true)
		}
	})

	c.Specify("[RedisConnectionPool] 10x LAZY Pool Pops 10x closed connections", func() {
		pool := RedisConnectionPool{Mode: LAZY, Size: 10, Urls: []string{"127.0.0.1:6994"}, Logger: redis_pool_logger}
		defer pool.Close()

		// Start the server ...
		cmd := exec.Command("redis-server", "--port", "6994")
		err := cmd.Start()
		c.Expect(err, gospec.Equals, nil)
		if err != nil {
			// Abort on errors
			return
		}
		time.Sleep(time.Duration(1) * time.Second)
		defer cmd.Wait()
		defer cmd.Process.Kill()

		err = pool.Open()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(pool.IsOpen(), gospec.Equals, true)

		// Has 10x connections
		var connection *RedisConnection

		for count := 10; count > 0; count-- {
			// Count decrements when the connection is pop'd
			c.Expect(pool.Len(), gospec.Equals, count)
			connection, err = pool.Pop()
			c.Expect(pool.Len(), gospec.Equals, count-1)

			// Expecting an open connection
			c.Expect(err, gospec.Equals, nil)
			c.Expect(connection, gospec.Satisfies, connection != nil)
			c.Expect(connection.IsClosed(), gospec.Equals, true)
		}
	})
}
