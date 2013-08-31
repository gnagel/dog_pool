package dog_pool

// import "os/exec"
// import "time"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

// import "github.com/alecthomas/log4go"

//
// NOTE: Use differient ports for each test!
//       gospec runs the specs in parallel!
//
func TestThriftPoolSpecs(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	// r.AddSpec(ThriftPoolSpecs)
	gospec.MainGoTest(r, t)
}

// // Helpers
// func ThriftPoolSpecs(c gospec.Context) {
// 	var thrift_pool_logger = log4go.NewDefaultLogger(log4go.CRITICAL)
//
// 	c.Specify("[ThriftConnectionPool] New Pool is not open", func() {
// 		pool := ThriftConnectionPool{Mode: AGRESSIVE, Size: 0, Urls: []string{}, Logger: thrift_pool_logger}
// 		defer pool.Close()
//
// 		c.Expect(pool.IsOpen(), gospec.Equals, false)
// 		c.Expect(pool.IsClosed(), gospec.Equals, true)
// 		c.Expect(pool.IsOpen(), gospec.Satisfies, pool.IsOpen() != pool.IsClosed())
// 		c.Expect(pool.Len(), gospec.Equals, -1)
// 	})
//
// 	c.Specify("[ThriftConnectionPool] Opening a Pool with Undefined Mode has errors", func() {
// 		pool := ThriftConnectionPool{Mode: 0, Size: 0, Urls: []string{}, Logger: thrift_pool_logger}
// 		defer pool.Close()
//
// 		// Should have an error
// 		err := pool.Open()
// 		c.Expect(err, gospec.Satisfies, err != nil)
//
// 		// Should be closed
// 		c.Expect(pool.IsClosed(), gospec.Equals, true)
// 		c.Expect(pool.Len(), gospec.Equals, -1)
// 	})
//
// 	c.Specify("[ThriftConnectionPool] Size=0 pool is Empty", func() {
// 		pool := ThriftConnectionPool{Mode: AGRESSIVE, Size: 0, Urls: []string{}, Logger: thrift_pool_logger}
// 		defer pool.Close()
//
// 		// Shouldn't have any errors
// 		err := pool.Open()
// 		c.Expect(err, gospec.Equals, nil)
//
// 		// Should be open
// 		c.Expect(pool.IsOpen(), gospec.Equals, true)
// 		c.Expect(pool.IsClosed(), gospec.Equals, false)
//
// 		// Should be empty
// 		c.Expect(pool.Len(), gospec.Equals, 0)
// 	})
//
// 	c.Specify("[ThriftConnectionPool] Pop from empty pool returns error", func() {
// 		pool := ThriftConnectionPool{Mode: AGRESSIVE, Size: 0, Urls: []string{}, Logger: thrift_pool_logger}
// 		defer pool.Close()
//
// 		// Shouldn't have any errors
// 		err := pool.Open()
// 		c.Expect(err, gospec.Equals, nil)
//
// 		// Should be open
// 		c.Expect(pool.IsOpen(), gospec.Equals, true)
// 		c.Expect(pool.IsClosed(), gospec.Equals, false)
//
// 		// Should be empty
// 		c.Expect(pool.Len(), gospec.Equals, 0)
//
// 		var connection *ThriftConnection
// 		connection, err = pool.Pop()
// 		c.Expect(err, gospec.Equals, ErrNoConnectionsAvailable)
// 		c.Expect(connection, gospec.Satisfies, nil == connection)
// 	})
//
// 	c.Specify("[ThriftConnectionPool] Opening connection to Invalid Host/Port has errors", func() {
// 		pool := ThriftConnectionPool{Mode: AGRESSIVE, Size: 1, Urls: []string{"127.0.0.1:6991"}, Logger: thrift_pool_logger}
// 		defer pool.Close()
//
// 		// Should have an error
// 		err := pool.Open()
// 		c.Expect(err, gospec.Satisfies, err != nil)
//
// 		// Should be closed
// 		c.Expect(pool.IsClosed(), gospec.Equals, true)
// 		c.Expect(pool.Len(), gospec.Equals, -1)
// 	})
//
// 	c.Specify("[ThriftConnectionPool] Opening connection to Valid Host/Port has no errors", func() {
// 		pool := ThriftConnectionPool{Mode: AGRESSIVE, Size: 1, Urls: []string{"127.0.0.1:6992"}, Logger: thrift_pool_logger}
// 		defer pool.Close()
//
// 		// Start the server ...
// 		cmd := exec.Command("thrift-server", "--port", "6992")
// 		err := cmd.Start()
// 		c.Expect(err, gospec.Equals, nil)
// 		if err != nil {
// 			// Abort on errors
// 			return
// 		}
// 		time.Sleep(time.Duration(1) * time.Second)
// 		defer cmd.Wait()
// 		defer cmd.Process.Kill()
//
// 		err = pool.Open()
// 		c.Expect(err, gospec.Equals, nil)
//
// 		c.Expect(pool.IsOpen(), gospec.Equals, true)
// 		c.Expect(pool.IsClosed(), gospec.Equals, false)
// 		c.Expect(pool.IsClosed(), gospec.Satisfies, pool.IsOpen() != pool.IsClosed())
// 	})
//
// 	c.Specify("[ThriftConnectionPool] 10x AGRESSIVE Pool Pops 10x open connections", func() {
// 		pool := ThriftConnectionPool{Mode: AGRESSIVE, Size: 10, Urls: []string{"127.0.0.1:6993"}, Logger: thrift_pool_logger}
// 		defer pool.Close()
//
// 		// Start the server ...
// 		cmd := exec.Command("thrift-server", "--port", "6993")
// 		err := cmd.Start()
// 		c.Expect(err, gospec.Equals, nil)
// 		if err != nil {
// 			// Abort on errors
// 			return
// 		}
// 		time.Sleep(time.Duration(1) * time.Second)
// 		defer cmd.Wait()
// 		defer cmd.Process.Kill()
//
// 		err = pool.Open()
// 		c.Expect(err, gospec.Equals, nil)
// 		c.Expect(pool.IsOpen(), gospec.Equals, true)
//
// 		// Has 10x connections
// 		var connection *ThriftConnection
//
// 		for count := 10; count > 0; count-- {
// 			// Count decrements when the connection is pop'd
// 			c.Expect(pool.Len(), gospec.Equals, count)
// 			connection, err = pool.Pop()
// 			c.Expect(pool.Len(), gospec.Equals, count-1)
//
// 			// Expecting an open connection
// 			c.Expect(err, gospec.Equals, nil)
// 			c.Expect(connection, gospec.Satisfies, connection != nil)
// 			c.Expect(connection.IsOpen(), gospec.Equals, true)
// 		}
// 	})
//
// 	c.Specify("[ThriftConnectionPool] 10x LAZY Pool Pops 10x closed connections", func() {
// 		pool := ThriftConnectionPool{Mode: LAZY, Size: 10, Urls: []string{"127.0.0.1:6994"}, Logger: thrift_pool_logger}
// 		defer pool.Close()
//
// 		// Start the server ...
// 		cmd := exec.Command("thrift-server", "--port", "6994")
// 		err := cmd.Start()
// 		c.Expect(err, gospec.Equals, nil)
// 		if err != nil {
// 			// Abort on errors
// 			return
// 		}
// 		time.Sleep(time.Duration(1) * time.Second)
// 		defer cmd.Wait()
// 		defer cmd.Process.Kill()
//
// 		err = pool.Open()
// 		c.Expect(err, gospec.Equals, nil)
// 		c.Expect(pool.IsOpen(), gospec.Equals, true)
//
// 		// Has 10x connections
// 		var connection *ThriftConnection
//
// 		for count := 10; count > 0; count-- {
// 			// Count decrements when the connection is pop'd
// 			c.Expect(pool.Len(), gospec.Equals, count)
// 			connection, err = pool.Pop()
// 			c.Expect(pool.Len(), gospec.Equals, count-1)
//
// 			// Expecting an open connection
// 			c.Expect(err, gospec.Equals, nil)
// 			c.Expect(connection, gospec.Satisfies, connection != nil)
// 			c.Expect(connection.IsClosed(), gospec.Equals, true)
// 		}
// 	})
// }
