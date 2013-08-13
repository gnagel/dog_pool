package dog_pool

import "os/exec"
import "time"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"

//
// NOTE: Use differient ports for each test!
//       gospec runs the specs in parallel!
//
func TestMemcachedConnectionSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.AddSpec(MemcachedConnectionSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func MemcachedConnectionSpecs(c gospec.Context) {
	var memcached_connection_logger = log4go.NewDefaultLogger(log4go.FINEST)

	c.Specify("[MemcachedConnection] New connection is not open", func() {
		connection := MemcachedConnection{Url: "127.0.0.1:11290", Logger: &memcached_connection_logger}
		defer connection.Close()

		open := connection.IsOpen()
		closed := connection.IsClosed()

		// Should be opposite of each other:
		c.Expect(open, gospec.Equals, false)
		c.Expect(closed, gospec.Equals, true)
		c.Expect(closed, gospec.Satisfies, open != closed)
	})

	c.Specify("[MemcachedConnection] Opening connection to Invalid Host/Port has errors", func() {
		connection := MemcachedConnection{Url: "127.0.0.1:11291", Logger: &memcached_connection_logger}
		defer connection.Close()

		// The server is not running ...
		// This should return an error
		err := connection.Open()
		c.Expect(err, gospec.Satisfies, err != nil)

		closed := connection.IsClosed()
		c.Expect(closed, gospec.Equals, true)
	})

	c.Specify("[MemcachedConnection] Opening connection to Valid Host/Port has no errors", func() {
		connection := MemcachedConnection{Url: "127.0.0.1:11292", Logger: &memcached_connection_logger}
		defer connection.Close()

		// Start the server ...
		cmd := exec.Command("memcached", "-p", "11292")
		err := cmd.Start()
		c.Expect(err, gospec.Equals, nil)
		if err != nil {
			// Abort on errors
			return
		}
		time.Sleep(time.Duration(1) * time.Second)
		defer cmd.Wait()
		defer cmd.Process.Kill()

		err = connection.Open()
		c.Expect(err, gospec.Equals, nil)

		open := connection.IsOpen()
		closed := connection.IsClosed()
		c.Expect(open, gospec.Equals, true)
		c.Expect(closed, gospec.Equals, false)
		c.Expect(closed, gospec.Satisfies, open != closed)
	})

	// c.Specify("[MemcachedConnection] Ping (-->Cmd-->Append+GetReply) (re-)opens the connection automatically", func() {
	// 	connection := MemcachedConnection{Url: "127.0.0.1:11293", Logger: &memcached_connection_logger}
	// 	defer connection.Close()
	//
	// 	// Start the server ...
	// 	cmd := exec.Command("memcached", "-p", "11293")
	// 	err := cmd.Start()
	// 	c.Expect(err, gospec.Equals, nil)
	// 	if err != nil {
	// 		// Abort on errors
	// 		return
	// 	}
	// 	time.Sleep(time.Duration(1) * time.Second)
	// 	defer cmd.Wait()
	// 	defer cmd.Process.Kill()
	//
	// 	// Starts off closed ...
	// 	c.Expect(connection.IsClosed(), gospec.Equals, true)
	//
	// 	// Ping the server
	// 	// Should now be open
	// 	err = connection.Ping()
	// 	c.Expect(err, gospec.Equals, nil)
	// 	c.Expect(connection.IsOpen(), gospec.Equals, true)
	//
	// 	// Close the connection
	// 	err = connection.Close()
	// 	c.Expect(err, gospec.Equals, nil)
	// 	c.Expect(connection.IsClosed(), gospec.Equals, true)
	//
	// 	// Ping the server again
	// 	// Should now be open again
	// 	err = connection.Ping()
	// 	c.Expect(err, gospec.Equals, nil)
	// 	c.Expect(connection.IsOpen(), gospec.Equals, true)
	// })
	//
	// c.Specify("[MemcachedConnection] Ping to invalid Host/Port has errors", func() {
	// 	connection := MemcachedConnection{Url: "127.0.0.1:11294", Logger: &memcached_connection_logger}
	// 	defer connection.Close()
	//
	// 	// Start the server ...
	// 	cmd := exec.Command("memcached", "-p", "11294")
	// 	err := cmd.Start()
	// 	c.Expect(err, gospec.Equals, nil)
	// 	if err != nil {
	// 		// Abort on errors
	// 		return
	// 	}
	// 	time.Sleep(time.Duration(1) * time.Second)
	// 	// Defer the evaluation of cmd
	// 	defer func() { cmd.Wait() }()
	// 	defer func() { cmd.Process.Kill() }()
	//
	// 	// Starts off closed ...
	// 	c.Expect(connection.IsClosed(), gospec.Equals, true)
	//
	// 	// Ping the server
	// 	// Should now be open
	// 	err = connection.Ping()
	// 	c.Expect(err, gospec.Equals, nil)
	// 	c.Expect(connection.IsOpen(), gospec.Equals, true)
	//
	// 	// Kill the server
	// 	cmd.Process.Kill()
	// 	cmd.Wait()
	//
	// 	// Ping the server again
	// 	// Should return an error and now be closed
	// 	err = connection.Ping()
	// 	c.Expect(err, gospec.Satisfies, err != nil)
	// 	c.Expect(connection.IsClosed(), gospec.Equals, true)
	//
	// 	// Re-Start the server ...
	// 	cmd = exec.Command("memcached-server", "--port", "11294")
	// 	err = cmd.Start()
	// 	c.Expect(err, gospec.Equals, nil)
	// 	if err != nil {
	// 		// Abort on errors
	// 		return
	// 	}
	// 	time.Sleep(time.Duration(1) * time.Second)
	//
	// 	// Ping the server
	// 	// Should now be open
	// 	err = connection.Ping()
	// 	c.Expect(err, gospec.Equals, nil)
	// 	c.Expect(connection.IsOpen(), gospec.Equals, true)
	// })
}
