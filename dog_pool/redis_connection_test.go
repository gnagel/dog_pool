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
func TestRedisConnectionSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.AddSpec(RedisConnectionSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisConnectionSpecs(c gospec.Context) {
	var redis_connection_logger = log4go.NewDefaultLogger(log4go.FINE)

	c.Specify("[RedisConnection] New connection is not open", func() {
		connection := RedisConnection{Url: "127.0.0.1:6990", Logger: &redis_connection_logger}
		defer connection.Close()

		open := connection.IsOpen()
		closed := connection.IsClosed()

		// Should be opposite of each other:
		c.Expect(open, gospec.Equals, false)
		c.Expect(closed, gospec.Equals, true)
		c.Expect(closed, gospec.Satisfies, open != closed)
	})

	c.Specify("[RedisConnection] Opening connection to Invalid Host/Port has errors", func() {
		connection := RedisConnection{Url: "127.0.0.1:6991", Logger: &redis_connection_logger}
		defer connection.Close()

		// The server is not running ...
		// This should return an error
		err := connection.Open()
		c.Expect(err, gospec.Satisfies, err != nil)

		closed := connection.IsClosed()
		c.Expect(closed, gospec.Equals, true)
	})

	c.Specify("[RedisConnection] Opening connection to Valid Host/Port has no errors", func() {
		connection := RedisConnection{Url: "127.0.0.1:6992", Logger: &redis_connection_logger}
		defer connection.Close()

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

		err = connection.Open()
		c.Expect(err, gospec.Equals, nil)

		open := connection.IsOpen()
		closed := connection.IsClosed()
		c.Expect(open, gospec.Equals, true)
		c.Expect(closed, gospec.Equals, false)
		c.Expect(closed, gospec.Satisfies, open != closed)
	})

	c.Specify("[RedisConnection] Ping (-->Cmd-->Append+GetReply) (re-)opens the connection automatically", func() {
		connection := RedisConnection{Url: "127.0.0.1:6993", Logger: &redis_connection_logger}
		defer connection.Close()

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

		// Starts off closed ...
		c.Expect(connection.IsClosed(), gospec.Equals, true)

		// Ping the server
		// Should now be open
		err = connection.Ping()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsOpen(), gospec.Equals, true)

		// Close the connection
		err = connection.Close()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsClosed(), gospec.Equals, true)

		// Ping the server again
		// Should now be open again
		err = connection.Ping()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsOpen(), gospec.Equals, true)
	})

	c.Specify("[RedisConnection] BatchCommands batches commands w/o Multi Exec or transactions", func() {
		connection := RedisConnection{Url: "127.0.0.1:6994", Logger: &redis_connection_logger}
		defer connection.Close()

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

		// Starts off closed ...
		c.Expect(connection.IsClosed(), gospec.Equals, true)

		commands := make([]*RedisBatchCommand, 4)
		commands[0] = &RedisBatchCommand{Cmd: "SET", Args: []string{"BOB", "1"}}
		commands[1] = &RedisBatchCommand{Cmd: "GET", Args: []string{"BOB"}}
		commands[2] = &RedisBatchCommand{Cmd: "DEL", Args: []string{"BOB"}}
		commands[3] = &RedisBatchCommand{Cmd: "GET", Args: []string{"MISS"}}

		// Execute the commands
		// Should now be open
		err = connection.BatchCommands(commands)
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsOpen(), gospec.Equals, true)

	})

	c.Specify("[RedisConnection] Ping to invalid Host/Port has errors", func() {
		connection := RedisConnection{Url: "127.0.0.1:6995", Logger: &redis_connection_logger}
		defer connection.Close()

		// Start the server ...
		cmd := exec.Command("redis-server", "--port", "6995")
		err := cmd.Start()
		c.Expect(err, gospec.Equals, nil)
		if err != nil {
			// Abort on errors
			return
		}
		time.Sleep(time.Duration(1) * time.Second)
		// Defer the evaluation of cmd
		defer func() { cmd.Wait() }()
		defer func() { cmd.Process.Kill() }()

		// Starts off closed ...
		c.Expect(connection.IsClosed(), gospec.Equals, true)

		// Ping the server
		// Should now be open
		err = connection.Ping()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsOpen(), gospec.Equals, true)

		// Kill the server
		cmd.Process.Kill()
		cmd.Wait()

		// Ping the server again
		// Should return an error and now be closed
		err = connection.Ping()
		c.Expect(err, gospec.Satisfies, err != nil)
		c.Expect(connection.IsClosed(), gospec.Equals, true)

		// Re-Start the server ...
		cmd = exec.Command("redis-server", "--port", "6995")
		err = cmd.Start()
		c.Expect(err, gospec.Equals, nil)
		if err != nil {
			// Abort on errors
			return
		}
		time.Sleep(time.Duration(1) * time.Second)

		// Ping the server
		// Should now be open
		err = connection.Ping()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsOpen(), gospec.Equals, true)
	})
}
