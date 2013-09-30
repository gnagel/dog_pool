package dog_pool

import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"

func TestRedisServerProcessSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisServerProcessSpecs)
	gospec.MainGoTest(r, t)
}

func RedisServerProcessSpecs(c gospec.Context) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)

	c.Specify("[RedisServerProcess] Starts a new Redis-Server", func() {
		server, err := StartRedisServer(&logger)
		defer server.Close()

		c.Expect(err, gospec.Equals, nil)
		c.Expect(server, gospec.Satisfies, server != nil)
		c.Expect(server.logger, gospec.Equals, &logger)
		c.Expect(server.port, gospec.Satisfies, server.port >= 1024)
		c.Expect(server.cmd, gospec.Satisfies, nil != server.cmd)
		c.Expect(server.connection, gospec.Satisfies, nil == server.connection)
	})

	c.Specify("[RedisServerProcess] Creates a connection to a Redis-Server", func() {
		server, err := StartRedisServer(&logger)
		defer server.Close()

		c.Expect(err, gospec.Equals, nil)
		c.Expect(server, gospec.Satisfies, server != nil)

		connection := server.Connection()
		c.Expect(connection, gospec.Satisfies, nil != connection)
		c.Expect(server.connection, gospec.Equals, connection)

		err = connection.Open()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsOpen(), gospec.Equals, true)
	})
}
