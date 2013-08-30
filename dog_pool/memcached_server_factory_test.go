package dog_pool

import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"

//
// NOTE: Use differient ports for each test!
//       gospec runs the specs in parallel!
//
func TestMemcachedServerProcessSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.AddSpec(MemcachedServerProcessSpecs)
	gospec.MainGoTest(r, t)
}

func MemcachedServerProcessSpecs(c gospec.Context) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)

	c.Specify("[MemcachedServerProcess] Starts a new Memcached-Server", func() {
		server, err := StartMemcachedServer(&logger)
		defer server.Close()

		c.Expect(err, gospec.Equals, nil)
		c.Expect(server, gospec.Satisfies, server != nil)
		c.Expect(server.logger, gospec.Equals, &logger)
		c.Expect(server.port, gospec.Satisfies, server.port >= 1024)
		c.Expect(server.cmd, gospec.Satisfies, nil != server.cmd)
		c.Expect(server.connection, gospec.Satisfies, nil == server.connection)
	})

	c.Specify("[MemcachedServerProcess] Creates a connection to a Memcached-Server", func() {
		server, err := StartMemcachedServer(&logger)
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
