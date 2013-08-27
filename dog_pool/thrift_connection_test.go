package dog_pool

// import "os/exec"
// import "time"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"

//
// NOTE: Use differient table name for each test!
//       gospec runs the specs in parallel!
//
func TestThriftConnectionSpecs(t *testing.T) {
	r := gospec.NewRunner()
	// r.AddSpec(ThriftConnectionSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func ThriftConnectionSpecs(c gospec.Context) {
	var thrift_connection_logger = log4go.NewDefaultLogger(log4go.CRITICAL)

	c.Specify("[ThriftConnection] New connection is not open", func() {
		connection := ThriftConnection{Url: "127.0.0.1:9090", Logger: &thrift_connection_logger}
		defer connection.Close()

		open := connection.IsOpen()
		closed := connection.IsClosed()

		// Should be opposite of each other:
		c.Expect(open, gospec.Equals, false)
		c.Expect(closed, gospec.Equals, true)
		c.Expect(closed, gospec.Satisfies, open != closed)
	})

	c.Specify("[ThriftConnection] Opening connection to Invalid Host/Port has errors", func() {
		connection := ThriftConnection{Url: "127.0.0.1:9091", Logger: &thrift_connection_logger}
		defer connection.Close()

		// The server is not running ...
		// This should return an error
		err := connection.Open()
		c.Expect(err, gospec.Satisfies, err != nil)

		closed := connection.IsClosed()
		c.Expect(closed, gospec.Equals, true)
	})

	c.Specify("[ThriftConnection] Opening connection to Valid Host/Port has no errors", func() {
		connection := ThriftConnection{Url: "127.0.0.1:9090", Logger: &thrift_connection_logger}
		defer connection.Close()

		err := connection.Open()
		c.Expect(err, gospec.Equals, nil)

		open := connection.IsOpen()
		closed := connection.IsClosed()
		c.Expect(open, gospec.Equals, true)
		c.Expect(closed, gospec.Equals, false)
		c.Expect(closed, gospec.Satisfies, open != closed)
	})

	c.Specify("[ThriftConnection] GetTableNames opens the connection automatically", func() {
		connection := ThriftConnection{Url: "127.0.0.1:9090", Logger: &thrift_connection_logger}
		defer connection.Close()

		// Starts off closed ...
		c.Expect(connection.IsClosed(), gospec.Equals, true)

		// Ping the server
		// Should now be open
		_, err := connection.GetTableNames()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsOpen(), gospec.Equals, true)

		// Close the connection
		err = connection.Close()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsClosed(), gospec.Equals, true)

		// Ping the server again
		// Should now be open again
		_, err = connection.GetTableNames()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(connection.IsOpen(), gospec.Equals, true)
	})
}
