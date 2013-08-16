//
// Extensions for the generated hbase.go thrift API
//

package thrift

import "time"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

//
// NOTE: Use differient table names in each test!
//       gospec runs the specs in parallel!
//
func TestHbaseClientSpecs(t *testing.T) {
	//
	// WARNING: This test assumes HBase & Thrift are already running!
	//

	r := gospec.NewRunner()
	r.AddSpec(HbaseClientSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func HbaseClientSpecs(c gospec.Context) {
	c.Specify("[HbaseClient] New Connection is not open", func() {
		connection := &HbaseClient{}
		defer connection.Close()

		c.Expect(connection.IsOpen(), gospec.Equals, false)
		c.Expect(connection.IsClosed(), gospec.Equals, true)
		c.Expect(connection.IsOpen(), gospec.Satisfies, connection.IsOpen() != connection.IsClosed())
	})

	c.Specify("[HbaseClient] Opening connection to Invalid Host/Port has errors", func() {
		connection := &HbaseClient{}
		defer connection.Close()

		// The server is not running ...
		// This should return an error
		err := connection.Open("127.0.0.1:9099", time.Duration(1)*time.Second)
		c.Expect(err, gospec.Satisfies, err != nil)

		closed := connection.IsClosed()
		c.Expect(closed, gospec.Equals, true)
	})

	c.Specify("[HbaseClient] Opening connection to Valid Host/Port has no errors", func() {
		connection := &HbaseClient{}
		defer connection.Close()

		err := connection.Open("127.0.0.1:9090", time.Duration(1)*time.Second)
		c.Expect(err, gospec.Equals, nil)

		open := connection.IsOpen()
		closed := connection.IsClosed()
		c.Expect(open, gospec.Equals, true)
		c.Expect(closed, gospec.Equals, false)
		c.Expect(closed, gospec.Satisfies, open != closed)
	})

	c.Specify("[HbaseClient] GetTableNames has no errors", func() {
		connection := &HbaseClient{}
		defer connection.Close()

		err := connection.Open("127.0.0.1:9090", time.Duration(1)*time.Second)
		c.Expect(err, gospec.Equals, nil)

		var ok bool
		ok, err = connection.IsTableEnabled("bob")
		c.Expect(ok, gospec.Equals, false)
		c.Expect(err, gospec.Satisfies, nil != err)

		// var tables []string
		// tables, err = connection.GetTableNames()
		// c.Expect(err, gospec.Equals, nil)
		// c.Expect(tables, gospec.Satisfies, tables != nil)
	})
}
