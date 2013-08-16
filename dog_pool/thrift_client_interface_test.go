//
// Thrift Client Interface
//
// Interface implemented by thrift.HbaseClient and dog_pool.ThriftConnection
//

package dog_pool

import "testing"
import "github.com/orfjackal/gospec/src/gospec"

// import "./thrift"

func TestThriftClientInterfaceSpecs(t *testing.T) {
	r := gospec.NewRunner()
	// r.AddSpec(ThriftClientInterfaceSpecs)
	gospec.MainGoTest(r, t)
}

// // Helpers
// func ThriftClientInterfaceSpecs(c gospec.Context) {
// 	c.Specify("[ThriftClientInterface] ThriftConnection satisfies ThriftClientInterface", func() {
// 		connection := &ThriftConnection{}
//
// 		// Wont' compile unless it implements the interface
// 		var thrift_interface ThriftClientInterface = connection
// 		c.Expect(thrift_interface, gospec.Satisfies, true)
// 	})
//
// 	c.Specify("[ThriftClientInterface] thrift.HbaseClient satisfies ThriftClientInterface", func() {
// 		client := &thrift.HbaseClient{}
//
// 		// Wont' compile unless it implements the interface
// 		var thrift_interface ThriftClientInterface = client
// 		c.Expect(thrift_interface, gospec.Satisfies, true)
// 	})
// }
