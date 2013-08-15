//
// Redis Client Interface
//
// Interface implemented by redis.Client and dog_pool.RedisConnection
//

package dog_pool

import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "./thrift_hbase"

func TestThriftHbaseClientInterfaceSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.AddSpec(ThriftHbaseClientInterfaceSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func ThriftHbaseClientInterfaceSpecs(c gospec.Context) {
	c.Specify("[ThriftHbaseClientInterface] ThriftHbaseConnection satisfies ThriftHbaseClientInterface", func() {
		connection := &ThriftHbaseConnection{}

		// Wont' compile unless it implements the interface
		var thrifthbase_interface ThriftHbaseClientInterface = connection
		c.Expect(thrifthbase_interface, gospec.Satisfies, true)
	})

	c.Specify("[ThriftHbaseClientInterface] thrift.HbaseClient satisfies ThriftHbaseClientInterface", func() {
		client := &thrift_hbase.HbaseClient{}

		// Wont' compile unless it implements the interface
		var thrifthbase_interface ThriftHbaseClientInterface = client
		c.Expect(thrifthbase_interface, gospec.Satisfies, true)
	})
}
