//
// Thrift+Hbase Client Interface
//
// Interface implemented by thrift.Hbase and dog_pool.ThriftConnection
//

package dog_pool

import "./thrift"
import goh_hbase "github.com/sdming/goh/Hbase"

type ThriftClientInterface interface {
	// Implemenent all of the client methods
	go_hase.IHbase

	// Plus these methods too ...


	// Open the connection
	Open() error

	// Close the connection
	Close() error
}
