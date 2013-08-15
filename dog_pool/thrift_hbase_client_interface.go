//
// Thrift+Hbase Client Interface
//
// Interface implemented by thrift.Hbase and dog_pool.ThriftHbaseConnection
//

package dog_pool

import "./thrift_hbase"

type ThriftHbaseClientInterface interface {
	// Implemenent all of the client methods
	thrift_hbase.Hbase

	// Plus these methods too ...

	// Is the connection open?
	IsOpen() bool
	// Is the connection closed?
	IsClosed() bool

	// Open the connection, return error on failure
	Open(url string) error

	// Close the connection
	Close() error
}
