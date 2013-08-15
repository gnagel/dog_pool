//
// Thrift+Hbase Client Interface
//
// Interface implemented by thrift.Hbase and dog_pool.ThriftHbaseConnection
//

package dog_pool

import "./thrift"
import "github.com/fzzy/radix/redis"

// Alias to the Hbase client
type ThriftHbaseClientInterface interface {
	ThriftHbase
}
