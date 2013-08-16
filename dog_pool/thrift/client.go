//
// Extensions for the generated hbase.go thrift API
//

package thrift

import "net"
import "net/rpc"
import "time"
import "github.com/samuel/go-thrift/thrift"

//
// ========================================
//
// Extend HbaseClient to implement ThriftHbaseClientInterface
//
// ========================================
//

// Is the connection open?
func (s *HbaseClient) IsOpen() bool {
	return nil != s.Client
}

// Is the connection closed?
func (s *HbaseClient) IsClosed() bool {
	return nil == s.Client
}

// Close the connection
func (s *HbaseClient) Close() error {
	if s.IsClosed() {
		return nil
	}

	client, _ := s.Client.(*rpc.Client)
	s.Client = nil

	return client.Close()
}

// Create the thrift client to connect to the server
func (s *HbaseClient) Open(url string, timeout time.Duration) error {
	// Connect to the server
	conn, err := net.DialTimeout("tcp", url, timeout)
	if err != nil {
		return err
	}

	conn.SetDeadline(time.Now().Add(time.Duration(100) * time.Millisecond))

	readwrite := thrift.NewFramedReadWriteCloser(conn, 1024)

	// Strict Write = true
	// Strict Read = false
	// protocol := thrift.NewBinaryProtocol(true, false)
	protocol := thrift.NewCompactProtocol()
	s.Client = thrift.NewClient(readwrite, protocol, false)

	return nil
}
