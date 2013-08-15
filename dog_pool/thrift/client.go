//
// Extensions for the generated hbase.go thrift API
//

package thrift

import "net"
import "net/rpc"
import "github.com/samuel/go-thrift/thrift"

// 
// ========================================
// 
// Extension to (generated) Hbase interface
// 
// ========================================
// 
type ThriftHbase interface {
	Hbase

	IsOpen() bool
	IsClosed() bool

	Open(url string) error
	Close() error
}

// 
// ========================================
// 
// Implementation of ThriftHbase for HbaseClient
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
func (s *HbaseClient) Open(url string) error {
	// Connect to the server
	conn, err := net.Dial("tcp", url)
	if err != nil {
		return err
	}

	readwrite := thrift.NewFramedReadWriteCloser(conn, 0)
	
	// Strict Write = true
	// Strict Read = false
	protocol := thrift.NewBinaryProtocol(true, false)
	s.Client = thrift.NewClient(readwrite, protocol, true)

	return nil
}
