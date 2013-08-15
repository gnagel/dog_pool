//
// Thrift Connection Wrapper written in GO
//

package dog_pool

import "time"
import "github.com/alecthomas/log4go"
import "./thrift"

//
// Connection Wrapper for Thrift
//
type ThriftConnection struct {
	Url string "Thrift URL this factory will connect to"

	Id string "(optional) Identifier for distingushing between thrift connections"

	Logger *log4go.Logger "Handle to the logger we are using"

	Timeout time.Duration "Connection Timeout"

	client *thrift.HbaseClient "Connection to a Thrift, may be nil"
}

//
// Lazily make a Thrift Connection
//
func makeLazyThriftConnection(url string, id string, timeout time.Duration, logger *log4go.Logger) (*ThriftConnection, error) {
	// Create a new factory instance
	p := &ThriftConnection{Url: url, Id: id, Logger: logger, Timeout: timeout}

	// Return the factory
	return p, nil
}

//
// Agressively make a Thrift Connection
//
func makeAgressiveThriftConnection(url string, id string, timeout time.Duration, logger *log4go.Logger) (*ThriftConnection, error) {
	// Create a new factory instance
	p, _ := makeLazyThriftConnection(url, id, timeout, logger)

	// Ping the server
	if err := p.Ping(); nil != err {
		// Close the connection
		p.Close()

		// Return the error
		return nil, err
	}

	// Return the factory
	return p, nil
}

//
//  ========================================
//
// ThriftClientInterface -and- thrift.HbaseClient implementation:
//
//  ========================================
//

func (p *ThriftConnection) AtomicIncrement(TableName []byte, Row []byte, Column []byte, Value int64) (int64, error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	// TODO
	output, err := p.client.AtomicIncrement(TableName, Row, Column, Value)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) Compact(TableNameOrRegionName []byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.Compact(TableNameOrRegionName)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) CreateTable(TableName []byte, ColumnFamilies []*thrift.ColumnDescriptor) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.CreateTable(TableName, ColumnFamilies)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) DeleteAll(TableName []byte, Row []byte, Column []byte, Attributes map[string][]byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.DeleteAll(TableName, Row, Column, Attributes)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) DeleteAllRow(TableName []byte, Row []byte, Attributes map[string][]byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.DeleteAllRow(TableName, Row, Attributes)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) DeleteAllRowTs(TableName []byte, Row []byte, Timestamp int64, Attributes map[string][]byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.DeleteAllRowTs(TableName, Row, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) DeleteAllTs(TableName []byte, Row []byte, Column []byte, Timestamp int64, Attributes map[string][]byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.DeleteAllTs(TableName, Row, Column, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) DeleteTable(TableName []byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.DeleteTable(TableName)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) DisableTable(TableName []byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.DisableTable(TableName)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) EnableTable(TableName []byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.EnableTable(TableName)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) Get(TableName []byte, Row []byte, Column []byte, Attributes map[string][]byte) ([]*thrift.TCell, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.Get(TableName, Row, Column, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetColumnDescriptors(TableName []byte) (map[string]*thrift.ColumnDescriptor, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetColumnDescriptors(TableName)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRegionInfo(Row []byte) (*thrift.TRegionInfo, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRegionInfo(Row)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRow(TableName []byte, Row []byte, Attributes map[string][]byte) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRow(TableName, Row, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRowOrBefore(TableName []byte, Row []byte, Family []byte) ([]*thrift.TCell, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRowOrBefore(TableName, Row, Family)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRowTs(TableName []byte, Row []byte, Timestamp int64, Attributes map[string][]byte) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRowTs(TableName, Row, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRowWithColumns(TableName []byte, Row []byte, Columns [][]byte, Attributes map[string][]byte) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRowWithColumns(TableName, Row, Columns, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRowWithColumnsTs(TableName []byte, Row []byte, Columns [][]byte, Timestamp int64, Attributes map[string][]byte) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRowWithColumnsTs(TableName, Row, Columns, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRows(TableName []byte, Rows [][]byte, Attributes map[string][]byte) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRows(TableName, Rows, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRowsTs(TableName []byte, Rows [][]byte, Timestamp int64, Attributes map[string][]byte) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRowsTs(TableName, Rows, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRowsWithColumns(TableName []byte, Rows [][]byte, Columns [][]byte, Attributes map[string][]byte) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRowsWithColumns(TableName, Rows, Columns, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetRowsWithColumnsTs(TableName []byte, Rows [][]byte, Columns [][]byte, Timestamp int64, Attributes map[string][]byte) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetRowsWithColumnsTs(TableName, Rows, Columns, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetTableNames() ([][]byte, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetTableNames()

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetTableRegions(TableName []byte) ([]*thrift.TRegionInfo, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetTableRegions(TableName)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetVer(TableName []byte, Row []byte, Column []byte, NumVersions int32, Attributes map[string][]byte) ([]*thrift.TCell, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetVer(TableName, Row, Column, NumVersions, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) GetVerTs(TableName []byte, Row []byte, Column []byte, Timestamp int64, NumVersions int32, Attributes map[string][]byte) ([]*thrift.TCell, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.GetVerTs(TableName, Row, Column, Timestamp, NumVersions, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) Increment(Increment *thrift.TIncrement) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.Increment(Increment)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) IncrementRows(Increments []*thrift.TIncrement) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.IncrementRows(Increments)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) IsTableEnabled(TableName []byte) (bool, error) {
	if err := p.SafeOpen(); nil != err {
		return false, err
	}

	// TODO
	output, err := p.client.IsTableEnabled(TableName)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) MajorCompact(TableNameOrRegionName []byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.MajorCompact(TableNameOrRegionName)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) MutateRow(TableName []byte, Row []byte, Mutations []*thrift.Mutation, Attributes map[string][]byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.MutateRow(TableName, Row, Mutations, Attributes)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) MutateRowTs(TableName []byte, Row []byte, Mutations []*thrift.Mutation, Timestamp int64, Attributes map[string][]byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.MutateRowTs(TableName, Row, Mutations, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) MutateRows(TableName []byte, RowBatches []*thrift.BatchMutation, Attributes map[string][]byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.MutateRows(TableName, RowBatches, Attributes)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) MutateRowsTs(TableName []byte, RowBatches []*thrift.BatchMutation, Timestamp int64, Attributes map[string][]byte) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.MutateRowsTs(TableName, RowBatches, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) ScannerClose(Id int32) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	// TODO
	err := p.client.ScannerClose(Id)

	if nil != err {
		p.Close()
	}
	return err
}

func (p *ThriftConnection) ScannerGet(Id int32) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.ScannerGet(Id)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) ScannerGetList(Id int32, NbRows int32) ([]*thrift.TRowResult, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	// TODO
	output, err := p.client.ScannerGetList(Id, NbRows)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) ScannerOpen(TableName []byte, StartRow []byte, Columns [][]byte, Attributes map[string][]byte) (int32, error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	// TODO
	output, err := p.client.ScannerOpen(TableName, StartRow, Columns, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) ScannerOpenTs(TableName []byte, StartRow []byte, Columns [][]byte, Timestamp int64, Attributes map[string][]byte) (int32, error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	// TODO
	output, err := p.client.ScannerOpenTs(TableName, StartRow, Columns, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) ScannerOpenWithPrefix(TableName []byte, StartAndPrefix []byte, Columns [][]byte, Attributes map[string][]byte) (int32, error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	// TODO
	output, err := p.client.ScannerOpenWithPrefix(TableName, StartAndPrefix, Columns, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) ScannerOpenWithScan(TableName []byte, Scan *thrift.TScan, Attributes map[string][]byte) (int32, error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	// TODO
	output, err := p.client.ScannerOpenWithScan(TableName, Scan, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) ScannerOpenWithStop(TableName []byte, StartRow []byte, StopRow []byte, Columns [][]byte, Attributes map[string][]byte) (int32, error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	// TODO
	output, err := p.client.ScannerOpenWithStop(TableName, StartRow, StopRow, Columns, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

func (p *ThriftConnection) ScannerOpenWithStopTs(TableName []byte, StartRow []byte, StopRow []byte, Columns [][]byte, Timestamp int64, Attributes map[string][]byte) (int32, error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	output, err := p.client.ScannerOpenWithStopTs(TableName, StartRow, StopRow, Columns, Timestamp, Attributes)

	if nil != err {
		p.Close()
	}
	return output, err
}

//
//  ========================================
//
// ThriftConnection implementation:
//
//  ========================================
//

//
// Ping the server, opening the client connection if necessary
// Returns:
//   nil   --> Ping was successful!
//   error --> Ping was failure
//
func (p *ThriftConnection) Ping() error {
	// return p.Cmd("ping").Err

	// TODO
	return nil
}

//
// Return true if the client connection exists
//
func (p *ThriftConnection) IsOpen() bool {
	output := nil != p.client

	// Debug logging
	p.Logger.Trace("[ThriftConnection][IsOpen][%s/%s] --> %v", p.Url, p.Id, output)

	return output
}

//
// Return true if the client connection exists
//
func (p *ThriftConnection) IsClosed() bool {
	output := nil == p.client

	// Debug logging
	p.Logger.Trace("[ThriftConnection][IsClosed][%s/%s] --> %v", p.Url, p.Id, output)

	return output
}

func (p *ThriftConnection) SafeOpen() error {
	if p.IsOpen() {
		return nil
	}
	return p.Open()
}

//
// Open a new connection to thrift
//
func (p *ThriftConnection) Open() error {
	// Set the default timeout
	if time.Duration(0) == p.Timeout {
		p.Timeout = time.Duration(10) * time.Second
	}

	// Open the TCP connection
	client := &thrift.HbaseClient{}

	// Open the connection &
	// Check for errors
	if err := client.Open(p.Url, p.Timeout); nil != err {
		// Log the event
		p.Logger.Error("[ThriftConnection][Open][%s/%s] --> Error = %v", p.Url, p.Id, err)

		// Return the error
		return err
	}

	// Save the client pointer
	p.client = client

	// Log the event
	p.Logger.Info("[ThriftConnection][Open][%s/%s] --> Opened!", p.Url, p.Id)

	// Return nil
	return nil
}

//
// Close closes the connection.
//
func (p *ThriftConnection) Close() (err error) {
	// Close the connection
	if nil != p.client {
		err = p.client.Close()
	}

	// Set the pointer to nil
	p.client = nil

	// Log the event
	p.Logger.Info("[ThriftConnection][Close][%s/%s] --> Closed!", p.Url, p.Id)

	return
}
