//
// Thrift Connection Wrapper written in GO
//

package dog_pool

import "time"
import "github.com/alecthomas/log4go"
import goh "github.com/sdming/goh"
import goh_hbase "github.com/sdming/goh/Hbase"

// import goh_hbase "github.com/sdming/goh/Hbase"

//
// Connection Wrapper for Thrift
//
type ThriftConnection struct {
	Url string "Thrift URL this factory will connect to"

	Id string "(optional) Identifier for distingushing between thrift connections"

	Logger *log4go.Logger "Handle to the logger we are using"

	Timeout time.Duration "Connection Timeout"

	client *goh.HClient "Connection to a Thrift, may be nil"
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
	client, err := goh.NewTcpClient(p.Url, goh.TBinaryProtocol, false)
	if err != nil {
		return err
	}

	// Open the connection &
	// Check for errors
	err = client.Open()
	if nil != err {
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

//
//  ========================================
//
// HClient implementation:
//
//  ========================================
//

func (p *ThriftConnection) EnableTable(tableName string) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.EnableTable((tableName))
}

func (p *ThriftConnection) DisableTable(tableName string) (err error) {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.DisableTable((tableName))
}

func (p *ThriftConnection) IsTableEnabled(tableName string) (ret bool, err error) {
	if err := p.SafeOpen(); nil != err {
		return false, err
	}

	return p.client.IsTableEnabled((tableName))
}

func (p *ThriftConnection) Compact(tableNameOrRegionName string) (err error) {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.Compact((tableNameOrRegionName))
}

func (p *ThriftConnection) MajorCompact(tableNameOrRegionName string) (err error) {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.MajorCompact((tableNameOrRegionName))
}

func (p *ThriftConnection) GetTableNames() (tables []string, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetTableNames()
}

func (p *ThriftConnection) GetColumnDescriptors(tableName string) (columns map[string]*goh.ColumnDescriptor, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetColumnDescriptors((tableName))
}

func (p *ThriftConnection) GetTableRegions(tableName string) (regions []*goh.TRegionInfo, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetTableRegions((tableName))
}

func (p *ThriftConnection) CreateTable(tableName string, columnFamilies []*goh.ColumnDescriptor) (exists bool, err error) {
	if err := p.SafeOpen(); nil != err {
		return false, err
	}

	return p.client.CreateTable((tableName), columnFamilies)
}

func (p *ThriftConnection) DeleteTable(tableName string) (err error) {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.DeleteTable((tableName))
}

func (p *ThriftConnection) Get(tableName string, row []byte, column string, attributes map[string]string) (data []*goh_hbase.TCell, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.Get((tableName), (row), (column), (attributes))
}

func (p *ThriftConnection) GetVer(tableName string, row []byte, column string, numVersions int32, attributes map[string]string) (data []*goh_hbase.TCell, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetVer((tableName), (row), (column), numVersions, (attributes))
}

func (p *ThriftConnection) GetVerTs(tableName string, row []byte, column string, timestamp int64, numVersions int32, attributes map[string]string) (data []*goh_hbase.TCell, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetVerTs((tableName), (row), (column), timestamp, numVersions, (attributes))
}

func (p *ThriftConnection) GetRow(tableName string, row []byte, attributes map[string]string) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRow((tableName), (row), (attributes))
}

func (p *ThriftConnection) GetRowWithColumns(tableName string, row []byte, columns []string, attributes map[string]string) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRowWithColumns((tableName), (row), (columns), (attributes))
}

func (p *ThriftConnection) GetRowTs(tableName string, row []byte, timestamp int64, attributes map[string]string) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRowTs((tableName), (row), timestamp, (attributes))
}

func (p *ThriftConnection) GetRowWithColumnsTs(tableName string, row []byte, columns []string, timestamp int64, attributes map[string]string) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRowWithColumnsTs((tableName), (row), (columns), timestamp, (attributes))
}

func (p *ThriftConnection) GetRows(tableName string, rows [][]byte, attributes map[string]string) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRows((tableName), (rows), (attributes))
}

func (p *ThriftConnection) GetRowsWithColumns(tableName string, rows [][]byte, columns []string, attributes map[string]string) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRowsWithColumns((tableName), (rows), (columns), (attributes))
}

func (p *ThriftConnection) GetRowsTs(tableName string, rows [][]byte, timestamp int64, attributes map[string]string) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRowsTs((tableName), (rows), timestamp, (attributes))
}

func (p *ThriftConnection) GetRowsWithColumnsTs(tableName string, rows [][]byte, columns []string, timestamp int64, attributes map[string]string) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRowsWithColumnsTs((tableName), (rows), (columns), timestamp, (attributes))
}

func (p *ThriftConnection) MutateRow(tableName string, row []byte, mutations []*goh_hbase.Mutation, attributes map[string]string) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.MutateRow((tableName), (row), mutations, (attributes))
}

func (p *ThriftConnection) MutateRowTs(tableName string, row []byte, mutations []*goh_hbase.Mutation, timestamp int64, attributes map[string]string) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.MutateRowTs((tableName), (row), mutations, timestamp, (attributes))
}

func (p *ThriftConnection) MutateRows(tableName string, rowBatches []*goh_hbase.BatchMutation, attributes map[string]string) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.MutateRows((tableName), rowBatches, (attributes))
}

func (p *ThriftConnection) MutateRowsTs(tableName string, rowBatches []*goh_hbase.BatchMutation, timestamp int64, attributes map[string]string) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.MutateRowsTs((tableName), rowBatches, timestamp, (attributes))
}

func (p *ThriftConnection) AtomicIncrement(tableName string, row []byte, column string, value int64) (v int64, err error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	return p.client.AtomicIncrement((tableName), (row), (column), value)
}

func (p *ThriftConnection) DeleteAll(tableName string, row []byte, column string, attributes map[string]string) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.DeleteAll((tableName), (row), (column), (attributes))
}

func (p *ThriftConnection) DeleteAllTs(tableName string, row []byte, column string, timestamp int64, attributes map[string]string) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.DeleteAllTs((tableName), (row), (column), timestamp, (attributes))
}

func (p *ThriftConnection) DeleteAllRow(tableName string, row []byte, attributes map[string]string) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.DeleteAllRow((tableName), (row), (attributes))
}

func (p *ThriftConnection) Increment(increment *goh_hbase.TIncrement) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.Increment(increment)
}

func (p *ThriftConnection) IncrementRows(increments []*goh_hbase.TIncrement) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.IncrementRows(increments)
}

func (p *ThriftConnection) DeleteAllRowTs(tableName string, row []byte, timestamp int64, attributes map[string]string) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.DeleteAllRowTs((tableName), (row), timestamp, (attributes))
}

func (p *ThriftConnection) ScannerOpenWithScan(tableName string, scan *goh.TScan, attributes map[string]string) (id int32, err error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	return p.client.ScannerOpenWithScan((tableName), (scan), (attributes))
}

func (p *ThriftConnection) ScannerOpen(tableName string, startRow []byte, columns []string, attributes map[string]string) (id int32, err error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	return p.client.ScannerOpen((tableName), (startRow), (columns), (attributes))
}

func (p *ThriftConnection) ScannerOpenWithStop(tableName string, startRow []byte, stopRow []byte, columns []string, attributes map[string]string) (id int32, err error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	return p.client.ScannerOpenWithStop((tableName), (startRow), (stopRow), (columns), (attributes))
}

func (p *ThriftConnection) ScannerOpenWithPrefix(tableName string, startAndPrefix []byte, columns []string, attributes map[string]string) (id int32, err error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	return p.client.ScannerOpenWithPrefix((tableName), (startAndPrefix), (columns), (attributes))
}

func (p *ThriftConnection) ScannerOpenTs(tableName string, startRow []byte, columns []string, timestamp int64, attributes map[string]string) (id int32, err error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	return p.client.ScannerOpenTs((tableName), (startRow), (columns), timestamp, (attributes))
}

func (p *ThriftConnection) ScannerOpenWithStopTs(tableName string, startRow []byte, stopRow []byte, columns []string, timestamp int64, attributes map[string]string) (id int32, err error) {
	if err := p.SafeOpen(); nil != err {
		return 0, err
	}

	return p.client.ScannerOpenWithStopTs((tableName), (startRow), (stopRow), (columns), timestamp, (attributes))
}

func (p *ThriftConnection) ScannerGet(id int32) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.ScannerGet((id))
}

func (p *ThriftConnection) ScannerGetList(id int32, nbRows int32) (data []*goh_hbase.TRowResult, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.ScannerGetList((id), nbRows)
}

func (p *ThriftConnection) ScannerClose(id int32) error {
	if err := p.SafeOpen(); nil != err {
		return err
	}

	return p.client.ScannerClose((id))
}

func (p *ThriftConnection) GetRowOrBefore(tableName string, row string, family string) (data []*goh_hbase.TCell, err error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRowOrBefore((tableName), (row), (family))
}

func (p *ThriftConnection) GetRegionInfo(row string) (*goh.TRegionInfo, error) {
	if err := p.SafeOpen(); nil != err {
		return nil, err
	}

	return p.client.GetRegionInfo((row))
}
