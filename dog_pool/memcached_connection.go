//
// Memcached Connection Wrapper written in GO
//

package dog_pool

import "bytes"
import "fmt"
import "runtime"
import "strconv"
import "strings"
import "time"
import "github.com/alecthomas/log4go"
import memcached "github.com/bradfitz/gomemcache/memcache"

//
// Connection Wrapper for Memcached
//
type MemcachedConnection struct {
	Url string "Memcached URL this factory will connect to"

	Id string "(optional) Identifier for distingushing between memcached connections"

	Logger *log4go.Logger "Handle to the logger we are using"

	Timeout time.Duration "Timeout"

	client *memcached.Client "Connection to a Memcached, may be nil"
}

//
// Lazily make a Redis Connection
//
func makeLazyMemcachedConnection(url string, id string, timeout time.Duration, logger *log4go.Logger) (*MemcachedConnection, error) {
	// Create a new factory instance
	p := &MemcachedConnection{Url: url, Id: id, Logger: logger, Timeout: timeout}

	// Return the factory
	return p, nil
}

//
// Agressively make a Memcached Connection
//
func makeAgressiveMemcachedConnection(url string, id string, timeout time.Duration, logger *log4go.Logger) (*MemcachedConnection, error) {
	// Create a new factory instance
	p, _ := makeLazyMemcachedConnection(url, id, timeout, logger)

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

func (p *MemcachedConnection) recoverPanic(cmd string, keys []string) error {
	r := recover()

	if nil == r {
		return nil
	}

	// Panic error
	p.Logger.Critical("[MemcachedConnection][%s][%s/%s] Memcached Keys = '%s' --> Panic Error = '%v'", cmd, p.Url, p.Id, strings.Join(keys, ", "), r)

	// Close the connection
	p.Close()

	// Cast the error
	if e, ok := r.(runtime.Error); ok {
		return e
	}

	// Return the error
	return r.(error)
}

func (p *MemcachedConnection) checkIsOpen(cmd string, keys []string) error {
	if p.IsOpen() {
		return nil
	}

	// Did opening the connection fail?
	err := p.Open()
	if nil != err {
		p.Logger.Warn("[MemcachedConnection][%s][%s/%s] Memcached Keys = '%s' --> Open Error = '%v'", cmd, p.Url, p.Id, strings.Join(keys, ","), err)
	}

	// Return the error, may be nil
	return err
}

func toStringSlice(item *memcached.Item) []string {
	return []string{item.Key, bytes.NewBuffer(item.Value).String(), strconv.Itoa(int(item.Expiration))}
}

//
//  ========================================
//
// MemcachedClientInterface -and- memcached.Client implementation:
//
//  ========================================
//

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (p *MemcachedConnection) GetMulti(keys []string) (output map[string]*memcached.Item, err error) {
	// Recover from panic'd errors
	defer func() {
		if recovered_err := p.recoverPanic("GetMulti", keys); nil != recovered_err {
			output = nil
			err = recovered_err
			return
		}
	}()

	// Open the connection if necessary
	if open_err := p.checkIsOpen("GetMulti", keys); nil != open_err {
		return nil, open_err
	}

	// Perform the memcached request
	stop_watch := MakeStopWatch(p, p.Logger, "GetMulti").Start()
	output, err = p.client.GetMulti(keys)
	stop_watch.Stop().LogDuration()

	switch err {
	case nil:
		p.Logger.Trace(func() string {
			buffer := make([]string, len(output))
			i := 0
			for key, item := range output {
				buffer[i] = fmt.Sprintf("%s=%s", key, bytes.NewBuffer(item.Value).String())
				i++
			}

			return fmt.Sprintf("[MemcachedConnection][Get][%s/%s] Keys = '%v' --> Got Values = [%s]!", p.Url, p.Id, strings.Join(keys, ","), strings.Join(buffer, ", "))
		})
	default:
		p.Logger.Error("[MemcachedConnection][Get][%s/%s] Key = '%v' --> Fatal Error = '%v'", p.Url, p.Id, strings.Join(keys, ","), err)
		p.Close()
	}

	return
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (p *MemcachedConnection) Get(key string) (item *memcached.Item, err error) {
	// Recover from panic'd errors
	defer func() {
		if recovered_err := p.recoverPanic("Get", []string{key}); nil != recovered_err {
			err = recovered_err
			return
		}
	}()

	// Open the connection if necessary
	if err := p.checkIsOpen("Get", []string{key}); nil != err {
		return nil, err
	}

	// Perform the memcached request
	stop_watch := MakeStopWatch(p, p.Logger, "Get").Start()
	item, err = p.client.Get(key)
	stop_watch.Stop().LogDuration()

	switch err {
	case nil:
		p.Logger.Trace("[MemcachedConnection][Get][%s/%s] Key = '%v' --> Got Value = '%v'!", p.Url, p.Id, key, bytes.NewBuffer(item.Value).String())
	case memcached.ErrCacheMiss:
		p.Logger.Trace("[MemcachedConnection][Get][%s/%s] Key = '%v' --> Not Stored = '%v'", p.Url, p.Id, key, err)
	default:
		p.Logger.Error("[MemcachedConnection][Get][%s/%s] Key = '%v' --> Fatal Error = '%v'", p.Url, p.Id, key, err)
		p.Close()
	}

	return
}

// Set writes the given item, unconditionally.
func (p *MemcachedConnection) Set(item *memcached.Item) (err error) {
	log_item := toStringSlice(item)

	// Recover from panic'd errors
	defer func() {
		if recovered_err := p.recoverPanic("Set", log_item); nil != recovered_err {
			err = recovered_err
			return
		}
	}()

	// Open the connection if necessary
	if err := p.checkIsOpen("Set", log_item); nil != err {
		return err
	}

	// Perform the memcached request
	stop_watch := MakeStopWatch(p, p.Logger, "Set").Start()
	err = p.client.Set(item)
	stop_watch.Stop().LogDuration()

	key := item.Key
	delta := bytes.NewBuffer(item.Value).String()
	switch err {
	case nil:
		p.Logger.Trace("[MemcachedConnection][Set][%s/%s] Key = '%v', Value = '%v', Expires = %d(s) --> Set Value!", p.Url, p.Id, key, delta, item.Expiration)
	default:
		p.Logger.Error("[MemcachedConnection][Set][%s/%s] Key = '%v', Value = '%v', Expires = %d(s) --> Fatal Error = '%v'", p.Url, p.Id, key, delta, item.Expiration, err)
		p.Close()
	}

	return
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (p *MemcachedConnection) Delete(key string) (err error) {
	// Recover from panic'd errors
	defer func() {
		if recovered_err := p.recoverPanic("Delete", []string{key}); nil != recovered_err {
			err = recovered_err
			return
		}
	}()

	// Open the connection if necessary
	if err := p.checkIsOpen("Delete", []string{key}); nil != err {
		return err
	}

	// Perform the memcached request
	stop_watch := MakeStopWatch(p, p.Logger, "Delete").Start()
	err = p.client.Delete(key)
	stop_watch.Stop().LogDuration()

	switch err {
	case nil:
		p.Logger.Trace("[MemcachedConnection][Delete][%s/%s] Key = '%v' --> Deleted Value!", p.Url, p.Id, key)
	case memcached.ErrCacheMiss:
		p.Logger.Trace("[MemcachedConnection][Delete][%s/%s] Key = '%v' --> Not Stored = '%v'", p.Url, p.Id, key, err)
	default:
		p.Logger.Error("[MemcachedConnection][Delete][%s/%s] Key = '%v' --> Fatal Error = '%v'", p.Url, p.Id, key, err)
		p.Close()
	}

	return
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (p *MemcachedConnection) Add(item *memcached.Item) (err error) {
	log_item := toStringSlice(item)

	// Recover from panic'd errors
	defer func() {
		if recovered_err := p.recoverPanic("Add", log_item); nil != recovered_err {
			err = recovered_err
			return
		}
	}()

	// Open the connection if necessary
	if err := p.checkIsOpen("Add", log_item); nil != err {
		return err
	}

	// Perform the memcached request
	stop_watch := MakeStopWatch(p, p.Logger, "Add").Start()
	err = p.client.Add(item)
	stop_watch.Stop().LogDuration()

	key := item.Key
	delta := bytes.NewBuffer(item.Value).String()
	switch err {
	case nil:
		p.Logger.Trace("[MemcachedConnection][Add][%s/%s] Key = '%v', Value = '%v' --> Added Value!", p.Url, p.Id, key, delta)
	case memcached.ErrNotStored:
		p.Logger.Trace("[MemcachedConnection][Add][%s/%s] Key = '%v', Value = '%v' --> Not Stored = '%v'", p.Url, p.Id, key, delta, err)
	default:
		p.Logger.Error("[MemcachedConnection][Add][%s/%s] Key = '%v', Value = '%v' --> Fatal Error = '%v'", p.Url, p.Id, key, delta, err)
		p.Close()
	}

	return
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (p *MemcachedConnection) Increment(key string, delta uint64) (newValue uint64, err error) {
	log_keys := []string{key, strconv.Itoa(int(delta))}

	// Recover from panic'd errors
	defer func() {
		if recovered_err := p.recoverPanic("Increment", log_keys); nil != recovered_err {
			err = recovered_err
			return
		}
	}()

	// Open the connection if necessary
	if err := p.checkIsOpen("Increment", log_keys); nil != err {
		return 0, err
	}

	// Perform the memcached request
	stop_watch := MakeStopWatch(p, p.Logger, "Increment").Start()
	newValue, err = p.client.Increment(key, delta)
	stop_watch.Stop().LogDuration()

	switch err {
	case nil:
		p.Logger.Trace("[MemcachedConnection][Increment][%s/%s] Key = '%v', Delta = %d --> Incremented Value = %d!", p.Url, p.Id, key, delta, newValue)
	case memcached.ErrCacheMiss:
		p.Logger.Trace("[MemcachedConnection][Increment][%s/%s] Key = '%v', Delta = %d --> Not Stored = '%v'", p.Url, p.Id, key, delta, err)
	default:
		p.Logger.Error("[MemcachedConnection][Increment][%s/%s] Key = '%v', Delta = %d --> Fatal Error = '%v'", p.Url, p.Id, key, err)
		p.Close()
	}

	return
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (p *MemcachedConnection) Decrement(key string, delta uint64) (newValue uint64, err error) {
	log_keys := []string{key, strconv.Itoa(int(delta))}

	// Recover from panic'd errors
	defer func() {
		if recovered_err := p.recoverPanic("Decrement", log_keys); nil != recovered_err {
			err = recovered_err
			return
		}
	}()

	// Open the connection if necessary
	if err := p.checkIsOpen("Decrement", log_keys); nil != err {
		return 0, err
	}

	// Perform the memcached request
	stop_watch := MakeStopWatch(p, p.Logger, "Decrement").Start()
	newValue, err = p.client.Decrement(key, delta)
	stop_watch.Stop().LogDuration()

	switch err {
	case nil:
		p.Logger.Trace("[MemcachedConnection][Decrement][%s/%s] Key = '%v', Delta = %d --> Decremented Value = %d!", p.Url, p.Id, key, delta, newValue)
	case memcached.ErrCacheMiss:
		p.Logger.Trace("[MemcachedConnection][Decrement][%s/%s] Key = '%v', Delta = %d --> Not Stored = '%v'", p.Url, p.Id, key, delta, err)
	default:
		p.Logger.Error("[MemcachedConnection][Decrement][%s/%s] Key = '%v', Delta = %d --> Fatal Error = '%v'", p.Url, p.Id, key, err)
		p.Close()
	}

	return
}

//
//  ========================================
//
// MemcachedConnection implementation:
//
//  ========================================
//

//
// Ping the server, opening the client connection if necessary
// Returns:
//   nil   --> Ping was successful!
//   error --> Ping was failure
//
func (p *MemcachedConnection) Ping() error {
	item := &memcached.Item{}
	item.Key = fmt.Sprintf("%s-%s-ping", p.Url, p.Id)
	item.Value = bytes.NewBufferString("1").Bytes()
	item.Expiration = int32(10) // Seconds

	// Set, then delete the item
	err := p.Set(item)
	if nil == err {
		p.Delete(item.Key)
	}

	// Return any errors from set
	return err
}

//
// Return true if the client connection exists
//
func (p *MemcachedConnection) IsOpen() bool {
	output := nil != p.client

	// Debug logging
	p.Logger.Trace("[MemcachedConnection][IsOpen][%s/%s] --> %v", p.Url, p.Id, output)

	return output
}

//
// Return true if the client connection exists
//
func (p *MemcachedConnection) IsClosed() bool {
	output := nil == p.client

	// Debug logging
	p.Logger.Trace("[MemcachedConnection][IsClosed][%s/%s] --> %v", p.Url, p.Id, output)

	return output
}

//
// Open a new connection to memcached
//
func (p *MemcachedConnection) Open() error {
	// Open the TCP connection -and-
	// Save the client pointer
	p.client = memcached.New(p.Url)
	p.client.Timeout = time.Duration(10) * time.Second

	// Log the event
	p.Logger.Info("[MemcachedConnection][Open][%s/%s] --> Opened!", p.Url, p.Id)

	// Perform a basic command on the server
	item := &memcached.Item{}
	item.Key = fmt.Sprintf("%s-%s-opened", p.Url, p.Id)
	item.Value = bytes.NewBufferString("1").Bytes()
	item.Expiration = int32(10) // Seconds

	// Set, then delete the item
	err := p.Set(item)
	if nil == err {
		p.Delete(item.Key)
	}

	// Check for errors
	if nil != err {
		// Reset the pointer to nil
		p.client = nil

		// Log the event
		p.Logger.Error("[MemcachedConnection][Open][%s/%s] --> Error = '%v'", p.Url, p.Id, err)

		// Return the error
		return err
	}

	// Return nil
	return nil
}

//
// Close closes the connection.
//
func (p *MemcachedConnection) Close() (err error) {
	// Set the pointer to nil
	p.client = nil

	// Log the event
	p.Logger.Info("[MemcachedConnection][Close][%s/%s] --> Closed!", p.Url, p.Id)

	return
}
