package dog_pool

import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"
import memcached "github.com/bradfitz/gomemcache/memcache"

//
// NOTE: Use differient ports for each test!
//       gospec runs the specs in parallel!
//
func TestMemcachedConnectionSpecs(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(MemcachedConnectionSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func MemcachedConnectionSpecs(c gospec.Context) {
	var memcached_connection_logger = log4go.NewDefaultLogger(log4go.CRITICAL)

	c.Specify("[MemcachedConnection] New connection is not open", func() {
		connection := MemcachedConnection{Url: "127.0.0.1:11290", Logger: &memcached_connection_logger}
		defer connection.Close()

		// Should be opposite of each other:
		c.Expect(connection.IsOpen(), gospec.Equals, false)
		c.Expect(connection.IsClosed(), gospec.Equals, true)
	})

	c.Specify("[MemcachedConnection] Opening connection to Invalid Host/Port has errors", func() {
		connection := MemcachedConnection{Url: "127.0.0.1:11291", Logger: &memcached_connection_logger}
		defer connection.Close()

		c.Expect(nil != connection.Open(), gospec.Equals, true)
		c.Expect(connection.IsClosed(), gospec.Equals, true)
	})

	c.Specify("[MemcachedConnection] Opening connection to Valid Host/Port has no errors", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartMemcachedServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		c.Expect(server.Connection().Open(), gospec.Equals, nil)
		c.Expect(server.Connection().IsOpen(), gospec.Equals, true)
		c.Expect(server.Connection().IsClosed(), gospec.Equals, false)
	})

	c.Specify("[MemcachedConnection] Ping (-->Set-->Delete) (re-)opens the connection automatically", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartMemcachedServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		// Starts off closed ...
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		// Ping the server
		// Should now be open
		c.Expect(server.Connection().Ping(), gospec.Equals, nil)
		c.Expect(server.Connection().IsOpen(), gospec.Equals, true)

		// Close the connection
		err = server.Connection().Close()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(server.Connection().IsClosed(), gospec.Equals, true)

		// Ping the server again
		// Should now be open again
		c.Expect(server.Connection().Ping(), gospec.Equals, nil)
		c.Expect(server.Connection().IsOpen(), gospec.Equals, true)
	})

	c.Specify("[MemcachedConnection][Get] Returns Cache Miss", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartMemcachedServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		item, err := server.Connection().Get("BOB")
		c.Expect(err, gospec.Equals, memcached.ErrCacheMiss)
		c.Expect(item, gospec.Satisfies, nil == item)
	})

	c.Specify("[MemcachedConnection][Set+Get] Returns Value", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		server, err := StartMemcachedServer(&logger)
		if nil != err {
			panic(err)
		}
		defer server.Close()

		item_input := &memcached.Item{Key: "BOB", Value: []byte("Hello")}
		c.Expect(server.Connection().Set(item_input), gospec.Equals, nil)

		item_output, err := server.Connection().Get("BOB")
		c.Expect(err, gospec.Equals, nil)
		c.Expect(item_output.Key, gospec.Equals, item_input.Key)
		c.Expect(string(item_output.Value), gospec.Equals, string(item_input.Value))
	})

}

func Benchmark_MemcachedConnection_Get(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartMemcachedServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	server.Connection().Set(&memcached.Item{Key: "BOB", Value: []byte("Hello")})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Get("BOB")
	}
}

func Benchmark_MemcachedConnection_Set(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartMemcachedServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Set(&memcached.Item{Key: "BOB", Value: []byte("Hello")})
	}
}

func Benchmark_MemcachedConnection_SetGet(b *testing.B) {
	logger := log4go.NewDefaultLogger(log4go.CRITICAL)
	server, err := StartMemcachedServer(&logger)
	if nil != err {
		panic(err)
	}
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.Connection().Set(&memcached.Item{Key: "BOB", Value: []byte("Hello")})
		server.Connection().Get("BOB")
	}
}
