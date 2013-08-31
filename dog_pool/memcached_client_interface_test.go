//
// Memcached Client Interface
//
// Interface implemented by memcached.Client and dog_pool.MemcachedConnection
//

package dog_pool

import memcached "github.com/bradfitz/gomemcache/memcache"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestMemcachedClientInterfaceSpecs(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(MemcachedClientInterfaceSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func MemcachedClientInterfaceSpecs(c gospec.Context) {
	c.Specify("[MemcachedClientInterface] MemcachedConnection satisfies MemcachedClientInterface", func() {
		connection := &MemcachedConnection{}

		// Wont' compile unless it implements the interface
		var memcached_interface MemcachedClientInterface = connection
		c.Expect(memcached_interface, gospec.Satisfies, true)
	})

	c.Specify("[MemcachedClientInterface] memcached.Client satisfies MemcachedClientInterface", func() {
		client := &memcached.Client{}

		// Wont' compile unless it implements the interface
		var memcached_interface MemcachedClientInterface = client
		c.Expect(memcached_interface, gospec.Satisfies, true)
	})
}
