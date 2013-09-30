//
// Redis Client Interface
//
// Interface implemented by redis.Client and dog_pool.RedisConnection
//

package dog_pool

import "testing"
import "github.com/RUNDSP/radix/redis"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisClientInterfaceSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisClientInterfaceSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisClientInterfaceSpecs(c gospec.Context) {
	c.Specify("[RedisClientInterface] RedisConnection satisfies RedisClientInterface", func() {
		connection := &RedisConnection{}

		// Wont' compile unless it implements the interface
		var redis_interface RedisClientInterface = connection
		c.Expect(redis_interface, gospec.Satisfies, true)
	})

	c.Specify("[RedisClientInterface] redis.Client satisfies RedisClientInterface", func() {
		client := &redis.Client{}

		// Wont' compile unless it implements the interface
		var redis_interface RedisClientInterface = client
		c.Expect(redis_interface, gospec.Satisfies, true)
	})
}
