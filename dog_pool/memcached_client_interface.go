//
// Memcached Client Interface
//
// Interface implemented by memcached.Client and dog_pool.MemcachedConnection
//

package dog_pool

import memcached "github.com/bradfitz/gomemcache/memcache"

type MemcachedClientInterface interface {
	// GetMulti is a batch version of Get. The returned map from keys to
	// items may have fewer elements than the input slice, due to memcache
	// cache misses. Each key must be at most 250 bytes in length.
	// If no error is returned, the returned map will also be non-nil.
	GetMulti(keys []string) (map[string]*memcached.Item, error)

	// Get gets the item for the given key. ErrCacheMiss is returned for a
	// memcache cache miss. The key must be at most 250 bytes in length.
	Get(key string) (item *memcached.Item, err error)

	// Set writes the given item, unconditionally.
	Set(item *memcached.Item) error

	// Delete deletes the item with the provided key. The error ErrCacheMiss is
	// returned if the item didn't already exist in the cache.
	Delete(key string) error

	// Add writes the given item, if no value already exists for its
	// key. ErrNotStored is returned if that condition is not met.
	Add(item *memcached.Item) error

	// Increment atomically increments key by delta. The return value is
	// the new value after being incremented or an error. If the value
	// didn't exist in memcached the error is ErrCacheMiss. The value in
	// memcached must be an decimal number, or an error will be returned.
	// On 64-bit overflow, the new value wraps around.
	Increment(key string, delta uint64) (newValue uint64, err error)

	// Decrement atomically decrements key by delta. The return value is
	// the new value after being decremented or an error. If the value
	// didn't exist in memcached the error is ErrCacheMiss. The value in
	// memcached must be an decimal number, or an error will be returned.
	// On underflow, the new value is capped at zero and does not wrap
	// around.
	Decrement(key string, delta uint64) (newValue uint64, err error)
}
