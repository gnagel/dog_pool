package dog_pool

import "strconv"
import "github.com/alecthomas/log4go"

//
// Helper to iterate urls
//
func loopStrings(values []string) func() [2]string {
	i := 0
	return func() string {
		value := values[i%len(values)]
		i++
		return [2]string{value, strconv.Itoa(i)}
	}
}

//
// Lazily make a Redis Connection
//
func makeLazyConnection(url string, id string, logger *log4go.Logger) (*RedisConnection, error) {
	// Create a new factory instance
	p := &RedisConnection{Url: url, Id: id, Logger: logger}

	// Return the factory
	return p, nil
}

//
// Agressively make a Redis Connection
//
func makeAgressiveConnection(url string, id string, logger *log4go.Logger) (*RedisConnection, error) {
	// Create a new factory instance
	p, _ := makeLazyConnection(url, id, logger)

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
