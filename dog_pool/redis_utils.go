package dog_pool

import "github.com/alecthomas/log4go"

//
// Helper to iterate urls
//
func loopStrings(values []string) func() string {
	i := 0
	return func() string {
		value := values[i%len(values)]
		i++
		return value
	}
}

//
// Lazily make a Redis Connection
//
func makeLazyConnection(url string, logger *log4go.Logger) (*RedisConnection, error) {
	// Create a new factory instance
	p := &RedisConnection{Url: url, Logger: logger}

	// Return the factory
	return p, nil
}

//
// Agressively make a Redis Connection
//
func makeAgressiveConnection(url string, logger *log4go.Logger) (*RedisConnection, error) {
	// Create a new factory instance
	p, _ := makeLazyConnection(url, logger)

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
