dog_pool
========

Connection Pool wrappers written in GO.

Currently supports:
- Redis

Future support:
- Memcached



Install
=======

	go get -u "github.com/gnagel/dog_pool/dog_pool"


GO Usage
========

	import "github.com/gnagel/dog_pool"
	import redis "github.com/fzzy/radix/redis"

	// Setup the pool
	pool := dog_pool.RedisConnectionPool{}
	pool.Mode = dog_pool.LAZY
	pool.Size = 100
	pool.Urls = []string{"127.0.0.1:6379"}
	pool.Logger = log4go.NewDefaultLogger(log4go.ERROR)
	
	// Initialize the connections
	if err := pool.Open(); nil != err {
		// Abort!
		panic(err)
	}
	
	// Pop a Redis connection from the pool
	connection := pool.Pop();
	defer pool.Push(connection)
	
	// Ping the redis server
	if err := connection.Ping(); nil != err {
		// Redis connection error!
	
		// Exit your test/go routine/app/etc here
		return err
	}
	
	
	// Get the redis client from the connection
	// This will re-connect if the client was previously disconnected
	client, err := connection.Client()
	if  nil != err {
		// Unable to connect to redis!
	
		// Exit your test/go routine/app/etc here
		return err
	}
	
	// Get the keys from the server
	client.Append("keys *")
	
	// Get the response
	reply := client.GetReply()
	
	// Connection error? Then tell the connection to invalidate the Redis connection
	if nil != reply.Err {
		// Close the connection
		connection.Close(reply.Err)
	
		// Exit your test/go routine/app/etc here
		return reply.Err
	}
	
	// ...
	// ...
	// ...
	


Authors:
========

Glenn Nagel <glenn@mercury-wireless.com>, <gnagel@rundsp.com>


Credits:
========

Ryan Day's original implementation that inspired this is here: [rday's gist](https://gist.github.com/rday/3504674)

Juhani Åhman's excellent redis implementation is here: [redis](https://github.com/fzzy/radix)

