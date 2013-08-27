package dog_pool_utils

import "fmt"
import "os/exec"
import "errors"
import "time"
import "github.com/alecthomas/log4go"

import dog_pool "../"

type RedisServerProcess struct {
	port       int
	logger     *log4go.Logger
	connection *dog_pool.RedisConnection
	cmd        *exec.Cmd
}

func StartRedisServer(logger *log4go.Logger) (*RedisServerProcess, error) {
	var err error
	if nil == logger {
		return nil, errors.New("Nil logger")
	}

	server := &RedisServerProcess{}
	server.port, err = findPort()
	server.logger = logger
	if nil != err {
		return nil, err
	}

	// Start the server ...
	server.cmd = exec.Command("redis-server", "--port", fmt.Sprintf("%d", server.port))
	err = server.cmd.Start()
	if nil != err {
		return nil, err
	}

	// Slight delay to start the server
	time.Sleep(time.Duration(1) * time.Second)

	return server, nil
}

//
// Close the redis-server and redis-connection
//
func (p *RedisServerProcess) Close() error {
	if nil != p.connection {
		p.connection.Close()
	}
	p.connection = nil

	if nil != p.cmd {
		p.cmd.Process.Kill()
		p.cmd.Wait()
	}
	p.cmd = nil

	p.port = 0

	return nil
}

//
// Get/Create a connection to redis
//
func (p *RedisServerProcess) Connection() *dog_pool.RedisConnection {
	if nil == p.cmd {
		panic("No redis-server running")
	}

	if nil == p.connection {
		p.connection = &dog_pool.RedisConnection{
			Url:    fmt.Sprintf("127.0.0.1:%d", p.port),
			Logger: p.logger,
		}
	}

	return p.connection
}
