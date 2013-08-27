package dog_pool_utils

import "fmt"
import "os/exec"
import "errors"
import "time"
import "../dog_pool"
import "github.com/alecthomas/log4go"

type MemcachedServerProcess struct {
	port       int
	logger     *log4go.Logger
	connection *dog_pool.MemcachedConnection
	cmd        *exec.Cmd
}

func StartMemcachedServer(logger *log4go.Logger) (*MemcachedServerProcess, error) {
	var err error
	if nil == logger {
		return nil, errors.New("Nil logger")
	}

	server := &MemcachedServerProcess{}
	server.port, err = findPort()
	server.logger = logger
	if nil != err {
		return nil, err
	}

	// Start the server ...
	server.cmd = exec.Command("memcached", "-p", fmt.Sprintf("%d", server.port))
	err = server.cmd.Start()
	if nil != err {
		return nil, err
	}

	// Slight delay to start the server
	time.Sleep(time.Duration(1) * time.Second)

	return server, nil
}

//
// Close the memcached-server and memcached-connection
//
func (p *MemcachedServerProcess) Close() error {
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
// Get/Create a connection to Memcached
//
func (p *MemcachedServerProcess) Connection() *dog_pool.MemcachedConnection {
	if nil == p.cmd {
		panic("No memcached-server running")
	}

	if nil == p.connection {
		p.connection = &dog_pool.MemcachedConnection{
			Url:    fmt.Sprintf("127.0.0.1:%d", p.port),
			Logger: p.logger,
		}
	}

	return p.connection
}
