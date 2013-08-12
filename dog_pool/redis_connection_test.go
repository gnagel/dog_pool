package dog_pool

import "os"
import "os/exec"
import "time"
import "testing"
import "github.com/alecthomas/log4go"

var redis_master = os.Getenv("REDIS_MASTER")
var redis_connection_logger = log4go.NewDefaultLogger(log4go.CRITICAL)

const method_expected_a_equal_b_from_url = "[%s][%s] Expected '%v', Actual '%v'"

//
// New connections are not open
//
func Test_RedisConnection_NewConnection_IsNotOpen(t *testing.T) {
	tag := "[RedisConnection][NewConnection IsNotOpen] Actual = %#v, Expected %#v"

	// Create a connection, but don't actually connect to redis yet ...
	connection := RedisConnection{Url: "127.0.0.1:6999", Logger: &redis_connection_logger}
	defer connection.Close(nil)

	// We shouldn't be connected
	if closed := connection.IsClosed(); closed != true {
		t.Errorf(tag, closed, true)
		return
	}
}

//
// Connections to Non-existent Redis Servers should fail
//
func Test_RedisConnection_BadConnection_HasErrors(t *testing.T) {
	tag := "[RedisConnection][BadConnection HasErrors] Actual = %#v, Expected %#v"

	// Create a connection, but don't actually connect to redis yet ...
	connection := RedisConnection{Url: "127.0.0.1:6999", Logger: &redis_connection_logger}
	defer connection.Close(nil)

	// Expected to fail
	if err := connection.Open(); nil == err {
		t.Errorf(tag, err, "!nil")
		return
	}

	// We shouldn't be connected
	if closed := connection.IsClosed(); closed != true {
		t.Errorf(tag, closed, true)
		return
	}
}

//
// Connections to Non-existent Redis Servers should fail
//
func Test_RedisConnection_GoodConnection_NoErrors(t *testing.T) {
	tag := "[RedisConnection][BadConnection HasErrors] Actual = %#v, Expected %#v"

	cmd := exec.Command("redis-server", "--port", "6999")
	if err := cmd.Start(); err != nil {
		t.Logf("skipping test; couldn't find memcached")
		return
	}
	time.Sleep(time.Duration(1) * time.Second)
	defer cmd.Wait()
	defer cmd.Process.Kill()

	// Connect to the actual redis server
	connection := RedisConnection{Url: "127.0.0.1:6999", Logger: &redis_connection_logger}
	defer connection.Close(nil)

	// Expected to succeed
	if err := connection.Open(); nil != err {
		t.Errorf(tag, err, nil)
		return
	}

	// We should be connected now
	if open := connection.IsOpen(); open != true {
		t.Errorf(tag, open, true)
		return
	}

	// Expected to succeed
	if err := connection.Ping(); nil != err {
		t.Errorf(tag, err, nil)
		return
	}
}

//
// The "Client" method sould automatically re-connect if disconnected
//
func Test_RedisConnection_Client_AutomaticallyReconnects(t *testing.T) {
	tag := "[RedisConnection]['Client' Reconnects to Redis] Actual = %#v, Expected %#v"

	cmd := exec.Command("redis-server", "--port", "6999")
	if err := cmd.Start(); err != nil {
		t.Logf("skipping test; couldn't find memcached")
		return
	}
	time.Sleep(time.Duration(1) * time.Second)
	defer cmd.Wait()
	defer cmd.Process.Kill()

	// Connect to the actual redis server
	connection := RedisConnection{Url: "127.0.0.1:6999", Logger: &redis_connection_logger}
	defer connection.Close(nil)

	// Expected to succeed
	if err := connection.Open(); nil != err {
		t.Errorf(tag, err, nil)
		return
	}

	// We should be connected now
	if ok := connection.IsOpen(); !ok {
		t.Errorf(tag, ok, true)
		return
	}

	// Disconnect from Redis
	connection.Close(nil)

	// We shouldn't be connected
	if closed := connection.IsClosed(); closed != true {
		t.Errorf(tag, closed, true)
		return
	}

	// Expected to succeed
	if err := connection.Ping(); nil != err {
		t.Errorf(tag, err, nil)
		return
	}

	// We should be connected again
	if open := connection.IsOpen(); open != true {
		t.Errorf(tag, open, true)
		return
	}
}
