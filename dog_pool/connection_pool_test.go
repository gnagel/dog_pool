package dog_pool

import "testing"

type stringWrapper struct {
	Value string "simple value"
}

//
// ConnectionPool: GetConnection
//

func Test_ConnectionPool_GetConnection_1(t *testing.T) {
	tag := "GetConnection - Nil Input, Nil Output"

	pool, _ := MakeConnectionPoolWrapper(1, func() (interface{}, error) {
		return nil, nil
	})

	// Pool contains 1 connection
	if c := pool.GetConnection(); c != nil {
		t.Errorf("[%s] Expected=%#v, Actual=%#v", tag, nil, c)
		return
	}

	// Pool contains 0 connections
	if c := pool.GetConnection(); c != nil {
		t.Errorf("[%s] Expected=%#v, Actual=%#v", tag, nil, c)
		return
	}
}

func Test_ConnectionPool_GetConnection_2(t *testing.T) {
	tag := "GetConnection - Non-Nil Input, Non-Nil Output"

	expected := &stringWrapper{Value: "Hello"}

	pool, _ := MakeConnectionPoolWrapper(1, func() (interface{}, error) {
		return expected, nil
	})

	// Pool contains 1 connection
	if c := pool.GetConnection(); c == nil || c.(*stringWrapper).Value != expected.Value {
		t.Errorf("[%s] Expected=%#v, Actual=%#v", tag, expected, c)
		return
	}

	// Pool contains 0 connections
	if c := pool.GetConnection(); c != nil {
		t.Errorf("[%s] Expected=%#v, Actual=%#v", tag, nil, c)
		return
	}
}

//
// ConnectionPool: ReleaseConnection
//

func Test_ConnectionPool_ReleaseConnection_1(t *testing.T) {
	tag := "ReleaseConnection - Nil Input, Nil Output"

	pool, _ := MakeConnectionPoolWrapper(1, func() (interface{}, error) {
		return nil, nil
	})

	// Pool contains 1 connection
	c := pool.GetConnection()

	if c != nil {
		t.Errorf("[%s] Expected=%#v, Actual=%#v", tag, nil, c)
		return
	}

	// Push the connection back into the pool
	pool.ReleaseConnection(c)
}

func Test_ConnectionPool_ReleaseConnection_2(t *testing.T) {
	tag := "ReleaseConnection - Non-Nil Input, Non-Nil Output"

	expected := &stringWrapper{Value: "Hello"}

	pool, _ := MakeConnectionPoolWrapper(1, func() (interface{}, error) {
		return expected, nil
	})

	// Pool contains 1 connection
	client := pool.GetConnection()
	if c := client; c == nil || c.(*stringWrapper).Value != expected.Value {
		t.Errorf("[%s] Expected=%#v, Actual=%#v", tag, expected, c)
		return
	}

	// Push the connection back into the pool
	pool.ReleaseConnection(client)
	client = nil

	// Pool contains 1 connection
	if c := pool.GetConnection(); c == nil || c.(*stringWrapper).Value != expected.Value {
		t.Errorf("[%s] Expected=%#v, Actual=%#v", tag, expected, c)
		return
	}
}
