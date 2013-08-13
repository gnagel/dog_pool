package dog_pool

import "errors"

//
// What mode are we building the connection pool in?
//
type ConnectionMode int

//
// How should we populate the connection pool?
//
const (
	_ ConnectionMode = iota
	LAZY
	AGRESSIVE
)

//
// Constants for connecting to Memcached/Redis
//
var ErrConnectionIsClosed = errors.New("Connection is closed, command aborted")
var ErrNoConnectionsAvailable = errors.New("No Connections available")
