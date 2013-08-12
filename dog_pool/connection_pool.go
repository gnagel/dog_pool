//
// Abstract Connection Pool Interface written in GO.
//

package dog_pool

type InitFunction func() (interface{}, error)

//
// Wrapper around a buffered Channel
//
type ConnectionPoolWrapper struct {
	size int              "Number of connections in the pool"
	conn chan interface{} "Buffered Channel of 'interface{}' objects"
}

//
// Create a new instance of the Pool and initialize it with N-many connections
//
func NewPool(size int, initfn InitFunction) (*ConnectionPoolWrapper, error) {
	// Create a buffered channel allowing size senders
	output := &ConnectionPoolWrapper{}
	output.size = size
	output.conn = make(chan interface{}, size)

	// Create a buffered channel allowing size senders
	output.conn = make(chan interface{}, size)

	// Fill the pool with connections
	for x := 0; x < size; x++ {
		// Create the connection.
		// Nil is a valid value here
		conn, err := initfn()

		// Abort on errors
		if err != nil {
			return nil, err
		}

		// If the init function succeeded, add the connection to the channel
		output.conn <- conn
	}

	// Return the new pool
	return output, nil
}

//
// Get a connection from the pool
//
// Output:
//   interface{} --> Pop'd a value from the pool
//   nil         --> Pool was empty, or contained nil placeholder
//
func (p *ConnectionPoolWrapper) GetConnection() interface{} {
	select {
	// Channel is not empty!
	case c := <-p.conn:
		return c
		// Channel is empty!
	default:
		return nil
	}

	return nil
}

//
// Return a connection from the pool
//
// NOTE: Nil is a value connection value here
//
func (p *ConnectionPoolWrapper) ReleaseConnection(conn interface{}) {
	p.conn <- conn
}

//
// Size of the pool
//
func (p *ConnectionPoolWrapper) Size() int {
	return p.size
}

//
// Length of the channel
//
func (p *ConnectionPoolWrapper) Len() int {
	return len(p.conn)
}
