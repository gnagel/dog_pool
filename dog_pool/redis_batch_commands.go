package dog_pool

//
// Typedef for an array of RedisBatchCommand commands
//
type RedisBatchCommands []*RedisBatchCommand

//
// Execute the batch on a connection
//
func (commands RedisBatchCommands) ExecuteBatch(connection RedisClientInterface) (err error) {
	err = nil

	// Append the commands
	for _, command := range commands {
		command.RedisAppend(connection)
	}

	// Execute the commands
	for _, command := range commands {
		command_err := command.RedisGetReply(connection).Err
		if nil != command_err {
			err = command_err
		}
	}

	// Return the error if any was found
	return err
}
