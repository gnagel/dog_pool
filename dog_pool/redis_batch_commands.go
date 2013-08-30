package dog_pool

import "fmt"
import "errors"
import "strings"
import "github.com/alecthomas/log4go"

type RedisBatchCommands []*RedisBatchCommand

func (commands RedisBatchCommands) ExecuteBatch(connection RedisClientInterface) error {
	client, ok := connection.(*RedisConnection)
	if !ok {
		return errors.New(fmt.Sprintf("Error casting %T to *RedisConnection, connection = %#v", connection, connection))
	}

	stop_watch := MakeStopWatchTags(commands, client.Logger, []string{client.Url, client.Id, "[BatchCommands][ExecuteBatch]"}).Start()

	stop_watch_commands := make([]*StopWatch, len(commands))
	for index, command := range commands {
		stop_watch_commands[index] = MakeStopWatch(commands, client.Logger, strings.Join([]string{"[BatchCommands][ExecuteBatch][Cmd]", command.Cmd}, " ")).Start()

		if nil == command.Args {
			client.Append(command.Cmd)
		} else {
			args := make([]interface{}, len(command.Args))
			for i, arg := range command.Args {
				args[i] = arg
			}
			client.Append(command.Cmd, args...)
		}
	}

	for index := range commands {
		command := commands[index]
		command.Reply = client.GetReply()

		stop_watch_commands[index].Stop().LogDurationAt(log4go.FINEST)

		if client.IsClosed() {
			return errors.New(fmt.Sprintf("[BatchCommands][ExecuteBatch] Connection closed while getting reply for cmd = %v", command))
		}
	}

	stop_watch.Stop().LogDurationAt(log4go.TRACE)

	return nil
}
