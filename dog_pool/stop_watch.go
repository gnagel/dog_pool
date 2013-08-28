package dog_pool

import "fmt"
import "strings"
import "time"
import "github.com/alecthomas/log4go"

type StopWatch struct {
	*log4go.Logger
	Connection interface{}
	Tags       []string

	time.Time
	time.Duration
}

func MakeStopWatch(connection interface{}, logger *log4go.Logger, tag string) *StopWatch {
	output := &StopWatch{}
	output.Logger = logger
	output.Connection = connection
	output.Tags = []string{tag}
	return output
}

func MakeStopWatchTags(connection interface{}, logger *log4go.Logger, tags []string) *StopWatch {
	output := &StopWatch{}
	output.Logger = logger
	output.Connection = connection
	output.Tags = tags
	return output
}

func (p *StopWatch) Start() *StopWatch {
	p.Time = time.Now()
	p.Duration = 0
	return p
}

func (p *StopWatch) Stop() *StopWatch {
	if p.Time.IsZero() {
		return p
	}

	p.Duration = time.Since(p.Time)
	return p
}

func (p *StopWatch) LogDuration() *StopWatch {
	return p.LogDurationAt(log4go.FINEST)
}

func (p *StopWatch) LogDurationAt(level log4go.Level) *StopWatch {
	if ns := p.Duration.Nanoseconds(); ns > 0 {
		micro := ns / int64(time.Microsecond)
		milli := ns / int64(time.Millisecond)
		sec := ns / int64(time.Second)
		p.Logger.Logc(level, func() string {
			return fmt.Sprintf("[%T | %s] Executed in %d ns / %d micro / %d milli / %d s", p.Connection, strings.Join(p.Tags, " | "), ns, micro, milli, sec)
		})
	}
	return p
}
