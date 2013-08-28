package dog_pool

import "time"
import "github.com/alecthomas/log4go"

type StopWatch struct {
	*log4go.Logger
	Connection interface{}
	Operation  string

	time.Time
	time.Duration
}

func MakeStopWatch(connection interface{}, logger *log4go.Logger, operation string) *StopWatch {
	output := &StopWatch{}
	output.Logger = logger
	output.Connection = connection
	output.Operation = operation
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
	if ns := p.Duration.Nanoseconds(); ns > 0 {
		p.Logger.Fine("[%T][%s] Executed in %d (ns)", p.Connection, p.Operation, ns)
	}
	return p
}
