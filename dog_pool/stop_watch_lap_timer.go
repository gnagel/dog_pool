package dog_pool

import "fmt"
import "strings"
import "time"

type StopWatchTimerLap struct {
	tag      string
	start_at time.Time
	duration time.Duration
}

type StopWatchTimerLaps struct {
	laps []*StopWatchTimerLap
}

func (p *StopWatchTimerLap) IsStarted() bool {
	return !p.start_at.IsZero()
}

func (p *StopWatchTimerLap) IsStopped() bool {
	return p.duration != 0
}

func (p *StopWatchTimerLap) Start() *StopWatchTimerLap {
	p.start_at = time.Now()
	p.duration = 0
	return p
}

func (p *StopWatchTimerLap) Stop() *StopWatchTimerLap {
	if !p.IsStarted() {
		return p
	}

	p.duration = time.Since(p.start_at)
	return p
}

func (p *StopWatchTimerLap) Nanoseconds() int64 {
	return p.duration.Nanoseconds()
}

func (p *StopWatchTimerLap) Microseconds() int64 {
	return p.duration.Nanoseconds() / int64(time.Microsecond)
}

func (p *StopWatchTimerLap) Milliseconds() int64 {
	return p.duration.Nanoseconds() / int64(time.Millisecond)
}

func (p *StopWatchTimerLap) Seconds() int64 {
	return p.duration.Nanoseconds() / int64(time.Second)
}

func (p *StopWatchTimerLap) String() string {
	return fmt.Sprintf("%s = %d micros", p.tag, p.Microseconds())
}

func CreateStopWatchTimerLaps() *StopWatchTimerLaps {
	output := &StopWatchTimerLaps{}
	output.laps = make([]*StopWatchTimerLap, 50)[0:0]

	output.StartStopWatch("Net Time")

	return output
}

// Create a new stopwatch timer
func (p *StopWatchTimerLaps) CreateStopWatch(tag string) *StopWatchTimerLap {
	output := &StopWatchTimerLap{}
	output.tag = tag

	p.laps = append(p.laps, output)
	return output
}

// Create and start a stopwatch timer
func (p *StopWatchTimerLaps) StartStopWatch(tag string) *StopWatchTimerLap {
	return p.CreateStopWatch(tag).Start()
}

// Format the stopwatch as a string
func (p *StopWatchTimerLaps) String() string {
	buffer := make([]string, len(p.laps))[0:0]

	for _, timer := range p.laps {
		switch {
		case !timer.IsStarted():
			// Panic on any un-started timers
			panic(fmt.Sprintf("Unstarted timer: %s", timer.tag))

		case !timer.IsStopped():
			// Stop any still running timers (i.e the "total" timer)
			timer.Stop()
			fallthrough

		default:
			// IsStopped() == true
			buffer = append(buffer, fmt.Sprintf("%s = %d", timer.tag, timer.Microseconds()))
		}
	}

	return fmt.Sprintf("Lap Times [%s] micros", strings.Join(buffer, ", "))
}
