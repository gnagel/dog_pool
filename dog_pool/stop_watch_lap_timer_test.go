package dog_pool

import "time"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestStopWatchTimerLapSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(StopWatchTimerLapSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func StopWatchTimerLapSpecs(c gospec.Context) {

	c.Specify("[StopWatchTimerLap] New StopWatchTimer is not started or stoppped", func() {
		value := &StopWatchTimerLap{}
		c.Expect(value.IsStarted(), gospec.Equals, false)
		c.Expect(value.IsStopped(), gospec.Equals, false)
	})

	c.Specify("[StopWatchTimerLap] Started StopWatchTimer is started and not stoppped", func() {
		value := &StopWatchTimerLap{}
		value.Start()
		c.Expect(value.IsStarted(), gospec.Equals, true)
		c.Expect(value.IsStopped(), gospec.Equals, false)
	})

	c.Specify("[StopWatchTimerLap] Stopped StopWatchTimer is not started and is stoppped", func() {
		value := &StopWatchTimerLap{}
		value.Start()
		value.Stop()
		c.Expect(value.IsStarted(), gospec.Equals, true)
		c.Expect(value.IsStopped(), gospec.Equals, true)
	})

	c.Specify("[StopWatchTimerLap] Formats String", func() {
		value := &StopWatchTimerLap{}
		value.tag = "Bob"
		value.duration = time.Duration(1500) * time.Microsecond
		c.Expect(value.String(), gospec.Equals, "Bob = 1500 micros")
	})

	c.Specify("[StopWatchTimerLaps] Creates StopWatchTimer", func() {
		laps := &StopWatchTimerLaps{}
		c.Expect(len(laps.laps), gospec.Equals, 0)

		timer := laps.CreateStopWatch("Bob")
		c.Expect(timer.IsStarted(), gospec.Equals, false)

		c.Expect(len(laps.laps), gospec.Equals, 1)
		c.Expect(laps.laps[0], gospec.Equals, timer)
	})

	c.Specify("[StopWatchTimerLaps] Create+Start StopWatchTimer", func() {
		laps := &StopWatchTimerLaps{}
		c.Expect(len(laps.laps), gospec.Equals, 0)

		timer := laps.StartStopWatch("Bob")
		c.Expect(timer.IsStarted(), gospec.Equals, true)
		c.Expect(timer.IsStopped(), gospec.Equals, false)

		c.Expect(len(laps.laps), gospec.Equals, 1)
		c.Expect(laps.laps[0], gospec.Equals, timer)
	})

	c.Specify("[StopWatchTimerLaps] Logs times", func() {
		laps := &StopWatchTimerLaps{}

		laps.StartStopWatch("Bob").duration = time.Duration(1500) * time.Microsecond
		laps.StartStopWatch("Gary").duration = time.Duration(15) * time.Microsecond
		laps.StartStopWatch("George").duration = time.Duration(200) * time.Microsecond

		c.Expect(laps.String(), gospec.Equals, "Bob = 1500 micros, Gary = 15 micros, George = 200 micros")
	})

	c.Specify("[CreateStopWatchTimerLaps] Creates with 'Total' timer pre-set", func() {
		laps := CreateStopWatchTimerLaps()
		c.Expect(len(laps.laps), gospec.Equals, 1)
		c.Expect(laps.laps[0].tag, gospec.Equals, "Net Time")
		c.Expect(laps.laps[0].IsStarted(), gospec.Equals, true)
		c.Expect(laps.laps[0].IsStopped(), gospec.Equals, false)
	})

}
