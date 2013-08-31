package dog_pool

import "time"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"
import "github.com/alecthomas/log4go"

func TestStopWatchSpecs(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(StopWatchSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func StopWatchSpecs(c gospec.Context) {

	c.Specify("[StopWatch] Makes StopWatch", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		value := MakeStopWatch(c, &logger, "Make")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.Logger, gospec.Equals, &logger)
		c.Expect(value.Connection, gospec.Equals, c)
		c.Expect(value.Tags[0], gospec.Equals, "Make")
		c.Expect(value.Time, gospec.Satisfies, value.Time.IsZero())
		c.Expect(value.Duration, gospec.Equals, time.Duration(0))
	})

	c.Specify("[StopWatch] Starts StopWatch", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		value := MakeStopWatch(c, &logger, "Make")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.Logger, gospec.Equals, &logger)
		c.Expect(value.Connection, gospec.Equals, c)
		c.Expect(value.Tags[0], gospec.Equals, "Make")
		c.Expect(value.Time, gospec.Satisfies, value.Time.IsZero())
		c.Expect(value.Duration, gospec.Equals, time.Duration(0))

		value.Start()
		c.Expect(value.Time, gospec.Satisfies, !value.Time.IsZero())
		c.Expect(value.Duration, gospec.Equals, time.Duration(0))

		time.Sleep(time.Duration(1) * time.Microsecond)
	})

	c.Specify("[StopWatch] Stops StopWatch", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		value := MakeStopWatch(c, &logger, "Make")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.Logger, gospec.Equals, &logger)
		c.Expect(value.Connection, gospec.Equals, c)
		c.Expect(value.Tags[0], gospec.Equals, "Make")
		c.Expect(value.Time, gospec.Satisfies, value.Time.IsZero())
		c.Expect(value.Duration, gospec.Equals, time.Duration(0))

		// Don't error out or panic if the stop watch was not started!
		value.Stop()
		c.Expect(value.Time, gospec.Satisfies, value.Time.IsZero())
		c.Expect(value.Duration, gospec.Equals, time.Duration(0))

		// Start the stop watch
		value.Start()
		c.Expect(value.Time, gospec.Satisfies, !value.Time.IsZero())
		c.Expect(value.Duration, gospec.Equals, time.Duration(0))

		// Sleep, then stop the stop watch
		time.Sleep(time.Duration(2) * time.Microsecond)
		value.Stop()
		c.Expect(value.Time, gospec.Satisfies, !value.Time.IsZero())
		c.Expect(value.Duration, gospec.Satisfies, value.Duration > 0)
		c.Expect(value.Duration.Nanoseconds(), gospec.Satisfies, value.Duration.Nanoseconds() >= 2*1000)
	})

	c.Specify("[StopWatch] Logs StopWatch", func() {
		logger := log4go.NewDefaultLogger(log4go.CRITICAL)
		value := MakeStopWatch(c, &logger, "Make")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.Logger, gospec.Equals, &logger)
		c.Expect(value.Connection, gospec.Equals, c)
		c.Expect(value.Tags[0], gospec.Equals, "Make")
		c.Expect(value.Time, gospec.Satisfies, value.Time.IsZero())
		c.Expect(value.Duration, gospec.Equals, time.Duration(0))

		// Start the stop watch
		value.Start()
		c.Expect(value.Time, gospec.Satisfies, !value.Time.IsZero())

		// Sleep, then stop the stop watch
		time.Sleep(time.Duration(2) * time.Microsecond)
		value.Stop()

		// Execute the logger
		value.LogDuration()
	})

}
