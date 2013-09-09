package dog_pool

import "bytes"
import "github.com/fzzy/radix/redis"

import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisBatchCommandSpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisBatchCommandSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisBatchCommandSpecs(c gospec.Context) {

	c.Specify("[RedisBatchCommand][GetCmd] Returns cmd value as String", func() {
		value := MakeRedisBatchCommand("Bob")
		c.Expect(value.cmd, gospec.Equals, "Bob")
		c.Expect(value.GetCmd(), gospec.Equals, "Bob")
	})

	c.Specify("[RedisBatchCommand][Args] Returns cmd value as String", func() {
		value := MakeRedisBatchCommandMget("Bob", "George", "Gary")
		c.Expect(value.cmd, gospec.Equals, "MGET")
		c.Expect(len(value.args), gospec.Equals, 3)
		c.Expect(value.args[0], gospec.Satisfies, bytes.Equal(value.args[0], []byte("Bob")))
		c.Expect(value.args[1], gospec.Satisfies, bytes.Equal(value.args[1], []byte("George")))
		c.Expect(value.args[2], gospec.Satisfies, bytes.Equal(value.args[2], []byte("Gary")))

		c.Expect(len(value.GetArgs()), gospec.Equals, 3)
		c.Expect(value.GetArgs()[0], gospec.Equals, "Bob")
		c.Expect(value.GetArgs()[1], gospec.Equals, "George")
		c.Expect(value.GetArgs()[2], gospec.Equals, "Gary")
	})

	c.Specify("[RedisBatchCommand][Reply] Returns redis.Reply pointer", func() {
		value := MakeRedisBatchCommand("Bob")
		value.reply = nil
		c.Expect(value.Reply(), gospec.Satisfies, nil == value.Reply())

		value.reply = &redis.Reply{}
		c.Expect(value.Reply(), gospec.Satisfies, nil != value.Reply())
		c.Expect(value.Reply(), gospec.Equals, value.reply)
	})

	c.Specify("[RedisBatchCommand][WriteArg] Appends byte slice to args", func() {
		value := MakeRedisBatchCommand("Bob")
		c.Expect(len(value.args), gospec.Equals, 0)
		c.Expect(cap(value.args), gospec.Equals, 0)

		value.WriteArg([]byte{0xFF})
		c.Expect(len(value.args), gospec.Equals, 1)
		c.Expect(value.args[0], gospec.Satisfies, bytes.Equal(value.args[0], []byte{0xFF}))
	})

	c.Specify("[RedisBatchCommand][WriteBoolArg] Appends byte slice to args", func() {
		value := MakeRedisBatchCommand("Bob")
		c.Expect(len(value.args), gospec.Equals, 0)
		c.Expect(cap(value.args), gospec.Equals, 0)

		value.WriteBoolArg(true)
		c.Expect(len(value.args), gospec.Equals, 1)
		c.Expect(value.args[0], gospec.Satisfies, bytes.Equal(value.args[0], []byte("1")))

		value.WriteBoolArg(false)
		c.Expect(len(value.args), gospec.Equals, 2)
		c.Expect(value.args[0], gospec.Satisfies, bytes.Equal(value.args[0], []byte("1")))
		c.Expect(value.args[1], gospec.Satisfies, bytes.Equal(value.args[1], []byte("0")))
	})

	c.Specify("[RedisBatchCommand][WriteStringArg] Appends byte slice to args", func() {
		value := MakeRedisBatchCommand("Bob")
		c.Expect(len(value.args), gospec.Equals, 0)
		c.Expect(cap(value.args), gospec.Equals, 0)

		value.WriteStringArg("Bob")
		c.Expect(len(value.args), gospec.Equals, 1)
		c.Expect(value.args[0], gospec.Satisfies, bytes.Equal(value.args[0], []byte("Bob")))
	})

	c.Specify("[RedisBatchCommand][WriteIntArg] Appends byte slice to args", func() {
		value := MakeRedisBatchCommand("Bob")
		c.Expect(len(value.args), gospec.Equals, 0)
		c.Expect(cap(value.args), gospec.Equals, 0)

		value.WriteIntArg(123)
		c.Expect(len(value.args), gospec.Equals, 1)
		c.Expect(value.args[0], gospec.Satisfies, bytes.Equal(value.args[0], []byte("123")))
	})

	c.Specify("[RedisBatchCommand][WriteFloatArg] Appends byte slice to args", func() {
		value := MakeRedisBatchCommand("Bob")
		c.Expect(len(value.args), gospec.Equals, 0)
		c.Expect(cap(value.args), gospec.Equals, 0)

		value.WriteFloatArg(123.456)
		c.Expect(len(value.args), gospec.Equals, 1)
		c.Expect(string(value.args[0]), gospec.Satisfies, bytes.Equal(value.args[0], []byte("123.456000")))
	})

}
