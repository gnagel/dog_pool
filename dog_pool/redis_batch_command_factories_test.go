package dog_pool

import "time"
import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisBatchCommandFactorySpecs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in benchmark mode.")
		return
	}
	r := gospec.NewRunner()
	r.AddSpec(RedisBatchCommandFactorySpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisBatchCommandFactorySpecs(c gospec.Context) {

	c.Specify("Constants", func() {
		c.Expect(string(cmd_bitop), gospec.Equals, "BITOP")
		c.Expect(string(cmd_bitop_and), gospec.Equals, "AND")
		c.Expect(string(cmd_bitop_not), gospec.Equals, "NOT")
		c.Expect(string(cmd_bitop_or), gospec.Equals, "OR")

		c.Expect(string(cmd_bitcount), gospec.Equals, "BITCOUNT")
		c.Expect(string(cmd_getbit), gospec.Equals, "GETBIT")
		c.Expect(string(cmd_setbit), gospec.Equals, "SETBIT")

		c.Expect(string(cmd_mget), gospec.Equals, "MGET")
		c.Expect(string(cmd_get), gospec.Equals, "GET")
		c.Expect(string(cmd_set), gospec.Equals, "SET")
		c.Expect(string(cmd_del), gospec.Equals, "DEL")
	})

	c.Specify("[MakeRedisBatchCommand] Makes simple Command", func() {
		value := MakeRedisBatchCommand("PING")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "PING")
	})

	c.Specify("[MakeRedisBatchCommand] Does not mutate or validate the 'CMD' value", func() {
		// Upper Case
		value := MakeRedisBatchCommand("PING")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "PING")

		// Lower case:
		value = MakeRedisBatchCommand("ping")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "ping")

		// Mixed case & Invalid Command:
		value = MakeRedisBatchCommand("Bob")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "Bob")
	})

	c.Specify("[MakeRedisBatchCommand][ExpireIn] Makes command", func() {
		value := MakeRedisBatchCommandExpireIn("A", time.Duration(15)*time.Hour)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "EXPIRE")
		c.Expect(len(value.GetArgs()), gospec.Equals, 2)
		c.Expect(value.GetArgs()[0], gospec.Equals, "A")
		c.Expect(value.GetArgs()[1], gospec.Equals, "54000")
	})

	c.Specify("[MakeRedisBatchCommand][Delete] Makes command", func() {
		value := MakeRedisBatchCommandDelete("A", "B", "C")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "DEL")
		c.Expect(len(value.GetArgs()), gospec.Equals, 3)
		c.Expect(value.GetArgs()[0], gospec.Equals, "A")
		c.Expect(value.GetArgs()[1], gospec.Equals, "B")
		c.Expect(value.GetArgs()[2], gospec.Equals, "C")
	})

	c.Specify("[MakeRedisBatchCommand][Mget] Makes command", func() {
		value := MakeRedisBatchCommandMget("A", "B", "C")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "MGET")
		c.Expect(len(value.GetArgs()), gospec.Equals, 3)
		c.Expect(value.GetArgs()[0], gospec.Equals, "A")
		c.Expect(value.GetArgs()[1], gospec.Equals, "B")
		c.Expect(value.GetArgs()[2], gospec.Equals, "C")
	})

	c.Specify("[MakeRedisBatchCommand][Get] Makes command", func() {
		value := MakeRedisBatchCommandGet("A")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "GET")
		c.Expect(len(value.GetArgs()), gospec.Equals, 1)
		c.Expect(value.GetArgs()[0], gospec.Equals, "A")
	})

	c.Specify("[MakeRedisBatchCommand][Set] Makes command", func() {
		value := MakeRedisBatchCommandSet("A", []byte("Bob"))
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "SET")
		c.Expect(len(value.GetArgs()), gospec.Equals, 2)
		c.Expect(value.GetArgs()[0], gospec.Equals, "A")
		c.Expect(value.GetArgs()[1], gospec.Equals, "Bob")
	})

	c.Specify("[MakeRedisBatchCommand][Bitop][And] Makes command", func() {
		value := MakeRedisBatchCommandBitopAnd("DEST", "Bob", "Gary", "George")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "BITOP")
		c.Expect(len(value.GetArgs()), gospec.Equals, 5)
		c.Expect(value.GetArgs()[0], gospec.Equals, "AND")
		c.Expect(value.GetArgs()[1], gospec.Equals, "DEST")
		c.Expect(value.GetArgs()[2], gospec.Equals, "Bob")
		c.Expect(value.GetArgs()[3], gospec.Equals, "Gary")
		c.Expect(value.GetArgs()[4], gospec.Equals, "George")
	})

	c.Specify("[MakeRedisBatchCommand][Bitop][Not] Makes command", func() {
		value := MakeRedisBatchCommandBitopNot("DEST", "SRC")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "BITOP")
		c.Expect(len(value.GetArgs()), gospec.Equals, 3)
		c.Expect(value.GetArgs()[0], gospec.Equals, "NOT")
		c.Expect(value.GetArgs()[1], gospec.Equals, "DEST")
		c.Expect(value.GetArgs()[2], gospec.Equals, "SRC")
	})

	c.Specify("[MakeRedisBatchCommand][Bitop][Or] Makes command", func() {
		value := MakeRedisBatchCommandBitopOr("DEST", "Bob", "Gary", "George")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "BITOP")
		c.Expect(len(value.GetArgs()), gospec.Equals, 5)
		c.Expect(value.GetArgs()[0], gospec.Equals, "OR")
		c.Expect(value.GetArgs()[1], gospec.Equals, "DEST")
		c.Expect(value.GetArgs()[2], gospec.Equals, "Bob")
		c.Expect(value.GetArgs()[3], gospec.Equals, "Gary")
		c.Expect(value.GetArgs()[4], gospec.Equals, "George")
	})

	c.Specify("[MakeRedisBatchCommand][BitCount] Makes command", func() {
		value := MakeRedisBatchCommandBitCount("KEY")
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "BITCOUNT")
		c.Expect(len(value.GetArgs()), gospec.Equals, 1)
		c.Expect(value.GetArgs()[0], gospec.Equals, "KEY")
	})

	c.Specify("[MakeRedisBatchCommand][Getbit] Makes command", func() {
		value := MakeRedisBatchCommandGetBit("KEY", 123)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "GETBIT")
		c.Expect(len(value.GetArgs()), gospec.Equals, 2)
		c.Expect(value.GetArgs()[0], gospec.Equals, "KEY")
		c.Expect(value.GetArgs()[1], gospec.Equals, "123")
	})

	c.Specify("[MakeRedisBatchCommand][Setbit] Makes command", func() {
		// ON:
		value := MakeRedisBatchCommandSetBit("KEY", 123, true)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "SETBIT")
		c.Expect(len(value.GetArgs()), gospec.Equals, 3)
		c.Expect(value.GetArgs()[0], gospec.Equals, "KEY")
		c.Expect(value.GetArgs()[1], gospec.Equals, "123")
		c.Expect(value.GetArgs()[2], gospec.Equals, "1")

		// OFF:
		value = MakeRedisBatchCommandSetBit("KEY", 123, false)
		c.Expect(value, gospec.Satisfies, nil != value)
		c.Expect(value.GetCmd(), gospec.Equals, "SETBIT")
		c.Expect(len(value.GetArgs()), gospec.Equals, 3)
		c.Expect(value.GetArgs()[0], gospec.Equals, "KEY")
		c.Expect(value.GetArgs()[1], gospec.Equals, "123")
		c.Expect(value.GetArgs()[2], gospec.Equals, "0")
	})

}
