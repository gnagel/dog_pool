package dog_pool

import "testing"
import "github.com/orfjackal/gospec/src/gospec"

func TestRedisBatchCommandSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.AddSpec(RedisBatchCommandSpecs)
	gospec.MainGoTest(r, t)
}

// Helpers
func RedisBatchCommandSpecs(c gospec.Context) {

	c.Specify("Constants", func() {
		c.Expect(BITCOUNT, gospec.Equals, "BITCOUNT")
		c.Expect(BITOP, gospec.Equals, "BITOP")
		c.Expect(BIT_AND, gospec.Equals, "AND")
		c.Expect(BIT_OR, gospec.Equals, "OR")
		c.Expect(BIT_NOT, gospec.Equals, "NOT")
	})

	c.Specify("[MakeBitopAnd] Makes Bitop Command", func() {
		command := MakeBitopAnd("DEST", []string{"A", "B", "C"})
		c.Expect(command.Cmd, gospec.Equals, "BITOP")
		c.Expect(len(command.Args), gospec.Equals, 5)
		c.Expect(command.Args[0], gospec.Equals, "AND")
		c.Expect(command.Args[1], gospec.Equals, "DEST")
		c.Expect(command.Args[2], gospec.Equals, "A")
		c.Expect(command.Args[3], gospec.Equals, "B")
		c.Expect(command.Args[4], gospec.Equals, "C")
	})

	c.Specify("[MakeBitopOr] Makes Bitop Command", func() {
		command := MakeBitopOr("DEST", []string{"A", "B", "C"})
		c.Expect(command.Cmd, gospec.Equals, "BITOP")
		c.Expect(len(command.Args), gospec.Equals, 5)
		c.Expect(command.Args[0], gospec.Equals, "OR")
		c.Expect(command.Args[1], gospec.Equals, "DEST")
		c.Expect(command.Args[2], gospec.Equals, "A")
		c.Expect(command.Args[3], gospec.Equals, "B")
		c.Expect(command.Args[4], gospec.Equals, "C")
	})

	c.Specify("[MakeBitopNot] Makes Bitop Command", func() {
		command := MakeBitopNot("DEST", []string{"A", "B", "C"})
		c.Expect(command.Cmd, gospec.Equals, "BITOP")
		c.Expect(len(command.Args), gospec.Equals, 5)
		c.Expect(command.Args[0], gospec.Equals, "NOT")
		c.Expect(command.Args[1], gospec.Equals, "DEST")
		c.Expect(command.Args[2], gospec.Equals, "A")
		c.Expect(command.Args[3], gospec.Equals, "B")
		c.Expect(command.Args[4], gospec.Equals, "C")
	})

	c.Specify("[MakeBitCount] Makes Bitop Command", func() {
		command := MakeBitCount("A")
		c.Expect(command.Cmd, gospec.Equals, "BITCOUNT")
		c.Expect(len(command.Args), gospec.Equals, 1)
		c.Expect(command.Args[0], gospec.Equals, "A")
	})

	c.Specify("[SelectBitopDestKeys] Selects only the destination keys from BITOP ... commands", func() {
		commands := []*RedisBatchCommand{}
		commands = append(commands, MakeBitCount("A"))
		commands = append(commands, MakeBitopNot("NOT-1", []string{"A", "B", "C"}))
		commands = append(commands, MakeGet("C"))
		commands = append(commands, MakeBitopAnd("AND-2", []string{"A", "B", "C"}))
		commands = append(commands, MakeDelete([]string{"D"}))
		commands = append(commands, MakeBitopOr("OR-3", []string{"A", "B", "C"}))
		commands = append(commands, MakeBitCount("OR-3"))

		keys := SelectBitopDestKeys(commands)
		c.Expect(keys, gospec.Satisfies, nil != keys)
		c.Expect(len(keys), gospec.Equals, 3)
		c.Expect(keys[0], gospec.Equals, "NOT-1")
		c.Expect(keys[1], gospec.Equals, "AND-2")
		c.Expect(keys[2], gospec.Equals, "OR-3")
	})

}
