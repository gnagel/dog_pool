package dog_pool

import "testing"
import "github.com/orfjackal/gospec/src/gospec"

//
// NOTE: Use differient ports for each test!
//       gospec runs the specs in parallel!
//
func TestFindPortSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.AddSpec(FindPortSpecs)
	gospec.MainGoTest(r, t)
}

func FindPortSpecs(c gospec.Context) {
	c.Specify("[findPort] Finds an open port", func() {
		port, err := findPort()
		c.Expect(err, gospec.Equals, nil)
		c.Expect(port, gospec.Satisfies, port >= 1024)
	})

}
