package dog_pool

import "strconv"

//
// Helper to iterate urls
//
func loopStrings(values []string) func() []string {
	i := 0
	return func() []string {
		value := values[i%len(values)]
		i++
		return []string{value, strconv.Itoa(i)}
	}
}
