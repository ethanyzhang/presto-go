package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBiMap(t *testing.T) {
	input := map[string]int{
		"one": 1,
		"two": 2,
	}
	biMap := NewBiMap(input)

	t.Run("Lookup", func(t *testing.T) {
		val, ok := biMap.Lookup("one")
		assert.True(t, ok)
		assert.Equal(t, 1, val)

		val, ok = biMap.Lookup("three")
		assert.False(t, ok)
		assert.Equal(t, 0, val)
	})

	t.Run("DirectLookup", func(t *testing.T) {
		val := biMap.DirectLookup("two")
		assert.Equal(t, 2, val)

		val = biMap.DirectLookup("three")
		assert.Equal(t, 0, val)
	})

	t.Run("RLookup", func(t *testing.T) {
		key, ok := biMap.RLookup(1)
		assert.True(t, ok)
		assert.Equal(t, "one", key)

		key, ok = biMap.RLookup(3)
		assert.False(t, ok)
		assert.Equal(t, "", key)
	})

	t.Run("DirectRLookup", func(t *testing.T) {
		key := biMap.DirectRLookup(2)
		assert.Equal(t, "two", key)

		key = biMap.DirectRLookup(3)
		assert.Equal(t, "", key)
	})

	t.Run("DuplicateValues", func(t *testing.T) {
		input := map[string]int{
			"one": 1,
			"uno": 1,
		}
		biMap := NewBiMap(input)

		// Forward lookup should work for both
		val, ok := biMap.Lookup("one")
		assert.True(t, ok)
		assert.Equal(t, 1, val)

		val, ok = biMap.Lookup("uno")
		assert.True(t, ok)
		assert.Equal(t, 1, val)

		// Reverse lookup will return one of them
		key, ok := biMap.RLookup(1)
		assert.True(t, ok)
		assert.Contains(t, []string{"one", "uno"}, key)
	})

	t.Run("EmptyMap", func(t *testing.T) {
		input := map[string]int{}
		biMap := NewBiMap(input)

		val, ok := biMap.Lookup("anything")
		assert.False(t, ok)
		assert.Equal(t, 0, val)

		key, ok := biMap.RLookup(123)
		assert.False(t, ok)
		assert.Equal(t, "", key)
	})

	t.Run("Immutability", func(t *testing.T) {
		input := map[string]int{
			"initial": 100,
		}
		biMap := NewBiMap(input)

		// 1. Modify the original source map
		input["initial"] = 999
		input["new_key"] = 200

		// 2. Verify BiMap still reflects the state at the time of creation
		val, ok := biMap.Lookup("initial")
		assert.True(t, ok)
		assert.Equal(t, 100, val, "BiMap value should not change when source map is modified")

		_, ok = biMap.Lookup("new_key")
		assert.False(t, ok, "BiMap should not contain keys added to the source map after initialization")

		// 3. Verify reverse mapping remains intact
		key, ok := biMap.RLookup(100)
		assert.True(t, ok)
		assert.Equal(t, "initial", key)

		_, ok = biMap.RLookup(999)
		assert.False(t, ok, "BiMap reverse mapping should not reflect changes to source map values")
	})
}
