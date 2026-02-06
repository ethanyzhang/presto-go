package presto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- NullSlice ---

func TestNullSlice_Scan(t *testing.T) {
	t.Run("string source", func(t *testing.T) {
		var s NullSlice[int]
		err := s.Scan(`[1,2,3]`)
		require.NoError(t, err)
		assert.True(t, s.Valid)
		assert.Equal(t, []int{1, 2, 3}, s.Slice)
	})

	t.Run("byte source", func(t *testing.T) {
		var s NullSlice[string]
		err := s.Scan([]byte(`["a","b"]`))
		require.NoError(t, err)
		assert.True(t, s.Valid)
		assert.Equal(t, []string{"a", "b"}, s.Slice)
	})

	t.Run("nil source", func(t *testing.T) {
		var s NullSlice[int]
		err := s.Scan(nil)
		require.NoError(t, err)
		assert.False(t, s.Valid)
		assert.Nil(t, s.Slice)
	})

	t.Run("invalid JSON", func(t *testing.T) {
		var s NullSlice[int]
		err := s.Scan(`{not json}`)
		assert.Error(t, err)
	})

	t.Run("unsupported type", func(t *testing.T) {
		var s NullSlice[int]
		err := s.Scan(42)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot scan int")
	})
}

func TestNullSlice_Value(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		s := NullSlice[int]{Slice: []int{1, 2}, Valid: true}
		v, err := s.Value()
		require.NoError(t, err)
		assert.Equal(t, "[1,2]", v)
	})

	t.Run("null", func(t *testing.T) {
		s := NullSlice[int]{Valid: false}
		v, err := s.Value()
		require.NoError(t, err)
		assert.Nil(t, v)
	})
}

// --- NullMap ---

func TestNullMap_Scan(t *testing.T) {
	t.Run("string source", func(t *testing.T) {
		var m NullMap[string, int]
		err := m.Scan(`{"a":1,"b":2}`)
		require.NoError(t, err)
		assert.True(t, m.Valid)
		assert.Equal(t, map[string]int{"a": 1, "b": 2}, m.Map)
	})

	t.Run("nil source", func(t *testing.T) {
		var m NullMap[string, int]
		err := m.Scan(nil)
		require.NoError(t, err)
		assert.False(t, m.Valid)
		assert.Nil(t, m.Map)
	})

	t.Run("invalid JSON", func(t *testing.T) {
		var m NullMap[string, int]
		err := m.Scan(`not json`)
		assert.Error(t, err)
	})

	t.Run("unsupported type", func(t *testing.T) {
		var m NullMap[string, int]
		err := m.Scan(42)
		assert.Error(t, err)
	})
}

func TestNullMap_Value(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		m := NullMap[string, int]{Map: map[string]int{"x": 1}, Valid: true}
		v, err := m.Value()
		require.NoError(t, err)
		assert.Equal(t, `{"x":1}`, v)
	})

	t.Run("null", func(t *testing.T) {
		m := NullMap[string, int]{Valid: false}
		v, err := m.Value()
		require.NoError(t, err)
		assert.Nil(t, v)
	})
}

// --- NullRow ---

func TestNullRow_Scan(t *testing.T) {
	type Address struct {
		Street string `json:"street"`
		City   string `json:"city"`
	}

	t.Run("struct target", func(t *testing.T) {
		var r NullRow[Address]
		err := r.Scan(`{"street":"123 Main St","city":"Springfield"}`)
		require.NoError(t, err)
		assert.True(t, r.Valid)
		assert.Equal(t, "123 Main St", r.Row.Street)
		assert.Equal(t, "Springfield", r.Row.City)
	})

	t.Run("map target", func(t *testing.T) {
		var r NullRow[map[string]any]
		err := r.Scan(`{"street":"123 Main St","city":"Springfield"}`)
		require.NoError(t, err)
		assert.True(t, r.Valid)
		assert.Equal(t, "123 Main St", r.Row["street"])
	})

	t.Run("nil source", func(t *testing.T) {
		var r NullRow[Address]
		err := r.Scan(nil)
		require.NoError(t, err)
		assert.False(t, r.Valid)
		assert.Equal(t, Address{}, r.Row)
	})

	t.Run("byte source", func(t *testing.T) {
		var r NullRow[Address]
		err := r.Scan([]byte(`{"street":"Oak Ave","city":"Portland"}`))
		require.NoError(t, err)
		assert.True(t, r.Valid)
		assert.Equal(t, "Oak Ave", r.Row.Street)
	})

	t.Run("invalid JSON", func(t *testing.T) {
		var r NullRow[Address]
		err := r.Scan(`{bad}`)
		assert.Error(t, err)
	})

	t.Run("unsupported type", func(t *testing.T) {
		var r NullRow[Address]
		err := r.Scan(42)
		assert.Error(t, err)
	})
}

func TestNullRow_Value(t *testing.T) {
	type Point struct {
		X int `json:"x"`
		Y int `json:"y"`
	}

	t.Run("valid", func(t *testing.T) {
		r := NullRow[Point]{Row: Point{X: 1, Y: 2}, Valid: true}
		v, err := r.Value()
		require.NoError(t, err)
		assert.Equal(t, `{"x":1,"y":2}`, v)
	})

	t.Run("null", func(t *testing.T) {
		r := NullRow[Point]{Valid: false}
		v, err := r.Value()
		require.NoError(t, err)
		assert.Nil(t, v)
	})
}
