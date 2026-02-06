package presto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeUnit_String(t *testing.T) {
	assert.Equal(t, "NONE", RuntimeUnitNone.String())
	assert.Equal(t, "NANO", RuntimeUnitNano.String())
	assert.Equal(t, "BYTE", RuntimeUnitByte.String())
	assert.Equal(t, "99", RuntimeUnit(99).String())
}

func TestRuntimeUnit_FmtStringer(t *testing.T) {
	// Verify it satisfies fmt.Stringer
	u := RuntimeUnitByte
	assert.Equal(t, "BYTE", u.String())
}

func TestParseRuntimeUnit(t *testing.T) {
	t.Run("Known values", func(t *testing.T) {
		u, err := ParseRuntimeUnit("NANO")
		require.NoError(t, err)
		assert.Equal(t, RuntimeUnitNano, u)
	})

	t.Run("Unknown value", func(t *testing.T) {
		u, err := ParseRuntimeUnit("UNKNOWN")
		assert.Error(t, err)
		assert.Equal(t, RuntimeUnitNone, u)
	})
}

func TestRuntimeUnit_MarshalText(t *testing.T) {
	u := RuntimeUnitByte
	b, err := u.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "BYTE", string(b))
}

func TestRuntimeUnit_UnmarshalText(t *testing.T) {
	var u RuntimeUnit
	err := u.UnmarshalText([]byte("NANO"))
	require.NoError(t, err)
	assert.Equal(t, RuntimeUnitNano, u)

	err = u.UnmarshalText([]byte("INVALID"))
	assert.Error(t, err)
}

func TestRuntimeUnit_JSONRoundTrip(t *testing.T) {
	type wrapper struct {
		Unit RuntimeUnit `json:"unit"`
	}

	original := wrapper{Unit: RuntimeUnitByte}
	data, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"BYTE"`)

	var decoded wrapper
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}
