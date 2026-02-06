package presto

import (
	"fmt"
	"presto-go/utils"
	"strconv"
)

// RuntimeUnit represents the unit of measurement for a runtime metric.
type RuntimeUnit int8

const (
	// RuntimeUnitNone represents no specific unit
	RuntimeUnitNone RuntimeUnit = iota
	// RuntimeUnitNano represents nanoseconds
	RuntimeUnitNano
	// RuntimeUnitByte represents bytes
	RuntimeUnitByte
)

var runtimeUnitMap = utils.NewBiMap(map[RuntimeUnit]string{
	RuntimeUnitNone: "NONE",
	RuntimeUnitNano: "NANO",
	RuntimeUnitByte: "BYTE",
})

// String returns the string representation of the RuntimeUnit.
func (u *RuntimeUnit) String() (string, error) {
	if value, ok := runtimeUnitMap.Lookup(*u); ok {
		return value, nil
	}
	return strconv.Itoa(int(*u)), fmt.Errorf("unknown RuntimeUnit %d", int(*u))
}

// ParseRuntimeUnit parses a string into a RuntimeUnit.
// If the string is unknown, it defaults to RuntimeUnitNone and returns an error.
func ParseRuntimeUnit(str string) (RuntimeUnit, error) {
	if key, ok := runtimeUnitMap.RLookup(str); ok {
		return key, nil
	}
	return RuntimeUnit(0), fmt.Errorf("unknown RuntimeUnit string %s, defaulting to %s",
		str, runtimeUnitMap.DirectLookup(RuntimeUnit(0)))
}

// MarshalText implements the encoding.TextMarshaler interface.
func (u *RuntimeUnit) MarshalText() (text []byte, err error) {
	str, err := u.String()
	return []byte(str), err
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (u *RuntimeUnit) UnmarshalText(text []byte) error {
	var err error
	*u, err = ParseRuntimeUnit(string(text))
	return err
}
