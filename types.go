package presto

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// NullSlice is a nullable JSON array that implements sql.Scanner and driver.Valuer.
// Use it to scan Presto ARRAY columns into Go slices.
//
//	var names NullSlice[string]
//	err := row.Scan(&names)
type NullSlice[T any] struct {
	Slice []T
	Valid bool // Valid is true if the value is not NULL
}

var _ sql.Scanner = (*NullSlice[any])(nil)
var _ driver.Valuer = (*NullSlice[any])(nil)

// Scan implements sql.Scanner. It expects a JSON string or []byte from the driver.
func (s *NullSlice[T]) Scan(src any) error {
	if src == nil {
		s.Slice = nil
		s.Valid = false
		return nil
	}

	var data []byte
	switch v := src.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return fmt.Errorf("presto: cannot scan %T into NullSlice", src)
	}

	if err := json.Unmarshal(data, &s.Slice); err != nil {
		return fmt.Errorf("presto: cannot unmarshal array: %w", err)
	}
	s.Valid = true
	return nil
}

// Value implements driver.Valuer.
func (s NullSlice[T]) Value() (driver.Value, error) {
	if !s.Valid {
		return nil, nil
	}
	b, err := json.Marshal(s.Slice)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// NullMap is a nullable JSON object that implements sql.Scanner and driver.Valuer.
// Use it to scan Presto MAP columns into Go maps.
//
//	var props NullMap[string, int]
//	err := row.Scan(&props)
type NullMap[K comparable, V any] struct {
	Map   map[K]V
	Valid bool // Valid is true if the value is not NULL
}

var _ sql.Scanner = (*NullMap[string, any])(nil)
var _ driver.Valuer = (*NullMap[string, any])(nil)

// Scan implements sql.Scanner. It expects a JSON string or []byte from the driver.
func (m *NullMap[K, V]) Scan(src any) error {
	if src == nil {
		m.Map = nil
		m.Valid = false
		return nil
	}

	var data []byte
	switch v := src.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return fmt.Errorf("presto: cannot scan %T into NullMap", src)
	}

	if err := json.Unmarshal(data, &m.Map); err != nil {
		return fmt.Errorf("presto: cannot unmarshal map: %w", err)
	}
	m.Valid = true
	return nil
}

// Value implements driver.Valuer.
func (m NullMap[K, V]) Value() (driver.Value, error) {
	if !m.Valid {
		return nil, nil
	}
	b, err := json.Marshal(m.Map)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// NullRow is a nullable JSON object that implements sql.Scanner and driver.Valuer.
// Use it to scan Presto ROW columns into Go structs or maps.
//
// Scan into a struct:
//
//	type Address struct {
//	    Street string `json:"street"`
//	    City   string `json:"city"`
//	}
//	var addr NullRow[Address]
//	err := row.Scan(&addr)
//
// Scan into a map:
//
//	var addr NullRow[map[string]any]
//	err := row.Scan(&addr)
type NullRow[T any] struct {
	Row   T
	Valid bool // Valid is true if the value is not NULL
}

var _ sql.Scanner = (*NullRow[any])(nil)
var _ driver.Valuer = (*NullRow[any])(nil)

// Scan implements sql.Scanner. It expects a JSON string or []byte from the driver.
func (r *NullRow[T]) Scan(src any) error {
	if src == nil {
		var zero T
		r.Row = zero
		r.Valid = false
		return nil
	}

	var data []byte
	switch v := src.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return fmt.Errorf("presto: cannot scan %T into NullRow", src)
	}

	if err := json.Unmarshal(data, &r.Row); err != nil {
		return fmt.Errorf("presto: cannot unmarshal row: %w", err)
	}
	r.Valid = true
	return nil
}

// Value implements driver.Valuer.
func (r NullRow[T]) Value() (driver.Value, error) {
	if !r.Valid {
		return nil, nil
	}
	b, err := json.Marshal(r.Row)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}
