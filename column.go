package presto

// Column represents metadata about a column in a query result.
type Column struct {
	// Name is the column name
	Name string `json:"name"`

	// Type is the Presto/Trino data type as a string
	Type string `json:"type"`

	// TypeSignature contains detailed type information
	TypeSignature ClientTypeSignature `json:"typeSignature"`
}
