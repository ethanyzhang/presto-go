package presto

// ClientTypeSignature contains detailed information about a Presto/Trino data type.
// This is a simplified version that only includes the raw type name.
// A full implementation would include type arguments, literal arguments, etc.
type ClientTypeSignature struct {
	// RawType is the base type name (e.g., "varchar", "bigint", "array")
	RawType string `json:"rawType"`
	// typeArguments
	// literalArguments
	// arguments
}
