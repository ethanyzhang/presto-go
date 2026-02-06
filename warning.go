package presto

// Warning represents a warning generated during query execution.
type Warning struct {
	WarningCode WarningCode `json:"warningCode"`
	Message     string      `json:"message"`
}

// WarningCode represents the code and name of a warning.
type WarningCode struct {
	Code int    `json:"code"`
	Name string `json:"name"`
}
