package presto

import (
	"fmt"
)

// QueryError represents an error that occurred during query execution on the Presto server.
// It contains detailed information about the error, including its type, location, and cause.
type QueryError struct {
	// Message is the human-readable error message
	Message string `json:"message"`

	// ErrorCode is a numeric code identifying the error type
	ErrorCode int `json:"errorCode"`

	// ErrorName is a string identifier for the error type
	ErrorName string `json:"errorName"`

	// ErrorType categorizes the error (e.g., "USER_ERROR", "INTERNAL_ERROR")
	ErrorType string `json:"errorType"`

	// Retriable indicates whether the query can be retried
	Retriable bool `json:"retriable"`

	// ErrorLocation contains line and column information for syntax errors
	ErrorLocation *ErrorLocation `json:"errorLocation,omitempty"`

	// FailureInfo contains detailed information about the failure
	FailureInfo *FailureInfo `json:"failureInfo,omitempty"`
}

// String returns a formatted string representation of the QueryError.
// The format is "ErrorName: Message".
func (q *QueryError) String() string {
	if q == nil {
		return "nil QueryError"
	}
	return fmt.Sprintf("%s: %s", q.ErrorName, q.Message)
}

// Error implements the error interface for QueryError.
// It returns the same string as String().
func (q *QueryError) Error() string {
	return q.String()
}

// ErrorLocation represents the position in a SQL query where an error occurred.
// This is typically used for syntax errors.
type ErrorLocation struct {
	// LineNumber is the 1-based line number in the SQL query
	LineNumber int `json:"lineNumber"`

	// ColumnNumber is the 1-based column number in the SQL query
	ColumnNumber int `json:"columnNumber"`
}

// String returns a formatted string representation of the ErrorLocation.
// The format is "line LineNumber:ColumnNumber".
func (e *ErrorLocation) String() string {
	return fmt.Sprintf("line %d:%d", e.LineNumber, e.ColumnNumber)
}

// FailureInfo contains detailed information about a query failure.
// It can include a stack trace and nested causes.
type FailureInfo struct {
	// Type is the Java class name of the exception
	Type string `json:"type"`

	// Message is the exception message
	Message string `json:"message,omitempty"`

	// Cause is the nested exception that caused this failure
	Cause *FailureInfo `json:"cause,omitempty"`

	// Suppressed contains any suppressed exceptions
	Suppressed []FailureInfo `json:"suppressed"`

	// Stack contains the stack trace elements
	Stack []string `json:"stack"`

	// ErrorLocation contains line and column information for syntax errors
	ErrorLocation *ErrorLocation `json:"errorLocation,omitempty"`
}
