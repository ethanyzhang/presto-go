package presto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryError_String(t *testing.T) {
	qe := &QueryError{
		ErrorName: "SYNTAX_ERROR",
		Message:   "line 1:8: Column 'foo' cannot be resolved",
	}
	assert.Equal(t, "SYNTAX_ERROR: line 1:8: Column 'foo' cannot be resolved", qe.String())
}

func TestQueryError_NilString(t *testing.T) {
	var qe *QueryError
	assert.Equal(t, "nil QueryError", qe.String())
}

func TestQueryError_Error(t *testing.T) {
	qe := &QueryError{
		ErrorName: "USER_ERROR",
		Message:   "table not found",
	}
	// Error() delegates to String()
	assert.Equal(t, qe.String(), qe.Error())

	// Verify it satisfies the error interface
	var err error = qe
	assert.Contains(t, err.Error(), "USER_ERROR")
}

func TestErrorLocation_String(t *testing.T) {
	loc := &ErrorLocation{LineNumber: 3, ColumnNumber: 15}
	assert.Equal(t, "line 3:15", loc.String())
}
