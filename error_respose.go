package presto

import (
	"fmt"
	"io"
	"net/http"
)

// ErrorResponse represents an HTTP error response from the Presto server.
// It wraps the HTTP response and provides access to the error message.
type ErrorResponse struct {
	// Response is the original HTTP response
	Response *http.Response

	// Message is the error message from the response body
	Message string
}

// Error implements the error interface for ErrorResponse.
// It returns a formatted string with the message and status code.
func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("%s (status code: %d)", e.Message, e.Response.StatusCode)
}

// NewErrorResponse creates a new ErrorResponse from an HTTP response.
// It reads the response body and closes it.
//
// Parameters:
//   - resp: The HTTP response containing an error
//
// Returns:
//   - An ErrorResponse wrapping the HTTP response
//   - An error if reading the response body fails
func NewErrorResponse(resp *http.Response) error {
	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return &ErrorResponse{
		Response: resp,
		Message:  string(bytes),
	}
}
