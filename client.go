package presto

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Presto/Trino protocol headers
const (
	UserHeader               = "X-Presto-User"
	CatalogHeader            = "X-Presto-Catalog"
	SchemaHeader             = "X-Presto-Schema"
	SessionHeader            = "X-Presto-Session"
	TransactionHeader        = "X-Presto-Transaction-Id"
	StartedTransactionHeader = "X-Presto-Started-Transaction-Id"
	ClearTransactionHeader   = "X-Presto-Clear-Transaction-Id"
	ClientInfoHeader         = "X-Presto-Client-Info"
	ClientTagHeader          = "X-Presto-Client-Tags"
	TimeZoneHeader           = "X-Presto-Time-Zone"

	DefaultUser         = "presto-go-client"
	ContentEncodingGzip = "gzip"
	MaxRetryAttempts    = 10
	MaxRetryDelay       = 30 * time.Second
)

// RequestOption allows for functional overrides on individual requests
type RequestOption func(*http.Request)

// Session represents an isolated execution context linked to a Presto client
type Session struct {
	client        *Client // Link to the parent client for network transport
	userInfo      *url.Userinfo
	basicAuth     string
	catalog       string
	schema        string
	timezone      string
	clientInfo    string
	transactionId string
	sessionParams map[string]any
	clientTags    []string

	// mu protects session state during concurrent access
	mu sync.RWMutex
}

// Client serves as the factory and network configuration provider
type Client struct {
	Session    // Embedded default session
	httpClient *http.Client
	serverUrl  *url.URL
	isTrino    bool
	forceHTTPS bool
}

// --- Initialization & Lifecycle ---

// NewClient initializes the client and links its embedded session to itself.
// basicAuth is an optional variadic parameter.
func NewClient(serverUrl string, basicAuth ...string) (*Client, error) {
	parsedUrl, err := url.Parse(serverUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL: %w", err)
	}

	c := &Client{
		httpClient: &http.Client{},
		serverUrl:  parsedUrl,
		Session: Session{
			userInfo:      url.User(DefaultUser),
			sessionParams: make(map[string]any),
		},
	}

	// Link the embedded session to the client
	c.Session.client = c

	if len(basicAuth) > 0 {
		c.basicAuth = basicAuth[0]
	}

	return c, nil
}

// Clone creates an isolated session copy that maintains the same client link
func (s *Session) Clone() *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	params := make(map[string]any, len(s.sessionParams))
	maps.Copy(params, s.sessionParams)

	tags := make([]string, len(s.clientTags))
	copy(tags, s.clientTags)

	return &Session{
		client:        s.client, // Maintain the same network client
		userInfo:      s.userInfo,
		basicAuth:     s.basicAuth,
		catalog:       s.catalog,
		schema:        s.schema,
		timezone:      s.timezone,
		clientInfo:    s.clientInfo,
		transactionId: s.transactionId,
		sessionParams: params,
		clientTags:    tags,
	}
}

// --- Session Setters (Fluent API) ---

func (s *Session) Catalog(catalog string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catalog = catalog
	return s
}

func (s *Session) Schema(schema string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schema = schema
	return s
}

func (s *Session) User(user string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userInfo = url.User(user)
	return s
}

func (s *Session) UserPassword(user, password string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userInfo = url.UserPassword(user, password)
	return s
}

func (s *Session) TimeZone(tz string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timezone = tz
	return s
}

func (s *Session) ClientInfo(info string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientInfo = info
	return s
}

// SessionParam sets or removes a session parameter. Set value to nil to remove.
func (s *Session) SessionParam(key string, value any) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value == nil {
		delete(s.sessionParams, key)
	} else {
		s.sessionParams[key] = value
	}
	return s
}

func (s *Session) ClearSessionParams() *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionParams = make(map[string]any)
	return s
}

func (s *Session) ClientTags(tags ...string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientTags = tags
	return s
}

func (s *Session) AppendClientTag(tag string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientTags = append(s.clientTags, tag)
	return s
}

// --- Request Lifecycle (No Client argument needed) ---

// NewRequest builds an http.Request using internal session and client states, accepting optional overrides.
func (s *Session) NewRequest(method, urlStr string, body any, options ...RequestOption) (*http.Request, error) {
	u, err := s.client.prepareURL(urlStr)
	if err != nil {
		return nil, err
	}

	bodyReader, contentType, err := s.client.prepareRequestBody(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(method, u.String(), bodyReader)
	if err != nil {
		return nil, err
	}

	s.applyHeaders(req)

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept-Encoding", ContentEncodingGzip)

	// Apply functional options for specific request overrides
	for _, opt := range options {
		opt(req)
	}

	return req, nil
}

func (s *Session) applyHeaders(req *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 1. Identity & Auth
	if s.userInfo != nil {
		req.Header.Set(s.client.CanonicalHeader(UserHeader), s.userInfo.Username())
		if s.basicAuth != "" {
			req.Header.Set("Authorization", "Basic "+s.basicAuth)
		} else if pass, ok := s.userInfo.Password(); ok {
			req.SetBasicAuth(s.userInfo.Username(), pass)
		}
	}

	// 2. Contextual Headers
	if s.catalog != "" {
		req.Header.Set(s.client.CanonicalHeader(CatalogHeader), s.catalog)
	}
	if s.schema != "" {
		req.Header.Set(s.client.CanonicalHeader(SchemaHeader), s.schema)
	}
	if s.timezone != "" {
		req.Header.Set(s.client.CanonicalHeader(TimeZoneHeader), s.timezone)
	}
	if s.clientInfo != "" {
		req.Header.Set(s.client.CanonicalHeader(ClientInfoHeader), s.clientInfo)
	}

	// 3. State Headers
	if s.transactionId != "" {
		req.Header.Set(s.client.CanonicalHeader(TransactionHeader), s.transactionId)
	}
	if len(s.sessionParams) > 0 {
		req.Header.Set(s.client.CanonicalHeader(SessionHeader), s.client.generateSessionHeader(s.sessionParams))
	}
	if len(s.clientTags) > 0 {
		req.Header.Set(s.client.CanonicalHeader(ClientTagHeader), strings.Join(s.clientTags, ","))
	}
}

// --- Execution & Transaction Synchronization ---

// Do executes the request and automatically manages transaction state
func (s *Session) Do(ctx context.Context, req *http.Request, v any) (*http.Response, error) {
	req = req.WithContext(ctx)

	// Buffer the request body so it can be replayed on retries.
	// io.Reader is consumed after the first attempt, so we need GetBody.
	if req.Body != nil && req.GetBody == nil {
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(bodyBytes)), nil
		}
	}

	retryDelay := time.Second
	for attempt := 0; attempt < MaxRetryAttempts; attempt++ {
		resp, err := s.client.httpClient.Do(req)
		if err != nil {
			// Retry on transient network errors, but not on context cancellation
			if !isRetryableNetError(err) {
				return nil, err
			}

			log.Debug().Err(err).Int("attempt", attempt+1).Msg("retrying on connection error")

			if req.GetBody != nil {
				req.Body, _ = req.GetBody()
			}

			time.Sleep(retryDelay)
			retryDelay *= 2
			if retryDelay > MaxRetryDelay {
				retryDelay = MaxRetryDelay
			}
			continue
		}

		s.updateTransactionState(resp)

		if resp.StatusCode == http.StatusOK {
			err = s.client.decodeResponseBody(resp, v)
			return resp, err
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			if closeErr := resp.Body.Close(); closeErr != nil {
				log.Debug().Err(closeErr).Msg("failed to close response body")
			}

			// Reset the request body for the next attempt
			if req.GetBody != nil {
				req.Body, _ = req.GetBody()
			}

			time.Sleep(retryDelay)
			retryDelay *= 2
			if retryDelay > MaxRetryDelay {
				retryDelay = MaxRetryDelay
			}
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, fmt.Errorf("presto server error: %d: %s", resp.StatusCode, string(body))
	}
	return nil, fmt.Errorf("max retries exceeded")
}

// isRetryableNetError returns true for transient network errors that warrant
// a retry (connection refused, DNS failures, connection reset, network timeouts).
// Context cancellation and deadline exceeded errors are NOT retried.
func isRetryableNetError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

func (s *Session) updateTransactionState(resp *http.Response) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id := resp.Header.Get(s.client.CanonicalHeader(StartedTransactionHeader)); id != "" {
		s.transactionId = id
	} else if resp.Header.Get(s.client.CanonicalHeader(ClearTransactionHeader)) == "true" {
		s.transactionId = ""
	}
}

// --- Client Configuration & Delegation ---

func (c *Client) IsTrino(isTrino bool) *Client {
	c.isTrino = isTrino
	return c
}

func (c *Client) ForceHTTPS(force bool) *Client {
	c.forceHTTPS = force
	return c
}

// --- Client Networking Utilities ---

func (c *Client) prepareURL(urlStr string) (*url.URL, error) {
	u, err := c.serverUrl.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if c.forceHTTPS && u.Scheme == "http" {
		u.Scheme = "https"
	}
	return u, nil
}

func (c *Client) prepareRequestBody(body any) (io.Reader, string, error) {
	if body == nil {
		return nil, "", nil
	}
	if s, ok := body.(string); ok {
		return strings.NewReader(s), "text/plain", nil
	}
	jsonBuf := &bytes.Buffer{}
	if err := json.NewEncoder(jsonBuf).Encode(body); err != nil {
		return nil, "", err
	}
	return jsonBuf, "application/json", nil
}

// CanonicalHeader transforms a Presto-style header key (starting with "X-Presto-")
// into its Trino equivalent ("X-Trino-") if the client is configured in Trino mode.
// This ensures compatibility across different versions of the Presto/Trino
// ecosystem while allowing the internal code to use a single, consistent
// naming convention.
func (c *Client) CanonicalHeader(name string) string {
	if c.isTrino {
		return strings.Replace(name, "X-Presto", "X-Trino", 1)
	}
	return name
}

func (c *Client) generateSessionHeader(params map[string]any) string {
	var pairs []string
	for k, v := range params {
		val := url.QueryEscape(fmt.Sprintf("%v", v))
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, val))
	}
	return strings.Join(pairs, ",")
}

func (c *Client) decodeResponseBody(resp *http.Response, v any) (err error) {
	// Ensure the main response body is always closed
	defer func() {
		closeErr := resp.Body.Close()
		if err == nil {
			err = closeErr
		}
	}()

	// 1. Early return if no destination is provided
	if v == nil {
		return nil
	}

	var reader io.Reader = resp.Body

	// 2. Handle decompression
	if resp.Header.Get("Content-Encoding") == ContentEncodingGzip {
		gz, gzErr := gzip.NewReader(resp.Body)
		if gzErr != nil {
			return fmt.Errorf("failed to create gzip reader: %w", gzErr)
		}

		// Use a closure to handle gz.Close() error
		defer func() {
			cErr := gz.Close()
			if cErr != nil {
				// We log it instead of returning it to avoid
				// overwriting primary decoding errors.
				log.Debug().Err(cErr).Msg("failed to close gzip reader")
			}
		}()
		reader = gz
	}

	// 3. Decode payload
	if w, ok := v.(io.Writer); ok {
		_, err = io.Copy(w, reader)
		return err
	}

	if err = json.NewDecoder(reader).Decode(v); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

// NewSession creates a new, isolated session using the client's current
// connection settings. The new session is linked to this client but
// maintains its own headers, transaction state, and tags.
func (c *Client) NewSession() *Session {
	// We call Clone on the embedded default session to ensure we pick up
	// any default settings (like basicAuth or User) already configured on the client.
	return c.Session.Clone()
}
