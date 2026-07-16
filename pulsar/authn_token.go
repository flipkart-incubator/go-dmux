package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// defaultSubjectTokenField is the form field name used to carry the subject
	// token when auth_subject_token_field is not supplied via config.
	defaultSubjectTokenField = "subject_token"
	// tokenRefreshSkew is how long before expiry we proactively refresh.
	tokenRefreshSkew = 60 * time.Second
	// authnHTTPTimeout bounds the token-exchange HTTP call.
	authnHTTPTimeout = 10 * time.Second
)

// authnTokenProvider fetches and caches short-lived tokens using a
// provider-agnostic token-exchange flow. It uses no client secret: the subject
// token is the workload's native identity token read from disk, and the rest of
// the request body is supplied verbatim from config (exchangeForm).
type authnTokenProvider struct {
	tokenEndpoint     string
	exchangeForm      map[string]string
	subjectTokenPath  string
	subjectTokenField string
	httpClient        *http.Client

	mu          sync.Mutex
	cachedToken string
	expiresAt   time.Time
}

// tokenExchangeResponse models the subset of the AuthN token-exchange response
// we consume.
type tokenExchangeResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// newAuthnTokenProvider builds a provider from the source config. The token
// endpoint is used verbatim from config; outbound requests go only to that URL.
// Only https endpoints are allowed to avoid sending credentials in plaintext.
func newAuthnTokenProvider(conf PulsarConf) (*authnTokenProvider, error) {
	if conf.AuthTokenEndpoint == "" {
		return nil, fmt.Errorf("machine_identity: auth_token_endpoint is required")
	}

	endpoint, err := url.Parse(conf.AuthTokenEndpoint)
	if err != nil {
		return nil, fmt.Errorf("machine_identity: invalid auth_token_endpoint: %w", err)
	}
	if endpoint.Scheme != "https" {
		return nil, fmt.Errorf("machine_identity: auth_token_endpoint must be https")
	}
	if endpoint.Host == "" {
		return nil, fmt.Errorf("machine_identity: auth_token_endpoint missing host")
	}

	if conf.AuthSubjectTokenPath == "" {
		return nil, fmt.Errorf("machine_identity: auth_subject_token_path is required")
	}
	if len(conf.AuthExchangeForm) == 0 {
		return nil, fmt.Errorf("machine_identity: auth_exchange_form is required")
	}

	subjectTokenField := conf.AuthSubjectTokenField
	if subjectTokenField == "" {
		subjectTokenField = defaultSubjectTokenField
	}

	return &authnTokenProvider{
		tokenEndpoint:     conf.AuthTokenEndpoint,
		exchangeForm:      conf.AuthExchangeForm,
		subjectTokenPath:  conf.AuthSubjectTokenPath,
		subjectTokenField: subjectTokenField,
		httpClient:        &http.Client{Timeout: authnHTTPTimeout},
	}, nil
}

// tokenSupplier returns a function compatible with
// pulsar.NewAuthenticationTokenFromSupplier. It serves a cached token and
// refreshes it shortly before expiry.
func (p *authnTokenProvider) tokenSupplier() func() (string, error) {
	return func() (string, error) {
		return p.getToken()
	}
}

func (p *authnTokenProvider) getToken() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cachedToken != "" && time.Now().Before(p.expiresAt.Add(-tokenRefreshSkew)) {
		return p.cachedToken, nil
	}

	token, ttl, err := p.fetchToken()
	if err != nil {
		return "", err
	}
	p.cachedToken = token
	p.expiresAt = time.Now().Add(ttl)
	return token, nil
}

// fetchToken reads the native identity token and exchanges it for an AuthN
// token. The subject token is re-read on every fetch because it rotates
// (~hourly) independently of the AuthN token.
func (p *authnTokenProvider) fetchToken() (string, time.Duration, error) {
	subjectToken, err := p.readSubjectToken()
	if err != nil {
		return "", 0, err
	}

	// Build the request body from the config-supplied form, then inject the
	// runtime subject token under the configured field name.
	form := url.Values{}
	for k, v := range p.exchangeForm {
		form.Set(k, v)
	}
	form.Set(p.subjectTokenField, subjectToken)

	ctx, cancel := context.WithTimeout(context.Background(), authnHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenEndpoint,
		strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("machine_identity: build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("machine_identity: token exchange request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("machine_identity: read token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Do not log the response body verbatim; it may echo request material.
		return "", 0, fmt.Errorf("machine_identity: token exchange returned status %d", resp.StatusCode)
	}

	var parsed tokenExchangeResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", 0, fmt.Errorf("machine_identity: parse token response: %w", err)
	}
	if parsed.AccessToken == "" {
		return "", 0, fmt.Errorf("machine_identity: empty access_token in response")
	}
	if parsed.ExpiresIn <= 0 {
		return "", 0, fmt.Errorf("machine_identity: missing or invalid expires_in in response")
	}

	ttl := time.Duration(parsed.ExpiresIn) * time.Second
	log.Printf("machine_identity: fetched new token from %s (ttl ~%s)\n",
		p.tokenEndpoint, ttl)
	return parsed.AccessToken, ttl, nil
}

func (p *authnTokenProvider) readSubjectToken() (string, error) {
	b, err := ioutil.ReadFile(p.subjectTokenPath)
	if err != nil {
		return "", fmt.Errorf("machine_identity: read subject token at %s: %w",
			p.subjectTokenPath, err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("machine_identity: subject token empty at %s", p.subjectTokenPath)
	}
	return token, nil
}
