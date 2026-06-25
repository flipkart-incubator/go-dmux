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
	// authnTokenExchangePath is the AuthN token-exchange endpoint path. This is
	// an AuthN protocol convention (not a deployment-specific value); the host
	// is derived from the configured issuer URL.
	authnTokenExchangePath = "/v3/oauth/token"
	// tokenExchangeGrantType is the standard RFC 8693 token-exchange grant.
	tokenExchangeGrantType = "urn:ietf:params:oauth:grant-type:token-exchange"
	// defaultSubjectTokenPath is the well-known mount path for the projected
	// Kubernetes identity token injected by the AuthN webhook. Used only as a
	// fallback when auth_subject_token_path is not supplied via config.
	defaultSubjectTokenPath = "/var/run/secrets/wif/k8s-authn/token"
	// tokenRefreshSkew is how long before expiry we proactively refresh.
	tokenRefreshSkew = 60 * time.Second
	// authnHTTPTimeout bounds the token-exchange HTTP call.
	authnHTTPTimeout = 10 * time.Second
)

// authnTokenProvider fetches and caches short-lived AuthN tokens using the
// Machine Identity (token-exchange) flow. It implements no client secret: the
// subject token is the pod's native identity token read from disk.
type authnTokenProvider struct {
	tokenEndpoint    string
	clientID         string
	targetClientID   string
	subjectTokenPath string
	httpClient       *http.Client

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
// endpoint host is derived from the configured issuer URL, so outbound calls
// are restricted to that host (SSRF-safe); only https/http issuers are allowed.
func newAuthnTokenProvider(conf PulsarConf) (*authnTokenProvider, error) {
	if conf.AuthClientId == "" {
		return nil, fmt.Errorf("machine_identity: client_id is required")
	}
	if conf.AuthTargetClientId == "" {
		return nil, fmt.Errorf("machine_identity: auth_target_client_id is required")
	}
	if conf.AuthIssuerURL == "" {
		return nil, fmt.Errorf("machine_identity: auth_issuer_url is required")
	}

	issuer, err := url.Parse(conf.AuthIssuerURL)
	if err != nil {
		return nil, fmt.Errorf("machine_identity: invalid auth_issuer_url: %w", err)
	}
	if issuer.Scheme != "https" && issuer.Scheme != "http" {
		return nil, fmt.Errorf("machine_identity: auth_issuer_url must be http(s)")
	}
	if issuer.Host == "" {
		return nil, fmt.Errorf("machine_identity: auth_issuer_url missing host")
	}

	// Derive the token endpoint from the issuer's scheme+host plus the AuthN
	// standard path. This pins all outbound requests to the issuer host.
	tokenEndpoint := (&url.URL{
		Scheme: issuer.Scheme,
		Host:   issuer.Host,
		Path:   authnTokenExchangePath,
	}).String()

	subjectTokenPath := conf.AuthSubjectTokenPath
	if subjectTokenPath == "" {
		subjectTokenPath = defaultSubjectTokenPath
	}

	return &authnTokenProvider{
		tokenEndpoint:    tokenEndpoint,
		clientID:         conf.AuthClientId,
		targetClientID:   conf.AuthTargetClientId,
		subjectTokenPath: subjectTokenPath,
		httpClient:       &http.Client{Timeout: authnHTTPTimeout},
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

	form := url.Values{}
	form.Set("grant_type", tokenExchangeGrantType)
	form.Set("client_id", p.clientID)
	form.Set("target_client_id", p.targetClientID)
	form.Set("subject_token", subjectToken)

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
	log.Printf("machine_identity: fetched new AuthN token for client %s (ttl ~%s)\n",
		p.clientID, ttl)
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
