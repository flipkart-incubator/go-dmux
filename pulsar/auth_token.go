package pulsar

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
)

const (
	// defaultSubjectTokenField is the form field name used to carry the subject
	// token when auth_subject_token_field is not supplied via config.
	defaultSubjectTokenField = "subject_token"
	// tokenRefreshSkew is how long before the real expiry we mark tokens as
	// expired, so the cached source refreshes proactively.
	tokenRefreshSkew = 60 * time.Second
	// authHTTPTimeout bounds the token-exchange HTTP call.
	authHTTPTimeout = 10 * time.Second
)

// authTokenProvider exchanges a native identity token for a downstream access
// token using a provider-agnostic token-exchange flow. It implements
// oauth2.TokenSource; caching is handled by oauth2.ReuseTokenSource in
// tokenSupplier.
type authTokenProvider struct {
	tokenEndpoint     string
	exchangeForm      map[string]string
	subjectTokenPath  string
	subjectTokenField string
	httpClient        *http.Client
}

// tokenExchangeResponse models the subset of the token-exchange response
// we consume.
type tokenExchangeResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// newAuthTokenProvider builds a provider from the source config. For
// machine_identity, auth_issuer_url is the full HTTPS token-exchange endpoint.
// Only https endpoints are allowed to avoid sending credentials in plaintext.
func newAuthTokenProvider(conf PulsarConf) (*authTokenProvider, error) {
	if conf.AuthIssuerURL == "" {
		return nil, fmt.Errorf("machine_identity: auth_issuer_url is required")
	}

	endpoint, err := url.Parse(conf.AuthIssuerURL)
	if err != nil {
		return nil, fmt.Errorf("machine_identity: invalid auth_issuer_url: %w", err)
	}
	if endpoint.Scheme != "https" {
		return nil, fmt.Errorf("machine_identity: auth_issuer_url must be https")
	}
	if endpoint.Host == "" {
		return nil, fmt.Errorf("machine_identity: auth_issuer_url missing host")
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

	return &authTokenProvider{
		tokenEndpoint:     conf.AuthIssuerURL,
		exchangeForm:      conf.AuthExchangeForm,
		subjectTokenPath:  conf.AuthSubjectTokenPath,
		subjectTokenField: subjectTokenField,
		httpClient:        &http.Client{Timeout: authHTTPTimeout},
	}, nil
}

// Token implements oauth2.TokenSource. It performs a token exchange on every
// call; callers should wrap this provider with oauth2.ReuseTokenSource, which
// caches until the token expires. We report Expiry tokenRefreshSkew earlier
// than the real expiry so the cached source refreshes proactively.
func (p *authTokenProvider) Token() (*oauth2.Token, error) {
	access, ttl, err := p.fetchToken()
	if err != nil {
		return nil, err
	}
	if ttl > tokenRefreshSkew {
		ttl -= tokenRefreshSkew
	}
	return &oauth2.Token{
		AccessToken: access,
		Expiry:      time.Now().Add(ttl),
	}, nil
}

// tokenSupplier returns a function compatible with
// pulsar.NewAuthenticationTokenFromSupplier. Caching and refresh are delegated
// to oauth2.ReuseTokenSource; the proactive refresh skew is baked into the
// token Expiry in Token.
func (p *authTokenProvider) tokenSupplier() func() (string, error) {
	cached := oauth2.ReuseTokenSource(nil, p)
	return func() (string, error) {
		tok, err := cached.Token()
		if err != nil {
			return "", err
		}
		return tok.AccessToken, nil
	}
}

// fetchToken reads the native identity token and exchanges it for a downstream
// token. The subject token is re-read on every fetch because it rotates
// (~hourly) independently of the exchanged token.
func (p *authTokenProvider) fetchToken() (string, time.Duration, error) {
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

	ctx, cancel := context.WithTimeout(context.Background(), authHTTPTimeout)
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
	return parsed.AccessToken, ttl, nil
}

func (p *authTokenProvider) readSubjectToken() (string, error) {
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
