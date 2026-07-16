package pulsar

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// writeSubjectToken writes contents to a temp file and returns its path.
func writeSubjectToken(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := ioutil.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatalf("failed to write subject token file: %v", err)
	}
	return path
}

// testGrantType is the token-exchange grant used in test form bodies.
const testGrantType = "urn:ietf:params:oauth:grant-type:token-exchange"

// testTokenPath is an arbitrary token-endpoint path used by the test servers.
const testTokenPath = "/v3/oauth/token"

// newTestProvider builds a provider pointed at the given endpoint and subject
// token path, bypassing newAuthnTokenProvider so individual fields can be set.
func newTestProvider(endpoint, subjectTokenPath string) *authnTokenProvider {
	return &authnTokenProvider{
		tokenEndpoint: endpoint,
		exchangeForm: map[string]string{
			"grant_type":       testGrantType,
			"client_id":        "test-client",
			"target_client_id": "test-target",
		},
		subjectTokenPath:  subjectTokenPath,
		subjectTokenField: defaultSubjectTokenField,
		httpClient:        &http.Client{Timeout: authnHTTPTimeout},
	}
}

func baseMachineIdentityConf() PulsarConf {
	return PulsarConf{
		AuthMode:             AuthModeMachineIdentity,
		AuthTokenEndpoint:    "https://authn.example.com/v3/oauth/token",
		AuthSubjectTokenPath: "/some/path/token",
		AuthExchangeForm: map[string]string{
			"grant_type":       testGrantType,
			"client_id":        "cid",
			"target_client_id": "tid",
		},
	}
}

func TestNewAuthnTokenProvider_Validation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(c *PulsarConf)
		wantErr bool
	}{
		{"valid config", func(c *PulsarConf) {}, false},
		{"missing token endpoint", func(c *PulsarConf) { c.AuthTokenEndpoint = "" }, true},
		{"non-https endpoint", func(c *PulsarConf) { c.AuthTokenEndpoint = "http://insecure.example.com/token" }, true},
		{"malformed endpoint", func(c *PulsarConf) { c.AuthTokenEndpoint = "://bad" }, true},
		{"endpoint without host", func(c *PulsarConf) { c.AuthTokenEndpoint = "https://" }, true},
		{"missing subject path", func(c *PulsarConf) { c.AuthSubjectTokenPath = "" }, true},
		{"empty exchange form", func(c *PulsarConf) { c.AuthExchangeForm = nil }, true},
		{"custom subject token field", func(c *PulsarConf) { c.AuthSubjectTokenField = "assertion" }, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := baseMachineIdentityConf()
			tc.mutate(&conf)
			p, err := newAuthnTokenProvider(conf)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (provider=%v)", p)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// Endpoint is used verbatim, no derivation.
			if p.tokenEndpoint != conf.AuthTokenEndpoint {
				t.Errorf("expected token endpoint %q, got %q", conf.AuthTokenEndpoint, p.tokenEndpoint)
			}
			// Subject token field defaults when unset, else uses the configured value.
			wantField := conf.AuthSubjectTokenField
			if wantField == "" {
				wantField = defaultSubjectTokenField
			}
			if p.subjectTokenField != wantField {
				t.Errorf("expected subject token field %q, got %q", wantField, p.subjectTokenField)
			}
		})
	}
}

func TestFetchToken_Success(t *testing.T) {
	subjectToken := "k8s-subject-token"
	subjPath := writeSubjectToken(t, subjectToken+"\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != testTokenPath {
			t.Errorf("expected path %s, got %s", testTokenPath, r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/x-www-form-urlencoded" {
			t.Errorf("expected form content-type, got %q", ct)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("failed to parse form: %v", err)
			return
		}
		if got := r.PostForm.Get("grant_type"); got != testGrantType {
			t.Errorf("grant_type = %q, want %q", got, testGrantType)
		}
		if got := r.PostForm.Get("client_id"); got != "test-client" {
			t.Errorf("client_id = %q, want test-client", got)
		}
		if got := r.PostForm.Get("target_client_id"); got != "test-target" {
			t.Errorf("target_client_id = %q, want test-target", got)
		}
		if got := r.PostForm.Get("subject_token"); got != subjectToken {
			t.Errorf("subject_token = %q, want %q (should be trimmed)", got, subjectToken)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token":"issued-token","expires_in":3600,"token_type":"bearer"}`))
	}))
	defer srv.Close()

	p := newTestProvider(srv.URL+testTokenPath, subjPath)
	token, ttl, err := p.fetchToken()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "issued-token" {
		t.Errorf("token = %q, want issued-token", token)
	}
	if ttl != 3600*time.Second {
		t.Errorf("ttl = %v, want 1h", ttl)
	}
}

func TestFetchToken_ErrorPaths(t *testing.T) {
	validSubj := writeSubjectToken(t, "subject")

	cases := []struct {
		name       string
		handler    http.HandlerFunc
		subjPath   string
		wantErrSub string
	}{
		{
			name: "non-200 status",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte("secret-leak-should-not-surface"))
			},
			subjPath:   validSubj,
			wantErrSub: "status 401",
		},
		{
			name: "empty access_token",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`{"access_token":"","expires_in":3600}`))
			},
			subjPath:   validSubj,
			wantErrSub: "empty access_token",
		},
		{
			name: "zero expires_in",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`{"access_token":"x","expires_in":0}`))
			},
			subjPath:   validSubj,
			wantErrSub: "expires_in",
		},
		{
			name: "malformed json",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`not-json`))
			},
			subjPath:   validSubj,
			wantErrSub: "parse token response",
		},
		{
			name:       "subject token missing",
			handler:    func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) },
			subjPath:   filepath.Join(t.TempDir(), "does-not-exist"),
			wantErrSub: "read subject token",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()

			p := newTestProvider(srv.URL+testTokenPath, tc.subjPath)
			_, _, err := p.fetchToken()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrSub)
			}
			if tc.name == "non-200 status" && strings.Contains(err.Error(), "secret-leak") {
				t.Errorf("error leaked response body: %q", err.Error())
			}
		})
	}
}

func TestGetToken_Caching(t *testing.T) {
	subjPath := writeSubjectToken(t, "subject")
	var hits int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer srv.Close()

	p := newTestProvider(srv.URL+testTokenPath, subjPath)

	if _, err := p.getToken(); err != nil {
		t.Fatalf("first getToken failed: %v", err)
	}
	if _, err := p.getToken(); err != nil {
		t.Fatalf("second getToken failed: %v", err)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Errorf("expected 1 server hit (second served from cache), got %d", got)
	}

	// Force expiry so the next call must refresh.
	p.mu.Lock()
	p.expiresAt = time.Now().Add(-time.Minute)
	p.mu.Unlock()

	if _, err := p.getToken(); err != nil {
		t.Fatalf("third getToken failed: %v", err)
	}
	if got := atomic.LoadInt32(&hits); got != 2 {
		t.Errorf("expected 2 server hits after forced expiry, got %d", got)
	}
}

func TestGetToken_RefreshSkew(t *testing.T) {
	subjPath := writeSubjectToken(t, "subject")
	var hits int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer srv.Close()

	p := newTestProvider(srv.URL+testTokenPath, subjPath)

	// Seed a token whose expiry falls inside the refresh skew window, so it is
	// treated as stale even though it has not strictly expired.
	p.mu.Lock()
	p.cachedToken = "stale"
	p.expiresAt = time.Now().Add(tokenRefreshSkew / 2)
	p.mu.Unlock()

	tok, err := p.getToken()
	if err != nil {
		t.Fatalf("getToken failed: %v", err)
	}
	if tok != "tok" {
		t.Errorf("expected refreshed token %q, got %q", "tok", tok)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Errorf("expected 1 refresh within skew window, got %d", got)
	}
}

func TestBuildAuthentication_ModeSelection(t *testing.T) {
	t.Run("machine_identity valid", func(t *testing.T) {
		src := GetPulsarSource(baseMachineIdentityConf())
		auth, err := src.buildAuthentication()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if auth == nil {
			t.Error("expected non-nil authentication")
		}
	})

	t.Run("machine_identity invalid config", func(t *testing.T) {
		conf := baseMachineIdentityConf()
		conf.AuthTokenEndpoint = "http://insecure.example.com/token"
		src := GetPulsarSource(conf)
		if _, err := src.buildAuthentication(); err == nil {
			t.Error("expected error for invalid machine_identity config, got nil")
		}
	})

	t.Run("legacy empty auth mode", func(t *testing.T) {
		// Empty auth mode must take the legacy client-credentials branch. That
		// branch never returns an error (unlike machine_identity, which would
		// error here on the invalid config), so a nil error confirms the correct
		// branch was selected. We do not assert on the returned Authentication
		// value because the SDK's NewAuthenticationOAuth2 may return nil for
		// placeholder credentials.
		src := GetPulsarSource(PulsarConf{
			AuthClientId:     "cid",
			AuthClientSecret: "secret",
			AuthIssuerURL:    "https://authn.example.com/v3/fk",
			AuthAudience:     "viesti-prod",
		})
		if _, err := src.buildAuthentication(); err != nil {
			t.Fatalf("unexpected error on legacy path: %v", err)
		}
	})
}
