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

// newTestProvider builds a provider pointed at the given endpoint and subject
// token path, bypassing newAuthnTokenProvider so individual fields can be set.
func newTestProvider(endpoint, subjectTokenPath string) *authnTokenProvider {
	return &authnTokenProvider{
		tokenEndpoint:    endpoint,
		clientID:         "test-client",
		targetClientID:   "test-target",
		subjectTokenPath: subjectTokenPath,
		httpClient:       &http.Client{Timeout: authnHTTPTimeout},
	}
}

func TestNewAuthnTokenProvider_Validation(t *testing.T) {
	base := PulsarConf{
		AuthMode:             AuthModeMachineIdentity,
		AuthClientId:         "cid",
		AuthTargetClientId:   "tid",
		AuthIssuerURL:        "https://authn.example.com/v3/fk",
		AuthSubjectTokenPath: "/some/path/token",
	}

	cases := []struct {
		name    string
		mutate  func(c *PulsarConf)
		wantErr bool
	}{
		{"valid config", func(c *PulsarConf) {}, false},
		{"missing client_id", func(c *PulsarConf) { c.AuthClientId = "" }, true},
		{"missing target_client_id", func(c *PulsarConf) { c.AuthTargetClientId = "" }, true},
		{"missing issuer", func(c *PulsarConf) { c.AuthIssuerURL = "" }, true},
		{"non-https issuer", func(c *PulsarConf) { c.AuthIssuerURL = "http://insecure.example.com" }, true},
		{"malformed issuer", func(c *PulsarConf) { c.AuthIssuerURL = "://bad" }, true},
		{"issuer without host", func(c *PulsarConf) { c.AuthIssuerURL = "https://" }, true},
		{"empty subject path falls back to default", func(c *PulsarConf) { c.AuthSubjectTokenPath = "" }, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := base
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
			if conf.AuthSubjectTokenPath == "" && p.subjectTokenPath != defaultSubjectTokenPath {
				t.Errorf("expected fallback subject path %q, got %q", defaultSubjectTokenPath, p.subjectTokenPath)
			}
			wantEndpoint := "https://authn.example.com" + authnTokenExchangePath
			if conf.AuthIssuerURL == base.AuthIssuerURL && p.tokenEndpoint != wantEndpoint {
				t.Errorf("expected token endpoint %q, got %q", wantEndpoint, p.tokenEndpoint)
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
		if r.URL.Path != authnTokenExchangePath {
			t.Errorf("expected path %s, got %s", authnTokenExchangePath, r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/x-www-form-urlencoded" {
			t.Errorf("expected form content-type, got %q", ct)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("failed to parse form: %v", err)
		}
		if got := r.PostForm.Get("grant_type"); got != tokenExchangeGrantType {
			t.Errorf("grant_type = %q, want %q", got, tokenExchangeGrantType)
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

	p := newTestProvider(srv.URL+authnTokenExchangePath, subjPath)
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

			p := newTestProvider(srv.URL+authnTokenExchangePath, tc.subjPath)
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

	p := newTestProvider(srv.URL+authnTokenExchangePath, subjPath)

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

	p := newTestProvider(srv.URL+authnTokenExchangePath, subjPath)

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
		src := GetPulsarSource(PulsarConf{
			AuthMode:           AuthModeMachineIdentity,
			AuthClientId:       "cid",
			AuthTargetClientId: "tid",
			AuthIssuerURL:      "https://authn.example.com/v3/fk",
		})
		auth, err := src.buildAuthentication()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if auth == nil {
			t.Error("expected non-nil authentication")
		}
	})

	t.Run("machine_identity invalid config", func(t *testing.T) {
		src := GetPulsarSource(PulsarConf{
			AuthMode:      AuthModeMachineIdentity,
			AuthClientId:  "cid",
			AuthIssuerURL: "http://insecure.example.com",
		})
		if _, err := src.buildAuthentication(); err == nil {
			t.Error("expected error for invalid machine_identity config, got nil")
		}
	})

	t.Run("legacy empty auth mode", func(t *testing.T) {
		// Empty auth mode must take the legacy client-credentials branch. That
		// branch never returns an error (unlike machine_identity, which would
		// error here on the missing target_client_id), so a nil error confirms
		// the correct branch was selected. We do not assert on the returned
		// Authentication value because the SDK's NewAuthenticationOAuth2 may
		// return nil for placeholder credentials.
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
