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
// token path, bypassing newAuthTokenProvider so individual fields can be set.
func newTestProvider(endpoint, subjectTokenPath string) *authTokenProvider {
	return &authTokenProvider{
		tokenEndpoint: endpoint,
		exchangeForm: map[string]string{
			"grant_type":       testGrantType,
			"client_id":        "test-client",
			"target_client_id": "test-target",
		},
		subjectTokenPath:  subjectTokenPath,
		subjectTokenField: defaultSubjectTokenField,
		httpClient:        &http.Client{Timeout: authHTTPTimeout},
	}
}

func baseMachineIdentityConf() PulsarConf {
	return PulsarConf{
		AuthMode:             AuthModeMachineIdentity,
		AuthIssuerURL:        "https://auth.example.com/v3/oauth/token",
		AuthSubjectTokenPath: "/some/path/token",
		AuthExchangeForm: map[string]string{
			"grant_type":       testGrantType,
			"client_id":        "cid",
			"target_client_id": "tid",
		},
	}
}

func TestNewAuthTokenProvider_Validation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(c *PulsarConf)
		wantErr bool
	}{
		{"valid config", func(c *PulsarConf) {}, false},
		{"missing issuer url", func(c *PulsarConf) { c.AuthIssuerURL = "" }, true},
		{"non-https issuer url", func(c *PulsarConf) { c.AuthIssuerURL = "http://insecure.example.com/token" }, true},
		{"malformed issuer url", func(c *PulsarConf) { c.AuthIssuerURL = "://bad" }, true},
		{"issuer url without host", func(c *PulsarConf) { c.AuthIssuerURL = "https://" }, true},
		{"missing subject path", func(c *PulsarConf) { c.AuthSubjectTokenPath = "" }, true},
		{"empty exchange form", func(c *PulsarConf) { c.AuthExchangeForm = nil }, true},
		{"custom subject token field", func(c *PulsarConf) { c.AuthSubjectTokenField = "assertion" }, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := baseMachineIdentityConf()
			tc.mutate(&conf)
			p, err := newAuthTokenProvider(conf)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (provider=%v)", p)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p.tokenEndpoint != conf.AuthIssuerURL {
				t.Errorf("expected token endpoint %q, got %q", conf.AuthIssuerURL, p.tokenEndpoint)
			}
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

func TestTokenSupplier_Caching(t *testing.T) {
	subjPath := writeSubjectToken(t, "subject")
	var hits int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Write([]byte(`{"access_token":"tok","expires_in":3600}`))
	}))
	defer srv.Close()

	p := newTestProvider(srv.URL+testTokenPath, subjPath)
	supplier := p.tokenSupplier()

	if _, err := supplier(); err != nil {
		t.Fatalf("first supplier call failed: %v", err)
	}
	if _, err := supplier(); err != nil {
		t.Fatalf("second supplier call failed: %v", err)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Errorf("expected 1 server hit (second served from cache), got %d", got)
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
		conf.AuthIssuerURL = "http://insecure.example.com/token"
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
			AuthIssuerURL:    "https://auth.example.com/v3/fk",
			AuthAudience:     "viesti-prod",
		})
		if _, err := src.buildAuthentication(); err != nil {
			t.Fatalf("unexpected error on legacy path: %v", err)
		}
	})
}
