package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"forgequeue/internal/auth"
)

func newAuthServer(t *testing.T) *Server {
	t.Helper()
	t.Setenv("FORGEQUEUE_AUTH_SECRET", "test-secret")
	t.Setenv("FORGEQUEUE_USERS", `[
		{"username":"admin","password":"admin-pw","roles":["admin"]},
		{"username":"viewer","password":"viewer-pw","roles":["viewer"]}
	]`)
	svc, err := auth.NewFromEnv()
	if err != nil {
		t.Fatalf("auth config: %v", err)
	}
	return NewServer("", nil, svc)
}

func login(t *testing.T, s *Server, username, password string) string {
	t.Helper()
	body := strings.NewReader(`{"username":"` + username + `","password":"` + password + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", body)
	rec := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp LoginResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode login: %v", err)
	}
	if resp.Token == "" || resp.User.Username != username {
		t.Fatalf("unexpected login response: %+v", resp)
	}
	return resp.Token
}

func TestLoginAndMe(t *testing.T) {
	s := newAuthServer(t)
	token := login(t, s, "admin", "admin-pw")

	req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("me status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var id auth.Identity
	if err := json.Unmarshal(rec.Body.Bytes(), &id); err != nil {
		t.Fatalf("decode me: %v", err)
	}
	if id.Username != "admin" || !id.Can(auth.PermJobsWrite) {
		t.Fatalf("unexpected identity: %+v", id)
	}
}

func TestRejectsBadCredentials(t *testing.T) {
	s := newAuthServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"username":"admin","password":"wrong"}`))
	rec := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestMeRequiresBearer(t *testing.T) {
	s := newAuthServer(t)
	for _, header := range []string{"", "Bearer ", "Bearer not-a-token", "Basic abc"} {
		req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rec := httptest.NewRecorder()
		s.srv.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("header %q: status = %d, want 401", header, rec.Code)
		}
	}
}

func TestPermissionEnforced(t *testing.T) {
	s := newAuthServer(t)
	viewer := login(t, s, "viewer", "viewer-pw")

	handler := s.require(auth.PermQueuesWrite, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Viewer: authenticated but not authorized for a write.
	req := httptest.NewRequest(http.MethodPost, "/api/queues/default/pause", nil)
	req.Header.Set("Authorization", "Bearer "+viewer)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("viewer write status = %d, want 403", rec.Code)
	}

	// Same viewer is allowed to read.
	read := s.require(auth.PermQueuesRead, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	req = httptest.NewRequest(http.MethodGet, "/api/queues", nil)
	req.Header.Set("Authorization", "Bearer "+viewer)
	rec = httptest.NewRecorder()
	read(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("viewer read status = %d, want 200", rec.Code)
	}
}

func do(s *Server, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(rec, req)
	return rec
}

func TestAuthConfigWithoutOIDC(t *testing.T) {
	s := newAuthServer(t)
	rec := do(s, http.MethodGet, "/api/auth/config", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var cfg AuthConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &cfg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !cfg.Password || cfg.OIDC {
		t.Fatalf("config = %+v, want password-only", cfg)
	}
	// SSO endpoints are absent when OIDC is disabled.
	if rec := do(s, http.MethodGet, "/api/auth/oidc/login", "", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("oidc login status = %d, want 404", rec.Code)
	}
}

// fakeOIDCProvider serves discovery, token and userinfo for a browser-less flow.
func fakeOIDCProvider(t *testing.T, email string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	var base string
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                 base,
			"authorization_endpoint": base + "/authorize",
			"token_endpoint":         base + "/token",
			"userinfo_endpoint":      base + "/userinfo",
		})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "tok"})
	})
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"sub": "1", "email": email, "name": "SSO Person"})
	})
	srv := httptest.NewServer(mux)
	base = srv.URL
	t.Cleanup(srv.Close)
	return srv
}

func TestOIDCLoginFlowEndToEnd(t *testing.T) {
	idp := fakeOIDCProvider(t, "ops@example.com")
	t.Setenv("FORGEQUEUE_AUTH_SECRET", "test-secret")
	t.Setenv("FORGEQUEUE_USERS", `[{"username":"admin","password":"pw","roles":["admin"]}]`)
	t.Setenv("FORGEQUEUE_OIDC_ISSUER", idp.URL)
	t.Setenv("FORGEQUEUE_OIDC_CLIENT_ID", "client")
	t.Setenv("FORGEQUEUE_OIDC_REDIRECT_URL", "http://api.local/api/auth/oidc/callback")
	t.Setenv("FORGEQUEUE_WEB_URL", "http://console.local")
	t.Setenv("FORGEQUEUE_OIDC_OPERATOR_EMAILS", "ops@example.com")

	svc, err := auth.NewFromEnv()
	if err != nil {
		t.Fatalf("auth config: %v", err)
	}
	oidc, err := auth.NewOIDCFromEnv(svc, auth.NewMemoryStore())
	if err != nil {
		t.Fatalf("oidc config: %v", err)
	}
	s := NewServer("", nil, svc)
	s.SetOIDC(oidc)

	// Config advertises both methods.
	rec := do(s, http.MethodGet, "/api/auth/config", "", "")
	var cfg AuthConfig
	_ = json.Unmarshal(rec.Body.Bytes(), &cfg)
	if !cfg.OIDC || !cfg.Password {
		t.Fatalf("config = %+v, want password+oidc", cfg)
	}

	// Begin: redirect to the provider with state.
	rec = do(s, http.MethodGet, "/api/auth/oidc/login?redirect="+url.QueryEscape("http://console.local/login"), "", "")
	if rec.Code != http.StatusFound {
		t.Fatalf("login status = %d, body = %s", rec.Code, rec.Body.String())
	}
	authURL, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse location: %v", err)
	}
	if !strings.HasPrefix(authURL.String(), idp.URL+"/authorize") {
		t.Fatalf("redirect target = %q", authURL)
	}
	state := authURL.Query().Get("state")
	if state == "" {
		t.Fatal("missing state")
	}

	// Callback: exchange the code and bounce back to the console with an otc.
	rec = do(s, http.MethodGet, "/api/auth/oidc/callback?state="+state+"&code=abc", "", "")
	if rec.Code != http.StatusFound {
		t.Fatalf("callback status = %d, body = %s", rec.Code, rec.Body.String())
	}
	back, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("parse callback location: %v", err)
	}
	if back.Scheme+"://"+back.Host+back.Path != "http://console.local/login" {
		t.Fatalf("return target = %q", back)
	}
	otc := back.Query().Get("code")
	if otc == "" {
		t.Fatalf("missing one-time code in %q", back)
	}

	// Exchange the otc for a usable bearer token (operator role).
	rec = do(s, http.MethodPost, "/api/auth/oidc/exchange", "", `{"code":"`+otc+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("exchange status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp LoginResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode exchange: %v", err)
	}
	if !resp.User.Can(auth.PermQueuesWrite) {
		t.Fatalf("sso identity lacks queues:write: %+v", resp.User)
	}

	// The token authenticates against protected endpoints.
	meRec := do(s, http.MethodGet, "/api/auth/me", resp.Token, "")
	if meRec.Code != http.StatusOK {
		t.Fatalf("me status = %d", meRec.Code)
	}

	// The one-time code cannot be replayed.
	if rec := do(s, http.MethodPost, "/api/auth/oidc/exchange", "", `{"code":"`+otc+`"}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("replay status = %d, want 401", rec.Code)
	}
}
