package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// fakeIdP stands up a minimal OpenID Connect provider for tests.
func fakeIdP(t *testing.T, email, name string) *httptest.Server {
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
		_ = r.ParseForm()
		if r.Form.Get("code_verifier") == "" {
			http.Error(w, "missing PKCE verifier", http.StatusBadRequest)
			return
		}
		if r.Form.Get("client_id") != "client" {
			http.Error(w, "bad client", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "opaque-token"})
	})
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer opaque-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"sub": "subject-1", "email": email, "name": name})
	})

	srv := httptest.NewServer(mux)
	base = srv.URL
	t.Cleanup(srv.Close)
	return srv
}

func newTestOIDC(t *testing.T, cfg OIDCConfig) *OIDC {
	t.Helper()
	svc, err := NewService(nil, []byte("test-secret"), time.Hour)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	oidc, err := NewOIDC(cfg, svc, NewMemoryStore())
	if err != nil {
		t.Fatalf("NewOIDC: %v", err)
	}
	return oidc
}

func TestOIDCFlow(t *testing.T) {
	idp := fakeIdP(t, "ops@example.com", "Ops User")
	oidc := newTestOIDC(t, OIDCConfig{
		Issuer:         idp.URL,
		ClientID:       "client",
		RedirectURL:    "http://console.local/api/auth/oidc/callback",
		WebURL:         "http://console.local",
		OperatorEmails: []string{"ops@example.com"},
		DefaultRole:    "",
	})
	ctx := context.Background()

	authURL, err := oidc.Begin(ctx, "")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	u, err := url.Parse(authURL)
	if err != nil {
		t.Fatalf("parse auth URL: %v", err)
	}
	q := u.Query()
	if q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" {
		t.Fatalf("expected S256 PKCE challenge, got %q", authURL)
	}
	state := q.Get("state")
	if state == "" {
		t.Fatal("missing state")
	}

	otc, sess, err := oidc.Complete(ctx, state, "auth-code")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(sess.Identity.Roles) != 1 || sess.Identity.Roles[0] != RoleOperator {
		t.Fatalf("roles = %v, want [operator]", sess.Identity.Roles)
	}
	if sess.Identity.Username != "Ops User" {
		t.Fatalf("username = %q", sess.Identity.Username)
	}
	if sess.WebReturn != "http://console.local" {
		t.Fatalf("webReturn = %q", sess.WebReturn)
	}

	exchanged, err := oidc.Exchange(ctx, otc)
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if exchanged.Token != sess.Token {
		t.Fatal("exchanged token differs")
	}
	if _, err := oidc.Exchange(ctx, otc); err == nil {
		t.Fatal("expected one-time code to be consumed")
	}
}

func TestOIDCRejectsUnauthorizedEmail(t *testing.T) {
	idp := fakeIdP(t, "stranger@example.com", "Stranger")
	oidc := newTestOIDC(t, OIDCConfig{
		Issuer:         idp.URL,
		ClientID:       "client",
		RedirectURL:    "http://console.local/api/auth/oidc/callback",
		AdminEmails:    []string{"admin@example.com"},
		OperatorEmails: []string{"ops@example.com"},
		DefaultRole:    "",
	})
	ctx := context.Background()

	authURL, _ := oidc.Begin(ctx, "")
	u, _ := url.Parse(authURL)
	if _, _, err := oidc.Complete(ctx, u.Query().Get("state"), "auth-code"); err == nil {
		t.Fatal("expected unauthorized email to be rejected")
	}
}

func TestOIDCDefaultRole(t *testing.T) {
	idp := fakeIdP(t, "anyone@example.com", "Anyone")
	oidc := newTestOIDC(t, OIDCConfig{
		Issuer:      idp.URL,
		ClientID:    "client",
		RedirectURL: "http://console.local/api/auth/oidc/callback",
		DefaultRole: RoleViewer,
	})
	ctx := context.Background()

	authURL, _ := oidc.Begin(ctx, "")
	u, _ := url.Parse(authURL)
	_, sess, err := oidc.Complete(ctx, u.Query().Get("state"), "auth-code")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(sess.Identity.Roles) != 1 || sess.Identity.Roles[0] != RoleViewer {
		t.Fatalf("roles = %v, want [viewer]", sess.Identity.Roles)
	}
}

func TestOIDCRejectsUnknownState(t *testing.T) {
	idp := fakeIdP(t, "ops@example.com", "Ops")
	oidc := newTestOIDC(t, OIDCConfig{
		Issuer:      idp.URL,
		ClientID:    "client",
		RedirectURL: "http://console.local/api/auth/oidc/callback",
		DefaultRole: RoleViewer,
	})
	if _, _, err := oidc.Complete(context.Background(), "bogus-state", "code"); err == nil {
		t.Fatal("expected unknown state to be rejected")
	}
}

func TestResolveReturnBlocksOpenRedirect(t *testing.T) {
	oidc := newTestOIDC(t, OIDCConfig{
		Issuer:      "https://idp.example.com",
		ClientID:    "client",
		RedirectURL: "https://console.example.com/api/auth/oidc/callback",
		WebURL:      "https://console.example.com",
	})
	oidc.SetAllowedOrigins([]string{"https://admin.example.com"})

	if got := oidc.ResolveReturn(""); got != "https://console.example.com" {
		t.Fatalf("default return = %q", got)
	}
	if got := oidc.ResolveReturn("/queues"); got != "/queues" {
		t.Fatalf("relative return = %q", got)
	}
	if got := oidc.ResolveReturn("https://admin.example.com/dlq"); got != "https://admin.example.com/dlq" {
		t.Fatalf("allowed origin = %q", got)
	}
	if got := oidc.ResolveReturn("https://evil.example.com"); got != "https://console.example.com" {
		t.Fatalf("open redirect not blocked: %q", got)
	}
}

func TestSSOTokenVerifiesWithoutLocalUser(t *testing.T) {
	svc, err := NewService(nil, []byte("secret"), time.Hour)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	id := svc.NewIdentity("SSO User", []Role{RoleOperator})
	token, _, err := svc.IssueToken(id)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	got, err := svc.VerifyToken(token)
	if err != nil {
		t.Fatalf("VerifyToken: %v", err)
	}
	if !got.Can(PermQueuesWrite) {
		t.Fatal("operator should hold queues:write")
	}
	if !strings.EqualFold(got.Username, "SSO User") {
		t.Fatalf("username = %q", got.Username)
	}
}
