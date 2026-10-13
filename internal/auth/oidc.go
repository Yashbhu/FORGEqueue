package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// OIDCConfig describes an OpenID Connect provider. Any provider that
// supports discovery works, so only the issuer and client ID are needed.
type OIDCConfig struct {
	Issuer         string
	ClientID       string
	ClientSecret   string   // optional: omit for public clients using PKCE
	RedirectURL    string   // our callback, e.g. http://host:8080/api/auth/oidc/callback
	WebURL         string   // where the browser returns after login, e.g. http://localhost:3000
	Scopes         []string // defaults to openid email profile
	ProviderName   string   // display name, defaults to the issuer host
	DefaultRole    Role     // role for non-allowlisted users, empty denies
	AdminEmails    []string // emails granted the admin role
	OperatorEmails []string // emails granted the operator role
	HTTPClient     *http.Client
	StateTTL       time.Duration // how long a login attempt may take
	CodeTTL        time.Duration // lifetime of the one-time exchange code
}

// SSOSession is the result of a completed SSO login.
type SSOSession struct {
	Token     string
	ExpiresAt time.Time
	Identity  Identity
	WebReturn string
}

// ErrSSOFailed is returned for any failure in the SSO flow. The cause is
// wrapped for logs but kept out of the HTTP response.
var ErrSSOFailed = errors.New("sso authentication failed")

// OIDC drives the authorization-code + PKCE flow and converts a provider
// identity into a ForgeQueue identity (and JWT).
type OIDC struct {
	cfg   OIDCConfig
	store StateStore
	svc   *Service
	http  *http.Client

	admin  map[string]bool
	op     map[string]bool
	scopes string

	allowed        map[string]bool
	allowedDefault string

	mu       sync.Mutex
	provider *providerMeta
}

type providerMeta struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
}

type authState struct {
	Verifier  string `json:"verifier"`
	WebReturn string `json:"web_return"`
}

type userInfo struct {
	Subject           string `json:"sub"`
	Email             string `json:"email"`
	Name              string `json:"name"`
	PreferredUsername string `json:"preferred_username"`
}

func NewOIDC(cfg OIDCConfig, svc *Service, store StateStore) (*OIDC, error) {
	if cfg.Issuer == "" || cfg.ClientID == "" || cfg.RedirectURL == "" {
		return nil, errors.New("auth: OIDC requires issuer, client id and redirect URL")
	}
	if store == nil {
		return nil, errors.New("auth: OIDC requires a state store")
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{"openid", "email", "profile"}
	}
	if cfg.StateTTL <= 0 {
		cfg.StateTTL = 10 * time.Minute
	}
	if cfg.CodeTTL <= 0 {
		cfg.CodeTTL = 60 * time.Second
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	allowed := map[string]bool{}
	for _, raw := range []string{cfg.WebURL, cfg.RedirectURL} {
		if origin := originOf(raw); origin != "" {
			allowed[origin] = true
		}
	}
	def := cfg.WebURL
	if def == "" {
		def = "/"
	}
	return &OIDC{
		cfg:            cfg,
		store:          store,
		svc:            svc,
		http:           hc,
		admin:          emailSet(cfg.AdminEmails),
		op:             emailSet(cfg.OperatorEmails),
		scopes:         strings.Join(cfg.Scopes, " "),
		allowed:        allowed,
		allowedDefault: def,
	}, nil
}

// SetAllowedOrigins registers extra post-login redirect origins. Relative
// targets and the configured web/redirect origins are always allowed.
func (o *OIDC) SetAllowedOrigins(origins []string) {
	for _, raw := range origins {
		if origin := originOf(raw); origin != "" {
			o.allowed[origin] = true
		}
	}
}

// DefaultReturn is the fallback post-login target used when the requested one
// is unknown (e.g. reporting an error before state is resolved).
func (o *OIDC) DefaultReturn() string { return o.allowedDefault }

// ResolveReturn validates a requested post-login target against the allowlist,
// falling back to the configured web URL.
func (o *OIDC) ResolveReturn(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return o.allowedDefault
	}
	if !strings.Contains(raw, "://") {
		if strings.HasPrefix(raw, "/") {
			return raw
		}
		return o.allowedDefault
	}
	if o.allowed[originOf(raw)] {
		return raw
	}
	return o.allowedDefault
}

// ProviderName is a human-friendly provider label for the login button.
func (o *OIDC) ProviderName() string {
	if o.cfg.ProviderName != "" {
		return o.cfg.ProviderName
	}
	if u, err := url.Parse(o.cfg.Issuer); err == nil && u.Host != "" {
		return u.Host
	}
	return "SSO"
}

// Begin creates fresh state + a PKCE verifier, stores them, and returns the
// provider authorization URL to redirect the browser to.
func (o *OIDC) Begin(ctx context.Context, webReturn string) (string, error) {
	webReturn = o.ResolveReturn(webReturn)
	meta, err := o.discover(ctx)
	if err != nil {
		return "", err
	}
	state, err := randomToken(24)
	if err != nil {
		return "", err
	}
	verifier, err := randomToken(32)
	if err != nil {
		return "", err
	}
	stored, _ := json.Marshal(authState{Verifier: verifier, WebReturn: webReturn})
	if err := o.store.Save(ctx, "oidc:state:"+state, stored, o.cfg.StateTTL); err != nil {
		return "", fmt.Errorf("store state: %w", err)
	}

	params := url.Values{}
	params.Set("response_type", "code")
	params.Set("client_id", o.cfg.ClientID)
	params.Set("redirect_uri", o.cfg.RedirectURL)
	params.Set("scope", o.scopes)
	params.Set("state", state)
	params.Set("code_challenge", pkceChallenge(verifier))
	params.Set("code_challenge_method", "S256")
	return meta.AuthorizationEndpoint + "?" + params.Encode(), nil
}

// Complete validates the callback, exchanges the code, fetches the user,
// maps roles, and mints a JWT. It returns the one-time exchange code.
func (o *OIDC) Complete(ctx context.Context, state, code string) (string, *SSOSession, error) {
	raw, ok, err := o.store.Load(ctx, "oidc:state:"+state)
	if err != nil {
		return "", nil, fmt.Errorf("%w: load state: %v", ErrSSOFailed, err)
	}
	if !ok {
		return "", nil, fmt.Errorf("%w: unknown or expired state", ErrSSOFailed)
	}
	_ = o.store.Delete(ctx, "oidc:state:"+state)

	var st authState
	if err := json.Unmarshal(raw, &st); err != nil {
		return "", nil, fmt.Errorf("%w: corrupt state", ErrSSOFailed)
	}

	meta, err := o.discover(ctx)
	if err != nil {
		return "", nil, err
	}

	info, err := o.exchange(ctx, meta, code, st.Verifier)
	if err != nil {
		return "", nil, err
	}

	email := strings.TrimSpace(info.Email)
	if email == "" {
		email = info.PreferredUsername
	}
	if email == "" {
		email = info.Subject
	}
	roles, err := o.rolesFor(email)
	if err != nil {
		return "", nil, err
	}

	username := info.Name
	if username == "" {
		username = email
	}
	identity := o.svc.NewIdentity(username, roles)
	token, expires, err := o.svc.IssueToken(identity)
	if err != nil {
		return "", nil, fmt.Errorf("%w: issue token: %v", ErrSSOFailed, err)
	}

	otc, err := randomToken(24)
	if err != nil {
		return "", nil, err
	}
	sess := SSOSession{Token: token, ExpiresAt: expires, Identity: identity, WebReturn: st.WebReturn}
	payload, _ := json.Marshal(sess)
	if err := o.store.Save(ctx, "oidc:code:"+otc, payload, o.cfg.CodeTTL); err != nil {
		return "", nil, fmt.Errorf("%w: store code: %v", ErrSSOFailed, err)
	}
	return otc, &sess, nil
}

// Exchange resolves a one-time code (returned to the browser) into the session.
func (o *OIDC) Exchange(ctx context.Context, code string) (*SSOSession, error) {
	raw, ok, err := o.store.Load(ctx, "oidc:code:"+code)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("invalid or expired code")
	}
	_ = o.store.Delete(ctx, "oidc:code:"+code)
	var sess SSOSession
	if err := json.Unmarshal(raw, &sess); err != nil {
		return nil, errors.New("invalid code payload")
	}
	return &sess, nil
}

func (o *OIDC) exchange(ctx context.Context, meta *providerMeta, code, verifier string) (*userInfo, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", o.cfg.RedirectURL)
	form.Set("client_id", o.cfg.ClientID)
	form.Set("code_verifier", verifier)
	if o.cfg.ClientSecret != "" {
		form.Set("client_secret", o.cfg.ClientSecret)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, meta.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := o.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: token request: %v", ErrSSOFailed, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: token endpoint status %d", ErrSSOFailed, resp.StatusCode)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tok); err != nil || tok.AccessToken == "" {
		return nil, fmt.Errorf("%w: malformed token response", ErrSSOFailed)
	}

	uiReq, err := http.NewRequestWithContext(ctx, http.MethodGet, meta.UserinfoEndpoint, nil)
	if err != nil {
		return nil, err
	}
	uiReq.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	uiReq.Header.Set("Accept", "application/json")
	uiResp, err := o.http.Do(uiReq)
	if err != nil {
		return nil, fmt.Errorf("%w: userinfo request: %v", ErrSSOFailed, err)
	}
	defer uiResp.Body.Close()
	uiBody, _ := io.ReadAll(io.LimitReader(uiResp.Body, 1<<20))
	if uiResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: userinfo status %d", ErrSSOFailed, uiResp.StatusCode)
	}
	var info userInfo
	if err := json.Unmarshal(uiBody, &info); err != nil || info.Subject == "" {
		return nil, fmt.Errorf("%w: malformed userinfo", ErrSSOFailed)
	}
	return &info, nil
}

func (o *OIDC) rolesFor(email string) ([]Role, error) {
	e := strings.ToLower(strings.TrimSpace(email))
	if o.admin[e] {
		return []Role{RoleAdmin}, nil
	}
	if o.op[e] {
		return []Role{RoleOperator}, nil
	}
	if o.cfg.DefaultRole != "" {
		return []Role{o.cfg.DefaultRole}, nil
	}
	return nil, fmt.Errorf("%w: %s is not authorized", ErrSSOFailed, email)
}

func (o *OIDC) discover(ctx context.Context) (*providerMeta, error) {
	o.mu.Lock()
	if o.provider != nil {
		meta := o.provider
		o.mu.Unlock()
		return meta, nil
	}
	o.mu.Unlock()

	wellKnown := strings.TrimRight(o.cfg.Issuer, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, wellKnown, nil)
	if err != nil {
		return nil, err
	}
	resp, err := o.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: discovery: %v", ErrSSOFailed, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: discovery status %d", ErrSSOFailed, resp.StatusCode)
	}
	var meta providerMeta
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, fmt.Errorf("%w: malformed discovery document", ErrSSOFailed)
	}
	if meta.AuthorizationEndpoint == "" || meta.TokenEndpoint == "" {
		return nil, fmt.Errorf("%w: discovery missing endpoints", ErrSSOFailed)
	}
	if meta.UserinfoEndpoint == "" {
		meta.UserinfoEndpoint = strings.TrimRight(meta.Issuer, "/") + "/userinfo"
	}

	o.mu.Lock()
	o.provider = &meta
	o.mu.Unlock()
	return &meta, nil
}

func emailSet(emails []string) map[string]bool {
	set := make(map[string]bool, len(emails))
	for _, e := range emails {
		if e = strings.ToLower(strings.TrimSpace(e)); e != "" {
			set[e] = true
		}
	}
	return set
}

func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// originOf returns the normalized scheme://host origin of a URL, or "".
func originOf(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return strings.ToLower(u.Scheme + "://" + u.Host)
}
