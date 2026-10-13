package auth

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// userConfig is the JSON shape accepted in FORGEQUEUE_USERS.
type userConfig struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	Roles    []string `json:"roles"`
}

// NewFromEnv builds a Service from the environment.
//
//	FORGEQUEUE_AUTH_SECRET    HS256 signing secret (random if unset)
//	FORGEQUEUE_AUTH_TTL       token lifetime (default 12h)
//	FORGEQUEUE_USERS          JSON array of {username, password, roles}
//	FORGEQUEUE_ADMIN_USER     bootstrap admin username (default "admin")
//	FORGEQUEUE_ADMIN_PASSWORD bootstrap admin password (random if unset)
//
// With no FORGEQUEUE_USERS, a single admin is bootstrapped.
func NewFromEnv() (*Service, error) {
	secret, err := resolveSecret()
	if err != nil {
		return nil, err
	}

	ttl := 12 * time.Hour
	if raw := strings.TrimSpace(os.Getenv("FORGEQUEUE_AUTH_TTL")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("auth: invalid FORGEQUEUE_AUTH_TTL %q: %w", raw, err)
		}
		ttl = parsed
	}

	var users []User
	switch {
	case strings.TrimSpace(os.Getenv("FORGEQUEUE_USERS")) != "":
		users, err = parseUsers(os.Getenv("FORGEQUEUE_USERS"))
		if err != nil {
			return nil, err
		}
	case oidcConfigured() && strings.TrimSpace(os.Getenv("FORGEQUEUE_ADMIN_PASSWORD")) == "":
		log.Printf("auth: OIDC is enabled; no local accounts configured (set FORGEQUEUE_USERS or FORGEQUEUE_ADMIN_PASSWORD for break-glass access)")
	default:
		users, err = bootstrapAdmin()
		if err != nil {
			return nil, err
		}
	}

	return NewService(users, secret, ttl)
}

// oidcConfigured reports whether single sign-on is enabled via the environment.
func oidcConfigured() bool {
	return strings.TrimSpace(os.Getenv("FORGEQUEUE_OIDC_ISSUER")) != ""
}

// NewOIDCFromEnv builds the SSO provider from the environment, or returns
// (nil, nil) when FORGEQUEUE_OIDC_ISSUER is unset.
//
//	FORGEQUEUE_OIDC_ISSUER           provider issuer URL (enables SSO)
//	FORGEQUEUE_OIDC_CLIENT_ID        OAuth client id
//	FORGEQUEUE_OIDC_CLIENT_SECRET    OAuth client secret (optional with PKCE)
//	FORGEQUEUE_OIDC_REDIRECT_URL     our callback URL
//	FORGEQUEUE_OIDC_PROVIDER_NAME    button label (defaults to issuer host)
//	FORGEQUEUE_OIDC_SCOPES           space-separated (default "openid email profile")
//	FORGEQUEUE_OIDC_ADMIN_EMAILS     comma-separated emails mapped to admin
//	FORGEQUEUE_OIDC_OPERATOR_EMAILS  comma-separated emails mapped to operator
//	FORGEQUEUE_OIDC_DEFAULT_ROLE     role for others (viewer|operator|admin|none, default viewer)
//	FORGEQUEUE_OIDC_ALLOWED_ORIGINS  extra origins allowed as post-login redirect targets
//	FORGEQUEUE_WEB_URL               default post-login redirect (default "/")
func NewOIDCFromEnv(svc *Service, store StateStore) (*OIDC, error) {
	if !oidcConfigured() {
		return nil, nil
	}

	cfg := OIDCConfig{
		Issuer:         strings.TrimSpace(os.Getenv("FORGEQUEUE_OIDC_ISSUER")),
		ClientID:       strings.TrimSpace(os.Getenv("FORGEQUEUE_OIDC_CLIENT_ID")),
		ClientSecret:   os.Getenv("FORGEQUEUE_OIDC_CLIENT_SECRET"),
		RedirectURL:    strings.TrimSpace(os.Getenv("FORGEQUEUE_OIDC_REDIRECT_URL")),
		WebURL:         strings.TrimSpace(os.Getenv("FORGEQUEUE_WEB_URL")),
		ProviderName:   strings.TrimSpace(os.Getenv("FORGEQUEUE_OIDC_PROVIDER_NAME")),
		AdminEmails:    splitList(os.Getenv("FORGEQUEUE_OIDC_ADMIN_EMAILS")),
		OperatorEmails: splitList(os.Getenv("FORGEQUEUE_OIDC_OPERATOR_EMAILS")),
	}
	if raw := strings.TrimSpace(os.Getenv("FORGEQUEUE_OIDC_SCOPES")); raw != "" {
		cfg.Scopes = strings.Fields(raw)
	}

	if raw := strings.TrimSpace(os.Getenv("FORGEQUEUE_OIDC_DEFAULT_ROLE")); raw != "" {
		if strings.EqualFold(raw, "none") {
			cfg.DefaultRole = ""
		} else {
			role := Role(raw)
			if !ValidRole(role) {
				return nil, fmt.Errorf("auth: invalid FORGEQUEUE_OIDC_DEFAULT_ROLE %q", raw)
			}
			cfg.DefaultRole = role
		}
	} else {
		cfg.DefaultRole = RoleViewer
	}

	if cfg.ClientID == "" || cfg.RedirectURL == "" {
		return nil, errors.New("auth: FORGEQUEUE_OIDC_CLIENT_ID and FORGEQUEUE_OIDC_REDIRECT_URL are required when FORGEQUEUE_OIDC_ISSUER is set")
	}

	oidc, err := NewOIDC(cfg, svc, store)
	if err != nil {
		return nil, err
	}
	oidc.SetAllowedOrigins(splitList(os.Getenv("FORGEQUEUE_OIDC_ALLOWED_ORIGINS")))

	dest := cfg.WebURL
	if dest == "" {
		dest = "/"
	}
	log.Printf("auth: OIDC enabled via %s (provider %q, default role %q, redirect %q)", cfg.Issuer, oidc.ProviderName(), roleOrNone(cfg.DefaultRole), dest)
	return oidc, nil
}

func roleOrNone(r Role) string {
	if r == "" {
		return "none"
	}
	return string(r)
}

// splitList parses a comma-separated environment value into trimmed, non-empty
// entries.
func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseUsers(raw string) ([]User, error) {
	var cfgs []userConfig
	if err := json.Unmarshal([]byte(raw), &cfgs); err != nil {
		return nil, fmt.Errorf("auth: FORGEQUEUE_USERS is not valid JSON: %w", err)
	}
	if len(cfgs) == 0 {
		return nil, fmt.Errorf("auth: FORGEQUEUE_USERS is empty")
	}
	users := make([]User, 0, len(cfgs))
	for _, c := range cfgs {
		if strings.TrimSpace(c.Username) == "" {
			return nil, fmt.Errorf("auth: FORGEQUEUE_USERS entry missing username")
		}
		if c.Password == "" {
			return nil, fmt.Errorf("auth: user %q missing password", c.Username)
		}
		hash, err := hashPassword(c.Password)
		if err != nil {
			return nil, err
		}
		roles := make([]Role, len(c.Roles))
		for i, r := range c.Roles {
			roles[i] = Role(r)
		}
		users = append(users, User{Username: c.Username, PasswordHash: hash, Roles: roles})
	}
	return users, nil
}

func bootstrapAdmin() ([]User, error) {
	username := strings.TrimSpace(os.Getenv("FORGEQUEUE_ADMIN_USER"))
	if username == "" {
		username = "admin"
	}

	password := os.Getenv("FORGEQUEUE_ADMIN_PASSWORD")
	generated := false
	if password == "" {
		var err error
		password, err = randomToken(16)
		if err != nil {
			return nil, err
		}
		generated = true
	}

	hash, err := hashPassword(password)
	if err != nil {
		return nil, err
	}
	if generated {
		log.Printf("auth: no FORGEQUEUE_USERS set; bootstrapped admin %q with generated password: %s", username, password)
		log.Printf("auth: set FORGEQUEUE_ADMIN_PASSWORD (or FORGEQUEUE_USERS) to pin this password")
	} else {
		log.Printf("auth: no FORGEQUEUE_USERS set; bootstrapped admin %q from FORGEQUEUE_ADMIN_PASSWORD", username)
	}
	return []User{{Username: username, PasswordHash: hash, Roles: []Role{RoleAdmin}}}, nil
}

func resolveSecret() ([]byte, error) {
	if raw := strings.TrimSpace(os.Getenv("FORGEQUEUE_AUTH_SECRET")); raw != "" {
		return []byte(raw), nil
	}
	secret, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	log.Printf("auth: FORGEQUEUE_AUTH_SECRET is unset; generated an ephemeral secret (tokens will not survive a restart)")
	return []byte(secret), nil
}

func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
