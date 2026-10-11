package auth

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
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

// NewFromEnv assembles a Service from environment configuration:
//
//	FORGEQUEUE_AUTH_SECRET   HS256 signing secret (required in production;
//	                         a random one is generated with a warning if unset)
//	FORGEQUEUE_AUTH_TTL      token lifetime as a Go duration (default 12h)
//	FORGEQUEUE_USERS         JSON array of {username, password, roles}
//	FORGEQUEUE_ADMIN_USER    bootstrap admin username (default "admin")
//	FORGEQUEUE_ADMIN_PASSWORD bootstrap admin password (random if unset)
//
// When FORGEQUEUE_USERS is empty, a single admin account is bootstrapped so a
// fresh deployment is never left with no way in.
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
	if raw := strings.TrimSpace(os.Getenv("FORGEQUEUE_USERS")); raw != "" {
		users, err = parseUsers(raw)
		if err != nil {
			return nil, err
		}
	} else {
		users, err = bootstrapAdmin()
		if err != nil {
			return nil, err
		}
	}

	return NewService(users, secret, ttl)
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
