package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidToken is returned for any token that fails signature or shape
// validation. Callers should not distinguish the reasons to avoid leaking
// information about why a token was rejected.
var ErrInvalidToken = errors.New("invalid token")

// claims is the JWT payload. Roles are embedded so the API can authorize a
// request without a Redis round trip; this means a role change takes effect on
// the next login (until then, the old roles remain valid until expiry).
type claims struct {
	Subject string   `json:"sub"`
	Roles   []string `json:"roles"`
	Issued  int64    `json:"iat"`
	Expires int64    `json:"exp"`
}

// tokenManager issues and verifies HS256 JWTs using the configured secret.
type tokenManager struct {
	secret []byte
	ttl    time.Duration
}

var jwtHeader = func() string {
	b, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	return b64url(b)
}()

func (m *tokenManager) issue(username string, roles []Role) (string, time.Time, error) {
	now := time.Now()
	expires := now.Add(m.ttl)
	rs := make([]string, len(roles))
	for i, r := range roles {
		rs[i] = string(r)
	}
	payload, err := json.Marshal(claims{
		Subject: username,
		Roles:   rs,
		Issued:  now.Unix(),
		Expires: expires.Unix(),
	})
	if err != nil {
		return "", time.Time{}, err
	}
	signingInput := jwtHeader + "." + b64url(payload)
	return signingInput + "." + m.sign(signingInput), expires, nil
}

func (m *tokenManager) verify(token string) (claims, error) {
	var c claims
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return c, ErrInvalidToken
	}
	signingInput := parts[0] + "." + parts[1]
	if !hmac.Equal([]byte(m.sign(signingInput)), []byte(parts[2])) {
		return c, ErrInvalidToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return c, ErrInvalidToken
	}
	if err := json.Unmarshal(payload, &c); err != nil {
		return c, ErrInvalidToken
	}
	if c.Subject == "" {
		return c, ErrInvalidToken
	}
	if time.Now().Unix() >= c.Expires {
		return c, fmt.Errorf("%w: expired", ErrInvalidToken)
	}
	return c, nil
}

func (m *tokenManager) sign(input string) string {
	mac := hmac.New(sha256.New, m.secret)
	mac.Write([]byte(input))
	return b64url(mac.Sum(nil))
}

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
