package auth

import (
	"context"
	"errors"
	"time"
)

// ErrInvalidCredentials is returned for an unknown username or a bad password.
// The same error is used for both.
var ErrInvalidCredentials = errors.New("invalid credentials")

// Identity is the authenticated principal attached to a request. Permissions
// are derived from Roles at authentication time.
type Identity struct {
	Username    string       `json:"username"`
	Roles       []Role       `json:"roles"`
	Permissions []Permission `json:"permissions"`
}

// Can reports whether the identity holds permission p.
func (i Identity) Can(p Permission) bool { return hasPermission(i.Permissions, p) }

// User is a configured account. PasswordHash is never serialized.
type User struct {
	Username     string
	PasswordHash string
	Roles        []Role
}

type ctxKey struct{}

// WithIdentity returns a context carrying the authenticated identity.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext returns the identity attached to ctx, if any.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

// Service authenticates users and issues/verifies bearer tokens.
type Service struct {
	users  map[string]*User
	tokens *tokenManager
}

// NewService builds a Service from already-resolved users and secrets.
func NewService(users []User, secret []byte, ttl time.Duration) (*Service, error) {
	if len(secret) == 0 {
		return nil, errors.New("auth: empty signing secret")
	}
	if ttl <= 0 {
		ttl = 12 * time.Hour
	}
	byName := make(map[string]*User, len(users))
	for i := range users {
		u := users[i]
		if u.Username == "" {
			return nil, errors.New("auth: user with empty username")
		}
		if _, dup := byName[u.Username]; dup {
			return nil, errors.New("auth: duplicate user " + u.Username)
		}
		for _, r := range u.Roles {
			if !ValidRole(r) {
				return nil, errors.New("auth: user " + u.Username + " has unknown role " + string(r))
			}
		}
		byName[u.Username] = &u
	}
	return &Service{
		users:  byName,
		tokens: &tokenManager{secret: secret, ttl: ttl},
	}, nil
}

// HasLocalUsers reports whether any username/password accounts are configured.
func (s *Service) HasLocalUsers() bool { return s != nil && len(s.users) > 0 }

// Authenticate verifies credentials and returns the resulting identity.
func (s *Service) Authenticate(username, password string) (Identity, error) {
	u, ok := s.users[username]
	if !ok || !verifyPassword(password, u.PasswordHash) {
		return Identity{}, ErrInvalidCredentials
	}
	return s.identityFor(u.Username, u.Roles), nil
}

// IssueToken mints a bearer token for an identity.
func (s *Service) IssueToken(id Identity) (string, time.Time, error) {
	return s.tokens.issue(id.Username, id.Roles)
}

// VerifyToken validates a bearer token and rebuilds the identity. Local users
// are re-read so a role change takes effect at once. SSO subjects use the
// roles embedded in the token.
func (s *Service) VerifyToken(token string) (Identity, error) {
	c, err := s.tokens.verify(token)
	if err != nil {
		return Identity{}, err
	}
	if u, ok := s.users[c.Subject]; ok {
		return s.identityFor(u.Username, u.Roles), nil
	}
	roles := make([]Role, 0, len(c.Roles))
	for _, r := range c.Roles {
		if ValidRole(Role(r)) {
			roles = append(roles, Role(r))
		}
	}
	if len(roles) == 0 {
		return Identity{}, ErrInvalidToken
	}
	return s.identityFor(c.Subject, roles), nil
}

// NewIdentity builds an identity for an externally-authenticated principal
// such as an SSO login.
func (s *Service) NewIdentity(username string, roles []Role) Identity {
	return s.identityFor(username, roles)
}

func (s *Service) identityFor(username string, roles []Role) Identity {
	return Identity{
		Username:    username,
		Roles:       append([]Role(nil), roles...),
		Permissions: permissionsForRoles(roles),
	}
}
