package auth

import (
	"strings"
	"testing"
	"time"
)

func TestPasswordHashRoundTrip(t *testing.T) {
	hash, err := hashPassword("correct horse battery staple")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if !strings.HasPrefix(hash, "pbkdf2$sha256$") {
		t.Fatalf("unexpected hash format: %q", hash)
	}
	if !verifyPassword("correct horse battery staple", hash) {
		t.Fatal("correct password rejected")
	}
	if verifyPassword("wrong password", hash) {
		t.Fatal("incorrect password accepted")
	}
	if verifyPassword("correct horse battery staple", "garbage") {
		t.Fatal("malformed hash accepted")
	}
}

func TestTokenIssueAndVerify(t *testing.T) {
	svc := newTestService(t)
	id := Identity{Username: "alice", Roles: []Role{RoleOperator}}
	token, expires, err := svc.IssueToken(id)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if !expires.After(time.Now()) {
		t.Fatalf("expiry %v is not in the future", expires)
	}
	got, err := svc.VerifyToken(token)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got.Username != "alice" {
		t.Fatalf("username = %q, want alice", got.Username)
	}
	if !got.Can(PermJobsWrite) || !got.Can(PermQueuesRead) {
		t.Fatalf("operator permissions = %v", got.Permissions)
	}
	if got.Can(PermSystemRead) == false {
		// operator inherits viewer's system:read
		t.Fatal("operator should read system")
	}
}

func TestTokenRejectsTampering(t *testing.T) {
	svc := newTestService(t)
	token, _, err := svc.IssueToken(Identity{Username: "alice", Roles: []Role{RoleViewer}})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	parts := strings.Split(token, ".")
	tampered := parts[0] + "." + parts[1] + "." + b64url([]byte("not-a-signature"))
	if _, err := svc.VerifyToken(tampered); err == nil {
		t.Fatal("tampered token verified")
	}
	if _, err := svc.VerifyToken("malformed"); err == nil {
		t.Fatal("malformed token verified")
	}
}

func TestTokenExpiry(t *testing.T) {
	users := []User{{Username: "alice", PasswordHash: mustHash(t, "pw"), Roles: []Role{RoleViewer}}}
	svc, err := NewService(users, []byte("secret"), -time.Second)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	// NewService clamps non-positive TTLs to a default, so mint with a tiny TTL
	// by reaching into the token manager directly.
	svc.tokens.ttl = time.Nanosecond
	token, _, err := svc.IssueToken(Identity{Username: "alice", Roles: []Role{RoleViewer}})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, err := svc.VerifyToken(token); err == nil {
		t.Fatal("expired token verified")
	}
}

func TestAuthenticate(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.Authenticate("alice", "s3cret"); err != nil {
		t.Fatalf("valid credentials rejected: %v", err)
	}
	if _, err := svc.Authenticate("alice", "nope"); err == nil {
		t.Fatal("bad password accepted")
	}
	if _, err := svc.Authenticate("ghost", "s3cret"); err == nil {
		t.Fatal("unknown user accepted")
	}
}

func TestViewerCannotWrite(t *testing.T) {
	perms := permissionsForRoles([]Role{RoleViewer})
	for _, p := range []Permission{PermJobsWrite, PermQueuesWrite, PermDLQWrite, PermDelayedWrite} {
		if hasPermission(perms, p) {
			t.Fatalf("viewer unexpectedly holds %s", p)
		}
	}
	for _, p := range []Permission{PermJobsRead, PermQueuesRead, PermMetricsRead} {
		if !hasPermission(perms, p) {
			t.Fatalf("viewer missing %s", p)
		}
	}
}

func TestAdminHoldsAllPermissions(t *testing.T) {
	perms := permissionsForRoles([]Role{RoleAdmin})
	all := []Permission{
		PermSystemRead, PermQueuesRead, PermQueuesWrite, PermJobsRead, PermJobsWrite,
		PermWorkersRead, PermDLQRead, PermDLQWrite, PermDelayedRead, PermDelayedWrite, PermMetricsRead,
	}
	for _, p := range all {
		if !hasPermission(perms, p) {
			t.Fatalf("admin missing %s", p)
		}
	}
}

func TestNewServiceValidation(t *testing.T) {
	hash := mustHash(t, "pw")
	if _, err := NewService([]User{{Username: "a", PasswordHash: hash, Roles: []Role{"wizard"}}}, []byte("s"), time.Hour); err == nil {
		t.Fatal("unknown role accepted")
	}
	if _, err := NewService([]User{
		{Username: "a", PasswordHash: hash, Roles: []Role{RoleViewer}},
		{Username: "a", PasswordHash: hash, Roles: []Role{RoleViewer}},
	}, []byte("s"), time.Hour); err == nil {
		t.Fatal("duplicate user accepted")
	}
	if _, err := NewService([]User{{Username: "a", PasswordHash: hash, Roles: []Role{RoleViewer}}}, nil, time.Hour); err == nil {
		t.Fatal("empty secret accepted")
	}
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := NewService(
		[]User{{Username: "alice", PasswordHash: mustHash(t, "s3cret"), Roles: []Role{RoleOperator}}},
		[]byte("test-secret"),
		time.Hour,
	)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return svc
}

func mustHash(t *testing.T, password string) string {
	t.Helper()
	hash, err := hashPassword(password)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	return hash
}
