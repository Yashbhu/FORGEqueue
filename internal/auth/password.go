package auth

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Password hashes use PBKDF2-HMAC-SHA256, encoded as:
//
//	pbkdf2$sha256$<iterations>$<salt-b64>$<derived-key-b64>
//
// Only the standard library is used so the server has no crypto dependency to
// manage. Verification is constant-time.
const (
	pbkdf2Iterations = 210_000
	pbkdf2KeyLength  = 32
	pbkdf2SaltLength = 16
)

var errBadHash = errors.New("malformed password hash")

func hashPassword(password string) (string, error) {
	salt := make([]byte, pbkdf2SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	dk, err := pbkdf2.Key(sha256.New, password, salt, pbkdf2Iterations, pbkdf2KeyLength)
	if err != nil {
		return "", err
	}
	enc := base64.RawStdEncoding
	return fmt.Sprintf("pbkdf2$sha256$%d$%s$%s",
		pbkdf2Iterations,
		enc.EncodeToString(salt),
		enc.EncodeToString(dk),
	), nil
}

func verifyPassword(password, encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 5 || parts[0] != "pbkdf2" || parts[1] != "sha256" {
		return false
	}
	iter, err := strconv.Atoi(parts[2])
	if err != nil || iter <= 0 {
		return false
	}
	enc := base64.RawStdEncoding
	salt, err := enc.DecodeString(parts[3])
	if err != nil {
		return false
	}
	want, err := enc.DecodeString(parts[4])
	if err != nil || len(want) == 0 {
		return false
	}
	got, err := pbkdf2.Key(sha256.New, password, salt, iter, len(want))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}
