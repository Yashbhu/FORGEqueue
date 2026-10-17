// Package version holds build identity for every binary. The values are
// injected at build time with -ldflags so a running process can report exactly
// which commit it came from; the defaults keep local builds working.
package version

var (
	// Version is the semantic release tag, e.g. "v1.0.0".
	Version = "v1.0.0"
	// Commit is the git SHA the binary was built from.
	Commit = "unknown"
	// Date is the build timestamp (RFC3339).
	Date = "unknown"
)

// Full returns "version (commit)" or just the version when no commit is known.
func Full() string {
	if Commit == "" || Commit == "unknown" {
		return Version
	}
	return Version + " (" + Commit + ")"
}
