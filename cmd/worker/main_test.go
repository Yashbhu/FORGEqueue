package main

import (
	"os"
	"testing"

	"forgequeue/internal/handlers"
)

func TestEnvString(t *testing.T) {
	tests := []struct {
		name  string
		set   bool
		value string
		want  string
	}{
		{"unset falls back", false, "", "fallback"},
		{"empty falls back", true, "", "fallback"},
		{"set is used", true, "custom", "custom"},
	}

	const key = "FORGEQUEUE_TEST_STRING"

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			os.Unsetenv(key)

			if tc.set {
				t.Setenv(key, tc.value)
			}

			if got := envString(key, "fallback"); got != tc.want {
				t.Errorf("envString() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEnvInt(t *testing.T) {
	tests := []struct {
		name  string
		set   bool
		value string
		want  int
	}{
		{"unset falls back", false, "", 4},
		{"empty falls back", true, "", 4},
		{"valid number is used", true, "9", 9},
		{"zero is respected, not treated as unset", true, "0", 0},
		{"negative is respected", true, "-1", -1},
		{"garbage falls back", true, "twelve", 4},
		{"float falls back", true, "2.5", 4},
		{"trailing space falls back", true, " 8 ", 4},
	}

	const key = "FORGEQUEUE_TEST_INT"

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			os.Unsetenv(key)

			if tc.set {
				t.Setenv(key, tc.value)
			}

			if got := envInt(key, 4); got != tc.want {
				t.Errorf("envInt() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestSortedKeysIsStable(t *testing.T) {
	m := map[string]handlers.WorkerHandler{
		"zebra":  handlers.Echo{},
		"alpha":  handlers.Fail{},
		"middle": handlers.Sleep{},
	}

	// Map iteration order is randomised by the runtime, so the log line has
	// to sort or two identical startups produce different output.
	want := "alpha, middle, zebra"

	for i := 0; i < 20; i++ {
		if got := sortedKeys(m); got != want {
			t.Fatalf("sortedKeys() = %q, want %q", got, want)
		}
	}

	if got := sortedKeys(nil); got != "" {
		t.Errorf("sortedKeys(nil) = %q, want an empty string", got)
	}
}
