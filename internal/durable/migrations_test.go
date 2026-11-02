package durable

import "testing"

// TestMigrationsAreOrdered guards the append-only contract: versions must be
// strictly increasing and unique so the ledger applies them exactly once and
// in a deterministic order.
func TestMigrationsAreOrdered(t *testing.T) {
	if len(migrations) == 0 {
		t.Fatal("no migrations defined")
	}
	for i, m := range migrations {
		if m.Version <= 0 {
			t.Errorf("migration %d has non-positive version %d", i, m.Version)
		}
		if m.Name == "" {
			t.Errorf("migration %d (version %d) has empty name", i, m.Version)
		}
		if m.SQL == "" {
			t.Errorf("migration %d (%s) has empty SQL", i, m.Name)
		}
		if i > 0 && m.Version <= migrations[i-1].Version {
			t.Errorf("migration %q version %d is not greater than previous %d",
				m.Name, m.Version, migrations[i-1].Version)
		}
	}
}
