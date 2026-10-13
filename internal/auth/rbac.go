package auth

import "sort"

// Permission is a single, coarse-grained capability granted to a role.
type Permission string

const (
	PermSystemRead   Permission = "system:read"
	PermQueuesRead   Permission = "queues:read"
	PermQueuesWrite  Permission = "queues:write"
	PermJobsRead     Permission = "jobs:read"
	PermJobsWrite    Permission = "jobs:write"
	PermWorkersRead  Permission = "workers:read"
	PermDLQRead      Permission = "dlq:read"
	PermDLQWrite     Permission = "dlq:write"
	PermDelayedRead  Permission = "delayed:read"
	PermDelayedWrite Permission = "delayed:write"
	PermMetricsRead  Permission = "metrics:read"
)

// Role groups permissions. Roles are assigned to users, permissions are
// never assigned directly.
type Role string

const (
	RoleAdmin    Role = "admin"
	RoleOperator Role = "operator"
	RoleViewer   Role = "viewer"
)

// rolePermissions maps each role to its permissions. Every permission
// must appear in at least one role.
var rolePermissions = map[Role][]Permission{
	RoleViewer: {
		PermSystemRead,
		PermQueuesRead,
		PermJobsRead,
		PermWorkersRead,
		PermDLQRead,
		PermDelayedRead,
		PermMetricsRead,
	},
	RoleOperator: {
		PermSystemRead,
		PermQueuesRead,
		PermQueuesWrite,
		PermJobsRead,
		PermJobsWrite,
		PermWorkersRead,
		PermDLQRead,
		PermDLQWrite,
		PermDelayedRead,
		PermDelayedWrite,
		PermMetricsRead,
	},
	RoleAdmin: {
		PermSystemRead,
		PermQueuesRead,
		PermQueuesWrite,
		PermJobsRead,
		PermJobsWrite,
		PermWorkersRead,
		PermDLQRead,
		PermDLQWrite,
		PermDelayedRead,
		PermDelayedWrite,
		PermMetricsRead,
	},
}

// ValidRole reports whether r is a role the server understands.
func ValidRole(r Role) bool {
	_, ok := rolePermissions[r]
	return ok
}

// permissionsForRoles flattens and de-duplicates the permissions granted by a
// set of roles, sorted for stable output.
func permissionsForRoles(roles []Role) []Permission {
	seen := make(map[Permission]struct{})
	for _, r := range roles {
		for _, p := range rolePermissions[r] {
			seen[p] = struct{}{}
		}
	}
	out := make([]Permission, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// hasPermission reports whether perms contains p.
func hasPermission(perms []Permission, p Permission) bool {
	for _, have := range perms {
		if have == p {
			return true
		}
	}
	return false
}
