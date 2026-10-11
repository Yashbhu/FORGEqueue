import type { ReactNode } from "react";
import { useHasPermission, type Permission } from "@/lib/auth";

/**
 * Renders children only when the current session holds `perm`. This is a UX
 * affordance, not a security boundary: the API enforces the same permissions
 * server-side, so hiding a control never grants or denies access on its own.
 */
export function Can({ perm, children }: { perm: Permission; children: ReactNode }) {
  return useHasPermission(perm) ? <>{children}</> : null;
}
