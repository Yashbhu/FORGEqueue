import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Link, useNavigate, useRouterState } from "@tanstack/react-router";
import { useIsFetching, useQueryClient } from "@tanstack/react-query";
import {
  Activity,
  BarChart3,
  Clock,
  Cpu,
  Layers,
  LayoutDashboard,
  LogOut,
  Monitor,
  Moon,
  PanelLeft,
  Plug,
  RefreshCw,
  Search,
  Settings,
  Skull,
  Sun,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { setPrefs, usePrefs, type AutoRefresh, type ThemePref } from "@/lib/prefs";
import { clearSession, useSession, type Permission } from "@/lib/auth";
import { useConnection, type ConnStatus } from "@/api/client";
import { useDelayed, useDlq, useQueues, useWorkers } from "@/api/queries";
import { getPrefs } from "@/lib/prefs";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Kbd, StatusBadge } from "./shared";

interface NavEntry {
  to: string;
  label: string;
  icon: typeof Layers;
  perm?: Permission;
}

const NAV: NavEntry[] = [
  { to: "/", label: "Overview", icon: LayoutDashboard, perm: "system:read" },
  { to: "/queues", label: "Queues", icon: Layers, perm: "queues:read" },
  { to: "/workers", label: "Workers", icon: Cpu, perm: "workers:read" },
  { to: "/delayed", label: "Delayed / Scheduled", icon: Clock, perm: "delayed:read" },
  { to: "/dlq", label: "Dead Letter Queue", icon: Skull, perm: "dlq:read" },
  { to: "/metrics", label: "Metrics", icon: BarChart3, perm: "metrics:read" },
];
const SYS: { to: string; label: string; icon: typeof Layers }[] = [
  { to: "/settings", label: "Settings", icon: Settings },
  { to: "/connection", label: "API / Connection", icon: Plug },
];

/** Filters nav entries down to those the session is allowed to see. */
function visibleNav<T extends { perm?: Permission }>(items: T[], perms: string[]): T[] {
  return items.filter((n) => !n.perm || perms.includes(n.perm));
}

const CONN_LABEL: Record<ConnStatus, string> = {
  connected: "Connected",
  connecting: "Connecting",
  disconnected: "Disconnected",
  error: "Error",
};
const CONN_DOT: Record<ConnStatus, string> = {
  connected: "bg-success",
  connecting: "bg-warning animate-pulse",
  disconnected: "bg-muted-foreground",
  error: "bg-destructive",
};

export function ConnectionDot({ withText = true }: { withText?: boolean }) {
  const { status } = useConnection();
  return (
    <span className="inline-flex items-center gap-1.5 text-xs">
      <span className={cn("size-2 rounded-full", CONN_DOT[status])} aria-hidden />
      {withText && <span className="text-muted-foreground">{CONN_LABEL[status]}</span>}
    </span>
  );
}

function useThemeEffect() {
  const { theme } = usePrefs();
  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const apply = () =>
      document.documentElement.classList.toggle(
        "dark",
        theme === "dark" || (theme === "system" && mq.matches),
      );
    apply();
    mq.addEventListener("change", apply);
    return () => mq.removeEventListener("change", apply);
  }, [theme]);
}

function Logo() {
  return (
    <svg viewBox="0 0 16 16" className="size-4 text-primary" aria-hidden>
      <rect x="1" y="2" width="4" height="12" fill="currentColor" />
      <rect x="6" y="5" width="4" height="9" fill="currentColor" opacity=".7" />
      <rect x="11" y="8" width="4" height="6" fill="currentColor" opacity=".4" />
    </svg>
  );
}

function NavItem({
  to,
  label,
  icon: Icon,
  collapsed,
}: {
  to: string;
  label: string;
  icon: typeof Layers;
  collapsed: boolean;
}) {
  const path = useRouterState({ select: (s) => s.location.pathname });
  const active =
    to === "/"
      ? path === "/"
      : path.startsWith(to) || (to === "/queues" && path.startsWith("/jobs"));
  const link = (
    <Link
      to={to}
      className={cn(
        "flex h-7 items-center gap-2 rounded-sm px-2 text-[13px] text-sidebar-foreground hover:bg-sidebar-accent",
        active && "bg-sidebar-accent font-medium text-sidebar-accent-foreground",
        collapsed && "justify-center px-0",
      )}
      aria-current={active ? "page" : undefined}
    >
      <Icon
        className={cn("size-3.5 shrink-0", active ? "text-primary" : "text-muted-foreground")}
      />
      {!collapsed && <span className="truncate">{label}</span>}
    </Link>
  );
  return collapsed ? (
    <Tooltip>
      <TooltipTrigger asChild>{link}</TooltipTrigger>
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  ) : (
    link
  );
}

function UserFooter({ collapsed }: { collapsed: boolean }) {
  const session = useSession();
  const qc = useQueryClient();
  if (!session) return null;
  const { username, roles } = session.user;

  const logout = () => {
    clearSession();
    qc.clear();
  };

  return (
    <div
      className={cn(
        "flex items-center gap-2 border-t px-3 py-2",
        collapsed && "justify-center px-0",
      )}
    >
      {!collapsed ? (
        <>
          <div className="min-w-0 flex-1">
            <div className="truncate text-xs font-medium" title={username}>
              {username}
            </div>
            <div className="truncate text-[10px] capitalize text-muted-foreground">
              {roles.join(", ")}
            </div>
          </div>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="size-7"
                aria-label="Sign out"
                onClick={logout}
              >
                <LogOut className="size-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>Sign out</TooltipContent>
          </Tooltip>
        </>
      ) : (
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="size-7"
              aria-label="Sign out"
              onClick={logout}
            >
              <LogOut className="size-3.5" />
            </Button>
          </TooltipTrigger>
          <TooltipContent side="right">Sign out ({username})</TooltipContent>
        </Tooltip>
      )}
    </div>
  );
}

function Sidebar({ collapsed }: { collapsed: boolean }) {
  const session = useSession();
  const perms = session?.user.permissions ?? [];
  const nav = visibleNav(NAV, perms);
  return (
    <aside
      className={cn(
        "sticky top-0 flex h-screen shrink-0 flex-col border-r bg-sidebar",
        collapsed ? "w-12" : "w-52",
      )}
    >
      <div
        className={cn(
          "flex h-10 items-center gap-2 border-b px-3 font-semibold tracking-tight",
          collapsed && "justify-center px-0",
        )}
      >
        <Logo />
        {!collapsed && <span>ForgeQueue</span>}
      </div>
      <nav className="flex-1 space-y-0.5 overflow-y-auto p-1.5" aria-label="Main">
        {nav.map((n) => (
          <NavItem key={n.to} {...n} collapsed={collapsed} />
        ))}
        <div className="my-2 border-t" />
        {!collapsed && (
          <div className="px-2 pb-1 text-[10px] uppercase tracking-wider text-muted-foreground">
            System
          </div>
        )}
        {SYS.map((n) => (
          <NavItem key={n.to} {...n} collapsed={collapsed} />
        ))}
      </nav>
      <UserFooter collapsed={collapsed} />
      <div
        className={cn(
          "space-y-1 border-t px-3 py-2",
          collapsed && "flex flex-col items-center px-0",
        )}
      >
        {!collapsed && (
          <div className="font-mono text-[11px] text-muted-foreground">
            {import.meta.env["VITE_APP_VERSION"] || "v0.9.1"}
          </div>
        )}
        <ConnectionDot withText={!collapsed} />
      </div>
    </aside>
  );
}

const THEMES: { v: ThemePref; icon: typeof Sun; label: string }[] = [
  { v: "light", icon: Sun, label: "Light" },
  { v: "dark", icon: Moon, label: "Dark" },
  { v: "system", icon: Monitor, label: "System" },
];

function Topbar({ onToggle, onSearch }: { onToggle: () => void; onSearch: () => void }) {
  const prefs = usePrefs();
  const qc = useQueryClient();
  const fetching = useIsFetching();
  const { lastSuccessAt } = useConnection();
  const [, tick] = useState(0);
  useEffect(() => {
    const t = setInterval(() => tick((x) => x + 1), 1000);
    return () => clearInterval(t);
  }, []);
  const env = prefs.apiBase.startsWith("/")
    ? "local-dev"
    : prefs.apiBase.replace(/^https?:\/\//, "");
  return (
    <header className="sticky top-0 z-20 flex h-10 items-center gap-2 border-b bg-background/95 px-2 backdrop-blur">
      <Button
        variant="ghost"
        size="icon"
        className="size-7"
        onClick={onToggle}
        aria-label="Toggle sidebar"
      >
        <PanelLeft className="size-3.5" />
      </Button>
      <span className="rounded-sm border px-1.5 py-px font-mono text-[11px] text-muted-foreground">
        {env}
      </span>
      <button
        type="button"
        onClick={onSearch}
        className="ml-2 flex h-7 w-64 items-center gap-2 rounded-sm border bg-muted/40 px-2 text-xs text-muted-foreground hover:bg-muted"
      >
        <Search className="size-3.5" />
        Search jobs, queues, workers…
        <span className="ml-auto">
          <Kbd>⌘K</Kbd>
        </span>
      </button>
      <div className="ml-auto flex items-center gap-1.5">
        <span className="hidden items-center gap-1 font-mono text-[11px] text-muted-foreground md:flex">
          <Activity className={cn("size-3", fetching > 0 && "text-primary")} />
          {lastSuccessAt
            ? `updated ${Math.max(0, Math.round((Date.now() - lastSuccessAt) / 1000))}s ago`
            : "—"}
        </span>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="size-7"
              aria-label="Refresh now"
              onClick={() => qc.invalidateQueries()}
            >
              <RefreshCw className={cn("size-3.5", fetching > 0 && "animate-spin")} />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Refresh now</TooltipContent>
        </Tooltip>
        <div
          className="flex items-center rounded-sm border p-px"
          role="radiogroup"
          aria-label="Auto-refresh"
        >
          {(["off", "5s", "10s", "30s"] as AutoRefresh[]).map((a) => (
            <button
              key={a}
              role="radio"
              aria-checked={prefs.autoRefresh === a}
              onClick={() => setPrefs({ autoRefresh: a })}
              className={cn(
                "rounded-[2px] px-1.5 py-0.5 font-mono text-[11px]",
                prefs.autoRefresh === a
                  ? "bg-accent text-foreground"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              {a}
            </button>
          ))}
        </div>
        <div
          className="flex items-center rounded-sm border p-px"
          role="radiogroup"
          aria-label="Theme"
        >
          {THEMES.map(({ v, icon: I, label }) => (
            <button
              key={v}
              role="radio"
              aria-checked={prefs.theme === v}
              aria-label={label}
              title={label}
              onClick={() => setPrefs({ theme: v })}
              className={cn(
                "rounded-[2px] p-1",
                prefs.theme === v
                  ? "bg-accent text-foreground"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              <I className="size-3" />
            </button>
          ))}
        </div>
        <span className="pl-1">
          <ConnectionDot />
        </span>
      </div>
    </header>
  );
}

function CommandPalette({ open, setOpen }: { open: boolean; setOpen: (o: boolean) => void }) {
  const navigate = useNavigate();
  const session = useSession();
  const perms = session?.user.permissions ?? [];
  const nav = visibleNav(NAV, perms);
  const queues = useQueues().data ?? [];
  const workers = useWorkers().data ?? [];
  const dlq = useDlq().data ?? [];
  const delayed = useDelayed().data ?? [];
  const jobs = useMemo(
    () => [
      ...dlq.map((d) => ({
        id: d.jobId,
        queue: d.originalQueue,
        task: d.task,
        status: "dlq" as const,
      })),
      ...delayed.map((d) => ({ id: d.jobId, queue: d.queue, task: d.task, status: d.status })),
    ],
    [dlq, delayed],
  );
  const go = (fn: () => void) => {
    setOpen(false);
    fn();
  };
  return (
    <CommandDialog open={open} onOpenChange={setOpen}>
      <CommandInput placeholder="Job ID, queue, task, worker…" className="text-xs" />
      <CommandList>
        <CommandEmpty>No results</CommandEmpty>
        <CommandGroup heading="Navigate">
          {[...nav, ...SYS].map((n) => (
            <CommandItem
              key={n.to}
              value={`go ${n.label}`}
              onSelect={() => go(() => navigate({ to: n.to }))}
              className="text-xs"
            >
              <n.icon className="size-3.5" /> {n.label}
            </CommandItem>
          ))}
        </CommandGroup>
        <CommandGroup heading="Queues">
          {queues.map((q) => (
            <CommandItem
              key={q.name}
              value={`queue ${q.name}`}
              onSelect={() =>
                go(() => navigate({ to: "/queues/$queueName", params: { queueName: q.name } }))
              }
              className="text-xs"
            >
              <Layers className="size-3.5" /> <span className="font-mono">{q.name}</span>
              <span className="ml-auto">
                <StatusBadge status={q.status} />
              </span>
            </CommandItem>
          ))}
        </CommandGroup>
        <CommandGroup heading="Workers">
          {workers.map((w) => (
            <CommandItem
              key={w.id}
              value={`worker ${w.id} ${w.hostname}`}
              onSelect={() =>
                go(() => navigate({ to: "/workers/$workerId", params: { workerId: w.id } }))
              }
              className="text-xs"
            >
              <Cpu className="size-3.5" /> <span className="font-mono">{w.id}</span>
              <span className="font-mono text-muted-foreground">{w.hostname}</span>
              <span className="ml-auto">
                <StatusBadge status={w.status} />
              </span>
            </CommandItem>
          ))}
        </CommandGroup>
        <CommandGroup heading="Jobs">
          {jobs.map((j) => (
            <CommandItem
              key={j.id}
              value={`job ${j.id} ${j.task} ${j.queue}`}
              onSelect={() => go(() => navigate({ to: "/jobs/$jobId", params: { jobId: j.id } }))}
              className="text-xs"
            >
              <span className="font-mono">{j.id.slice(0, 13)}</span>
              <span className="font-mono text-muted-foreground">
                {j.queue} · {j.task}
              </span>
              <span className="ml-auto">
                <StatusBadge status={j.status} />
              </span>
            </CommandItem>
          ))}
        </CommandGroup>
      </CommandList>
    </CommandDialog>
  );
}

export function AppShell({ children }: { children: ReactNode }) {
  useThemeEffect();
  const [collapsed, setCollapsed] = useState(false);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    getPrefs();
    const h = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen((o) => !o);
      }
    };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, []);
  return (
    <div className="flex min-h-screen w-full">
      <Sidebar collapsed={collapsed} />
      <div className="flex min-w-0 flex-1 flex-col">
        <Topbar onToggle={() => setCollapsed(!collapsed)} onSearch={() => setOpen(true)} />
        <main className="mx-auto w-full max-w-[1600px] flex-1 p-3">{children}</main>
      </div>
      <CommandPalette open={open} setOpen={setOpen} />
    </div>
  );
}
