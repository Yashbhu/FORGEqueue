import { useEffect, useState, type ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import { format, formatDistanceToNowStrict } from "date-fns";
import { Check, Copy, Inbox } from "lucide-react";
import { toast } from "sonner";
import { cn } from "@/lib/utils";
import { fmtDuration, fmtNum } from "@/lib/format";
import { usePrefs } from "@/lib/prefs";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import type { JobStatus, QueueStatus, WorkerStatus } from "@/types/forgequeue";

type Tone = "success" | "warning" | "danger" | "info" | "neutral";
const toneClass: Record<Tone, string> = {
  success: "tone-success",
  warning: "tone-warning",
  danger: "tone-danger",
  info: "tone-info",
  neutral: "tone-neutral",
};
const textTone: Record<Tone, string> = {
  success: "text-success",
  warning: "text-warning",
  danger: "text-destructive",
  info: "text-info",
  neutral: "",
};
const STATUS_TONE: Record<JobStatus | QueueStatus | WorkerStatus, Tone> = {
  pending: "neutral",
  processing: "info",
  completed: "success",
  failed: "danger",
  retrying: "warning",
  delayed: "neutral",
  dlq: "danger",
  cancelled: "neutral",
  healthy: "success",
  paused: "warning",
  degraded: "warning",
  unknown: "neutral",
  idle: "neutral",
  busy: "info",
  stale: "warning",
  disconnected: "danger",
};

export function StatusBadge({ status }: { status: JobStatus | QueueStatus | WorkerStatus }) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-sm border px-1.5 py-px font-mono text-[11px] leading-4",
        toneClass[STATUS_TONE[status]],
      )}
    >
      <span className="size-1.5 rounded-full bg-current" aria-hidden />
      {status}
    </span>
  );
}

export function PageHeader({
  title,
  description,
  actions,
  meta,
}: {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  meta?: ReactNode;
}) {
  return (
    <header className="mb-3 flex flex-wrap items-end justify-between gap-2 border-b pb-3">
      <div className="min-w-0">
        <h1 className="flex items-center gap-2 text-base font-semibold">{title}</h1>
        {description && <p className="mt-0.5 text-xs text-muted-foreground">{description}</p>}
        {meta && <div className="mt-1.5 flex flex-wrap items-center gap-2 text-xs">{meta}</div>}
      </div>
      {actions && <div className="flex flex-wrap items-center gap-1.5">{actions}</div>}
    </header>
  );
}

export function Section({
  title,
  actions,
  children,
  className,
  flush,
}: {
  title: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  flush?: boolean;
}) {
  return (
    <section className={cn("rounded-md border bg-card", className)}>
      <div className="flex h-8 items-center justify-between gap-2 border-b px-2.5">
        <h2 className="text-xs font-medium">{title}</h2>
        {actions && <div className="flex items-center gap-1">{actions}</div>}
      </div>
      <div className={flush ? "" : "p-2.5"}>{children}</div>
    </section>
  );
}

export function StatCard({
  label,
  value,
  tone,
  to,
  hint,
}: {
  label: string;
  value: number | string | undefined;
  tone?: Tone | undefined;
  to?: string;
  hint?: string | undefined;
}) {
  const body = (
    <div className="rounded-md border bg-card px-2.5 py-2 transition-colors hover:border-ring/40">
      <div className="text-[11px] text-muted-foreground">{label}</div>
      <div
        className={cn(
          "font-mono text-lg font-medium leading-6 tabular-nums",
          tone && textTone[tone],
        )}
      >
        {value === undefined ? (
          <span className="inline-block h-4 w-10 animate-pulse rounded bg-muted" />
        ) : typeof value === "number" ? (
          fmtNum(value)
        ) : (
          value
        )}
      </div>
      {hint && <div className="text-[11px] text-muted-foreground">{hint}</div>}
    </div>
  );
  return to ? <Link to={to}>{body}</Link> : body;
}

/** Ticks every second so relative times stay live. */
export function useNow(ms = 1000) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(t);
  }, [ms]);
  return now;
}

export function TimeAgo({ iso, future }: { iso: string | null | undefined; future?: boolean }) {
  const { relativeTime } = usePrefs();
  useNow(5000);
  if (!iso) return <span className="text-muted-foreground">—</span>;
  const d = new Date(iso);
  const rel = formatDistanceToNowStrict(d, { addSuffix: true });
  const abs = format(d, "yyyy-MM-dd HH:mm:ss");
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <time
          dateTime={iso}
          className={cn("whitespace-nowrap font-mono text-xs", future && "text-info")}
        >
          {relativeTime ? rel : abs}
        </time>
      </TooltipTrigger>
      <TooltipContent className="font-mono text-xs">
        {relativeTime ? `${abs} · ${iso}` : rel}
      </TooltipContent>
    </Tooltip>
  );
}

export const Duration = ({ ms }: { ms: number | null | undefined }) => (
  <span className="whitespace-nowrap font-mono text-xs tabular-nums">{fmtDuration(ms)}</span>
);

export function CopyButton({ value, label = "Copy" }: { value: string; label?: string }) {
  const [done, setDone] = useState(false);
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-label={label}
          onClick={(e) => {
            e.stopPropagation();
            navigator.clipboard.writeText(value).then(() => {
              setDone(true);
              toast.success("Copied to clipboard");
              setTimeout(() => setDone(false), 1200);
            });
          }}
          className="inline-flex size-5 shrink-0 items-center justify-center rounded-sm text-muted-foreground hover:bg-accent hover:text-foreground focus-visible:outline focus-visible:outline-ring"
        >
          {done ? <Check className="size-3" /> : <Copy className="size-3" />}
        </button>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  );
}

export function JobId({ id, full, link = true }: { id: string; full?: boolean; link?: boolean }) {
  const text = full ? id : id.slice(0, 13);
  return (
    <span className="inline-flex items-center gap-0.5 font-mono text-xs">
      {link ? (
        <Link
          to="/jobs/$jobId"
          params={{ jobId: id }}
          className="text-primary hover:underline"
          onClick={(e) => e.stopPropagation()}
          title={id}
        >
          {text}
        </Link>
      ) : (
        <span title={id}>{text}</span>
      )}
      <CopyButton value={id} label="Copy job ID" />
    </span>
  );
}

export const Mono = ({ children, className }: { children: ReactNode; className?: string }) => (
  <span className={cn("font-mono text-xs", className)}>{children}</span>
);

export function EmptyState({
  title,
  description,
  icon: Icon = Inbox,
}: {
  title: string;
  description?: string | undefined;
  icon?: typeof Inbox;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-1 py-8 text-center">
      <Icon className="size-5 text-muted-foreground" aria-hidden />
      <div className="text-sm font-medium">{title}</div>
      {description && <div className="text-xs text-muted-foreground">{description}</div>}
    </div>
  );
}

export const Kbd = ({ children }: { children: ReactNode }) => (
  <kbd className="rounded-sm border bg-muted px-1 font-mono text-[10px] text-muted-foreground">
    {children}
  </kbd>
);

export function KV({ items }: { items: [string, ReactNode][] }) {
  return (
    <dl className="grid grid-cols-1 gap-x-4 gap-y-1.5 sm:grid-cols-2 lg:grid-cols-3">
      {items.map(([k, v]) => (
        <div key={k} className="min-w-0">
          <dt className="text-[11px] text-muted-foreground">{k}</dt>
          <dd className="truncate font-mono text-xs">
            {v ?? <span className="text-muted-foreground">—</span>}
          </dd>
        </div>
      ))}
    </dl>
  );
}

export function ErrorBlock({
  message,
  stack,
  code,
}: {
  message: string;
  stack?: string;
  code?: string;
}) {
  return (
    <div className="rounded-sm border tone-danger">
      <div className="flex items-start justify-between gap-2 border-b border-inherit px-2 py-1.5 font-mono text-xs">
        <span className="break-all">
          {code && <span className="mr-1.5 font-semibold">{code}</span>}
          {message}
        </span>
        <CopyButton value={[code, message, stack].filter(Boolean).join("\n")} label="Copy error" />
      </div>
      {stack && (
        <pre className="max-h-64 overflow-auto px-2 py-1.5 font-mono text-[11px] leading-4 text-foreground/80">
          {stack}
        </pre>
      )}
    </div>
  );
}
