import type { ReactNode } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { PageHeader, Section } from "@/components/shared";
import { Switch } from "@/components/ui/switch";
import { setPrefs, usePrefs, type Prefs } from "@/lib/prefs";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/settings")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "Settings — ForgeQueue Console" },
      {
        name: "description",
        content: "Console preferences: theme, table density, polling interval and time format.",
      },
      { property: "og:title", content: "Settings — ForgeQueue Console" },
      {
        property: "og:description",
        content: "Console preferences: theme, table density, polling interval and time format.",
      },
    ],
  }),
  component: SettingsPage,
});

function Row({ label, hint, children }: { label: string; hint?: string; children: ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-4 border-b py-2 last:border-b-0">
      <div>
        <div className="text-xs font-medium">{label}</div>
        {hint && <div className="text-[11px] text-muted-foreground">{hint}</div>}
      </div>
      {children}
    </div>
  );
}

function Seg<K extends keyof Prefs>({ k, options }: { k: K; options: Prefs[K][] }) {
  const p = usePrefs();
  return (
    <div className="flex rounded-sm border p-px" role="radiogroup">
      {options.map((o) => (
        <button
          key={String(o)}
          role="radio"
          aria-checked={p[k] === o}
          onClick={() => setPrefs({ [k]: o } as Partial<Prefs>)}
          className={cn(
            "rounded-[2px] px-2 py-0.5 font-mono text-xs",
            p[k] === o
              ? "bg-accent text-foreground"
              : "text-muted-foreground hover:text-foreground",
          )}
        >
          {String(o)}
        </button>
      ))}
    </div>
  );
}

function SettingsPage() {
  const p = usePrefs();
  return (
    <>
      <PageHeader title="Settings" description="Stored locally in this browser." />
      <div className="max-w-2xl space-y-3">
        <Section title="Appearance">
          <Row label="Theme" hint="System follows prefers-color-scheme.">
            <Seg k="theme" options={["light", "dark", "system"]} />
          </Row>
          <Row label="Table density">
            <Seg k="density" options={["compact", "comfortable"]} />
          </Row>
        </Section>
        <Section title="Behavior">
          <Row label="Polling interval" hint="Applies to all live views.">
            <Seg k="autoRefresh" options={["off", "5s", "10s", "30s"]} />
          </Row>
          <Row label="Relative time by default" hint="Hover any timestamp for the exact value.">
            <Switch
              checked={p.relativeTime}
              onCheckedChange={(v) => setPrefs({ relativeTime: v })}
            />
          </Row>
        </Section>
      </div>
    </>
  );
}
