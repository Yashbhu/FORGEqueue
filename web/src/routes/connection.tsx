import { useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { api } from "@/api/endpoints";
import { useConnection } from "@/api/client";
import { ConnectionDot } from "@/components/app-shell";
import { KV, PageHeader, Section, TimeAgo } from "@/components/shared";
import { Button } from "@/components/ui/button";
import { setPrefs, usePrefs } from "@/lib/prefs";

export const Route = createFileRoute("/connection")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "API / Connection — ForgeQueue Console" },
      {
        name: "description",
        content: "Configure the ForgeQueue gRPC Gateway base URL and check connectivity.",
      },
      { property: "og:title", content: "API / Connection — ForgeQueue Console" },
      {
        property: "og:description",
        content: "Configure the ForgeQueue gRPC Gateway base URL and check connectivity.",
      },
    ],
  }),
  component: ConnectionPage,
});

function ConnectionPage() {
  const prefs = usePrefs();
  const conn = useConnection();
  const qc = useQueryClient();
  const [base, setBase] = useState(prefs.apiBase);
  const [version, setVersion] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);

  const test = async () => {
    setTesting(true);
    try {
      const v = await api.version();
      setVersion(v.version);
      toast.success(`Connected · ${v.version}`);
    } catch (e) {
      toast.error(`Connection failed: ${e instanceof Error ? e.message : e}`);
    } finally {
      setTesting(false);
    }
  };

  return (
    <>
      <PageHeader
        title="API / Connection"
        description="HTTP/JSON via the ForgeQueue gRPC Gateway."
      />
      <div className="max-w-2xl space-y-3">
        <Section title="Endpoint">
          <form
            className="flex gap-1.5"
            onSubmit={(e) => {
              e.preventDefault();
              setPrefs({ apiBase: base });
              qc.invalidateQueries();
              toast.success("API base updated");
            }}
          >
            <input
              value={base}
              onChange={(e) => setBase(e.target.value)}
              aria-label="API base URL"
              className="h-7 flex-1 rounded-sm border bg-background px-2 font-mono text-xs focus-visible:outline focus-visible:outline-ring"
            />
            <Button type="submit" size="sm" variant="outline" className="h-7 text-xs">
              Save
            </Button>
            <Button
              type="button"
              size="sm"
              className="h-7 text-xs"
              onClick={test}
              disabled={testing}
            >
              {testing ? "Testing…" : "Test connection"}
            </Button>
          </form>
          <p className="mt-1.5 text-[11px] text-muted-foreground">
            Default from <code className="font-mono">VITE_API_BASE_URL</code> (
            {import.meta.env["VITE_API_BASE_URL"] || "/api"}).
          </p>
        </Section>
        <Section title="Status">
          <KV
            items={[
              ["status", <ConnectionDot />],
              [
                "last_successful_fetch",
                conn.lastSuccessAt ? (
                  <TimeAgo iso={new Date(conn.lastSuccessAt).toISOString()} />
                ) : null,
              ],
              ["server_version", version],
              [
                "last_error",
                conn.lastError && <span className="text-destructive">{conn.lastError}</span>,
              ],
            ]}
          />
        </Section>
      </div>
    </>
  );
}
