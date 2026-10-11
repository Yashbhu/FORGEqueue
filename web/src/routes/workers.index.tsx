import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useWorkers } from "@/api/queries";
import { DataTable } from "@/components/data-table";
import {
  Mono,
  PageHeader,
  Section,
  StatCard,
  StatusBadge,
  TimeAgo,
  useNow,
} from "@/components/shared";
import { fmtDuration } from "@/lib/format";

export const Route = createFileRoute("/workers/")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "Workers — ForgeQueue Console" },
      {
        name: "description",
        content: "Worker pool health: heartbeats, leases, concurrency and versions.",
      },
      { property: "og:title", content: "Workers — ForgeQueue Console" },
      {
        property: "og:description",
        content: "Worker pool health: heartbeats, leases, concurrency and versions.",
      },
    ],
  }),
  component: WorkersList,
});

function WorkersList() {
  const { data } = useWorkers();
  const navigate = useNavigate();
  const now = useNow(1000);
  const healthy = data?.filter((w) => ["healthy", "busy", "idle"].includes(w.status)).length;
  const expired = data?.filter((w) => w.leases.some((l) => Date.parse(l.expiresAt) < now)).length;
  return (
    <>
      <PageHeader
        title="Workers"
        description="Heartbeat older than 30s marks a worker stale; its leases are reclaimed on expiry."
      />
      <div className="mb-3 grid grid-cols-2 gap-2 md:grid-cols-5">
        <StatCard label="Active" value={data?.length} />
        <StatCard label="Healthy" value={healthy} tone="success" />
        <StatCard
          label="Unhealthy"
          value={data && data.length - (healthy ?? 0)}
          tone={data && data.length - (healthy ?? 0) ? "warning" : undefined}
        />
        <StatCard label="Expired leases" value={expired} tone={expired ? "danger" : undefined} />
        <StatCard
          label="Jobs processing"
          value={data?.reduce((a, w) => a + w.jobsProcessing, 0)}
          tone="info"
        />
      </div>
      <Section title="Worker pool" flush>
        <DataTable
          rows={data}
          getId={(w) => w.id}
          onRowClick={(w) => navigate({ to: "/workers/$workerId", params: { workerId: w.id } })}
          rowClassName={(w) =>
            w.status === "stale" || w.status === "disconnected" ? "bg-warning/5" : undefined
          }
          columns={[
            {
              key: "id",
              header: "Worker ID",
              sort: (w) => w.id,
              cell: (w) => <Mono className="text-primary">{w.id}</Mono>,
            },
            {
              key: "host",
              header: "Hostname",
              sort: (w) => w.hostname,
              cell: (w) => <Mono>{w.hostname}</Mono>,
            },
            {
              key: "st",
              header: "Status",
              sort: (w) => w.status,
              cell: (w) => <StatusBadge status={w.status} />,
            },
            {
              key: "hb",
              header: "Last heartbeat",
              sort: (w) => w.lastHeartbeatAt,
              cell: (w) => {
                const age = now - Date.parse(w.lastHeartbeatAt);
                return (
                  <span
                    className={
                      age > 30_000 ? "font-mono text-xs text-warning" : "font-mono text-xs"
                    }
                  >
                    {fmtDuration(age)} ago
                  </span>
                );
              },
            },
            {
              key: "lc",
              header: "Leases",
              align: "right",
              sort: (w) => w.leaseCount,
              cell: (w) => w.leaseCount,
            },
            {
              key: "jp",
              header: "Processing",
              align: "right",
              sort: (w) => w.jobsProcessing,
              cell: (w) => `${w.jobsProcessing}/${w.concurrency}`,
            },
            {
              key: "sa",
              header: "Started at",
              sort: (w) => w.startedAt,
              cell: (w) => <TimeAgo iso={w.startedAt} />,
            },
            {
              key: "up",
              header: "Uptime",
              align: "right",
              sort: (w) => -Date.parse(w.startedAt),
              cell: (w) => fmtDuration(now - Date.parse(w.startedAt)),
            },
            {
              key: "v",
              header: "Version",
              sort: (w) => w.version,
              cell: (w) => <Mono>{w.version}</Mono>,
            },
          ]}
        />
      </Section>
    </>
  );
}
