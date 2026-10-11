import { useState } from "react";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { Pause, Play } from "lucide-react";
import { DensityToggle, FilterSelect, SearchInput, Toolbar } from "@/components/toolbar";
import { useAction, useQueues } from "@/api/queries";
import { api } from "@/api/endpoints";
import { DataTable } from "@/components/data-table";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { Mono, PageHeader, Section, StatusBadge, TimeAgo } from "@/components/shared";
import { Button } from "@/components/ui/button";
import { fmtNum, fmtRate } from "@/lib/format";
import type { QueueStatus } from "@/types/forgequeue";

export const Route = createFileRoute("/queues/")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "Queues — ForgeQueue Console" },
      {
        name: "description",
        content: "All ForgeQueue queues with depth, processing, failures and rate.",
      },
      { property: "og:title", content: "Queues — ForgeQueue Console" },
      {
        property: "og:description",
        content: "All ForgeQueue queues with depth, processing, failures and rate.",
      },
    ],
  }),
  component: QueuesList,
});

function QueuesList() {
  const { data } = useQueues();
  const [q, setQ] = useState("");
  const [status, setStatus] = useState("");
  const navigate = useNavigate();
  const pause = useAction(api.pauseQueue, (n) => `Paused queue ${n}`);
  const resume = useAction(api.resumeQueue, (n) => `Resumed queue ${n}`);
  const rows = data?.filter((x) => x.name.includes(q) && (!status || x.status === status));

  return (
    <>
      <PageHeader title="Queues" description={data ? `${data.length} queues` : "Loading…"} />
      <Section title="All queues" flush>
        <Toolbar>
          <SearchInput value={q} onChange={setQ} placeholder="Filter by name" />
          <FilterSelect
            label="Status"
            value={status}
            onChange={setStatus}
            options={["healthy", "paused", "degraded", "unknown"] satisfies QueueStatus[]}
          />
          <div className="ml-auto">
            <DensityToggle />
          </div>
        </Toolbar>
        <DataTable
          rows={rows}
          getId={(x) => x.name}
          onRowClick={(x) => navigate({ to: "/queues/$queueName", params: { queueName: x.name } })}
          defaultSort={{ key: "pending", dir: "desc" }}
          emptyTitle="No queues match"
          columns={[
            {
              key: "name",
              header: "Queue",
              sort: (x) => x.name,
              cell: (x) => <Mono className="text-primary">{x.name}</Mono>,
            },
            {
              key: "pending",
              header: "Pending",
              align: "right",
              sort: (x) => x.pending,
              cell: (x) => fmtNum(x.pending),
            },
            {
              key: "processing",
              header: "Processing",
              align: "right",
              sort: (x) => x.processing,
              cell: (x) => fmtNum(x.processing),
            },
            {
              key: "delayed",
              header: "Delayed",
              align: "right",
              sort: (x) => x.delayed,
              cell: (x) => fmtNum(x.delayed),
            },
            {
              key: "failed",
              header: "Failed",
              align: "right",
              sort: (x) => x.failed,
              cell: (x) => (
                <span className={x.failed ? "text-destructive" : ""}>{fmtNum(x.failed)}</span>
              ),
            },
            {
              key: "workers",
              header: "Workers",
              align: "right",
              sort: (x) => x.workers,
              cell: (x) => x.workers,
            },
            {
              key: "rate",
              header: "Rate",
              align: "right",
              sort: (x) => x.ratePerMin,
              cell: (x) => fmtRate(x.ratePerMin),
            },
            {
              key: "oldest",
              header: "Oldest",
              sort: (x) => x.oldestJobAt ?? "",
              cell: (x) => <TimeAgo iso={x.oldestJobAt} />,
            },
            {
              key: "status",
              header: "Status",
              sort: (x) => x.status,
              cell: (x) => <StatusBadge status={x.status} />,
            },
            {
              key: "actions",
              header: "Actions",
              align: "right",
              cell: (x) => (
                <span onClick={(e) => e.stopPropagation()} className="inline-flex gap-1">
                  {x.status === "paused" ? (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 px-1.5 text-xs"
                      onClick={() => resume.mutate(x.name)}
                    >
                      <Play className="size-3" /> Resume
                    </Button>
                  ) : (
                    <ConfirmDialog
                      trigger={
                        <Button variant="ghost" size="sm" className="h-6 px-1.5 text-xs">
                          <Pause className="size-3" /> Pause
                        </Button>
                      }
                      title={`Pause queue '${x.name}'?`}
                      description={`Workers will stop claiming new jobs. ${fmtNum(x.pending)} pending jobs will wait until resumed. In-flight jobs continue.`}
                      confirmLabel="Pause queue"
                      onConfirm={() => pause.mutate(x.name)}
                    />
                  )}
                </span>
              ),
            },
          ]}
        />
      </Section>
    </>
  );
}
