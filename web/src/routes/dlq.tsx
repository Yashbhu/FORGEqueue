import { useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { RotateCcw, Trash2, Undo2 } from "lucide-react";
import { api } from "@/api/endpoints";
import { useAction, useBulk, useDlq } from "@/api/queries";
import { DataTable } from "@/components/data-table";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { BulkBar } from "@/components/job-tables";
import { JobId, Mono, PageHeader, Section, StatCard, TimeAgo } from "@/components/shared";
import { FilterSelect, SearchInput, Toolbar } from "@/components/toolbar";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { fmtDuration } from "@/lib/format";

export const Route = createFileRoute("/dlq")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "Dead Letter Queue — ForgeQueue Console" },
      {
        name: "description",
        content: "Jobs that exhausted retries: inspect errors, retry, re-enqueue or discard.",
      },
      { property: "og:title", content: "Dead Letter Queue — ForgeQueue Console" },
      {
        property: "og:description",
        content: "Jobs that exhausted retries: inspect errors, retry, re-enqueue or discard.",
      },
    ],
  }),
  component: DlqPage,
});

function DlqPage() {
  const { data } = useDlq();
  const [sel, setSel] = useState<Set<string>>(new Set());
  const [q, setQ] = useState("");
  const [queue, setQueue] = useState("");
  const retry = useBulk(api.retryDlq, "Retried");
  const del = useBulk(api.deleteDlq, "Deleted");
  const purge = useAction(api.purgeDlq, () => "DLQ purged");
  const ids = [...sel];
  const rows = data?.filter(
    (d) =>
      (!queue || d.originalQueue === queue) &&
      (d.jobId.includes(q) ||
        d.task.includes(q) ||
        d.lastError.toLowerCase().includes(q.toLowerCase())),
  );
  const today = data?.filter(
    (d) => new Date(d.failedAt).toDateString() === new Date().toDateString(),
  ).length;
  const queues = [...new Set(data?.map((d) => d.originalQueue) ?? [])].sort();

  return (
    <>
      <PageHeader
        title="Dead Letter Queue"
        description="Jobs that exhausted max attempts. Retrying moves them back to their original queue."
        actions={
          <ConfirmDialog
            trigger={
              <Button
                size="sm"
                variant="outline"
                className="h-7 text-xs text-destructive"
                disabled={!data?.length}
              >
                <Trash2 className="size-3" /> Purge DLQ
              </Button>
            }
            destructive
            typeToConfirm="purge dlq"
            title="Purge entire DLQ?"
            description={`This action cannot be undone. Affected: ${data?.length ?? 0} jobs across ${queues.length} queues.`}
            confirmLabel="Purge DLQ"
            onConfirm={() => purge.mutate(undefined)}
          />
        }
      />
      <div className="mb-3 grid grid-cols-3 gap-2 md:w-1/2">
        <StatCard
          label="DLQ jobs"
          value={data?.length}
          tone={data?.length ? "danger" : undefined}
        />
        <StatCard label="Queues affected" value={queues.length} />
        <StatCard label="Failures today" value={today} />
      </div>
      <Section title="Dead-lettered jobs" flush>
        <Toolbar>
          <SearchInput value={q} onChange={setQ} placeholder="Job ID, task or error" />
          <FilterSelect label="Queue" value={queue} onChange={setQueue} options={queues} />
        </Toolbar>
        <BulkBar count={sel.size} onClear={() => setSel(new Set())}>
          <Button
            variant="outline"
            size="sm"
            className="h-6 text-xs"
            onClick={() => (retry.mutate(ids), setSel(new Set()))}
          >
            <RotateCcw className="size-3" /> Retry selected
          </Button>
          <ConfirmDialog
            trigger={
              <Button variant="outline" size="sm" className="h-6 text-xs text-destructive">
                <Trash2 className="size-3" /> Delete selected
              </Button>
            }
            destructive
            title={`Delete ${sel.size} DLQ jobs?`}
            description={`Permanently discards ${sel.size} jobs. They will not be retried.`}
            confirmLabel={`Delete ${sel.size}`}
            onConfirm={() => (del.mutate(ids), setSel(new Set()))}
          />
        </BulkBar>
        <DataTable
          rows={rows}
          getId={(d) => d.jobId}
          selected={sel}
          onSelectedChange={setSel}
          defaultSort={{ key: "f", dir: "desc" }}
          emptyTitle="DLQ is empty"
          emptyDescription="No jobs have exhausted their retries."
          columns={[
            { key: "id", header: "Job ID", cell: (d) => <JobId id={d.jobId} /> },
            {
              key: "q",
              header: "Original queue",
              sort: (d) => d.originalQueue,
              cell: (d) => <Mono>{d.originalQueue}</Mono>,
            },
            { key: "t", header: "Task", sort: (d) => d.task, cell: (d) => <Mono>{d.task}</Mono> },
            {
              key: "a",
              header: "Attempts",
              align: "right",
              sort: (d) => d.attempts,
              cell: (d) => d.attempts,
            },
            {
              key: "e",
              header: "Last error",
              className: "max-w-[380px]",
              cell: (d) => (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <span className="block truncate font-mono text-xs text-destructive">
                      {d.lastError}
                    </span>
                  </TooltipTrigger>
                  <TooltipContent className="max-w-md font-mono text-xs">
                    {d.lastError}
                  </TooltipContent>
                </Tooltip>
              ),
            },
            {
              key: "f",
              header: "Failed at",
              sort: (d) => d.failedAt,
              cell: (d) => <TimeAgo iso={d.failedAt} />,
            },
            {
              key: "age",
              header: "Age",
              align: "right",
              sort: (d) => -Date.parse(d.failedAt),
              cell: (d) => fmtDuration(Date.now() - Date.parse(d.failedAt)),
            },
            {
              key: "act",
              header: "Actions",
              align: "right",
              cell: (d) => (
                <span className="inline-flex gap-0.5">
                  <Button
                    variant="ghost"
                    size="icon"
                    className="size-6"
                    aria-label="Retry"
                    title="Retry"
                    onClick={() => retry.mutate([d.jobId])}
                  >
                    <RotateCcw className="size-3" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="size-6"
                    aria-label="Re-enqueue"
                    title="Re-enqueue (reset attempts)"
                    onClick={() => retry.mutate([d.jobId])}
                  >
                    <Undo2 className="size-3" />
                  </Button>
                  <ConfirmDialog
                    trigger={
                      <Button
                        variant="ghost"
                        size="icon"
                        className="size-6 text-destructive"
                        aria-label="Delete"
                        title="Delete"
                      >
                        <Trash2 className="size-3" />
                      </Button>
                    }
                    destructive
                    title="Delete DLQ job?"
                    description={
                      <>
                        Permanently discards <code className="font-mono">{d.jobId}</code>.
                      </>
                    }
                    confirmLabel="Delete"
                    onConfirm={() => del.mutate([d.jobId])}
                  />
                </span>
              ),
            },
          ]}
        />
      </Section>
    </>
  );
}
