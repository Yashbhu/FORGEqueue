import { createFileRoute, Link } from "@tanstack/react-router";
import { Pause, Play, Trash2 } from "lucide-react";
import { api } from "@/api/endpoints";
import { useAction, useDelayed, useQueue, useQueueJobs, useThroughput } from "@/api/queries";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { DelayedTable, JobsTable } from "@/components/job-tables";
import { Legend, THROUGHPUT, TimeChart } from "@/components/charts";
import { EmptyState, Mono, PageHeader, Section, StatCard, StatusBadge } from "@/components/shared";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { fmtNum } from "@/lib/format";

export const Route = createFileRoute("/queues/$queueName")({
  ssr: false,
  head: ({ params }) => ({
    meta: [
      { title: `Queue ${params.queueName} — ForgeQueue Console` },
      {
        name: "description",
        content: `Inspect jobs, delays, failures and metrics for queue ${params.queueName}.`,
      },
      { property: "og:title", content: `Queue ${params.queueName} — ForgeQueue Console` },
      {
        property: "og:description",
        content: `Inspect jobs, delays, failures and metrics for queue ${params.queueName}.`,
      },
    ],
  }),
  component: QueueDetail,
});

function QueueDetail() {
  const { queueName } = Route.useParams();
  const { data: q, error } = useQueue(queueName);
  const jobs = useQueueJobs(queueName).data;
  const delayed = useDelayed().data?.filter((d) => d.queue === queueName);
  const tp = useThroughput("1h", queueName).data;
  const pause = useAction(api.pauseQueue, (n) => `Paused queue ${n}`);
  const resume = useAction(api.resumeQueue, (n) => `Resumed queue ${n}`);
  const purge = useAction(api.purgeQueue, (n) => `Purged pending jobs from ${n}`);

  if (error)
    return <EmptyState title={`Queue '${queueName}' not found`} description={error.message} />;

  const failures = jobs?.filter((j) => j.status === "failed" || j.status === "retrying");
  return (
    <>
      <div className="mb-1 text-xs text-muted-foreground">
        <Link to="/queues" className="hover:text-foreground">
          Queues
        </Link>{" "}
        / <Mono>{queueName}</Mono>
      </div>
      <PageHeader
        title={
          <>
            <Mono className="text-base">{queueName}</Mono> {q && <StatusBadge status={q.status} />}
          </>
        }
        actions={
          q && (
            <>
              {q.status === "paused" ? (
                <Button
                  size="sm"
                  variant="outline"
                  className="h-7 text-xs"
                  onClick={() => resume.mutate(queueName)}
                >
                  <Play className="size-3" /> Resume
                </Button>
              ) : (
                <ConfirmDialog
                  trigger={
                    <Button size="sm" variant="outline" className="h-7 text-xs">
                      <Pause className="size-3" /> Pause
                    </Button>
                  }
                  title={`Pause queue '${queueName}'?`}
                  description={`Workers stop claiming. ${fmtNum(q.pending)} pending jobs will wait.`}
                  confirmLabel="Pause queue"
                  onConfirm={() => pause.mutate(queueName)}
                />
              )}
              <ConfirmDialog
                trigger={
                  <Button size="sm" variant="outline" className="h-7 text-xs text-destructive">
                    <Trash2 className="size-3" /> Purge
                  </Button>
                }
                destructive
                typeToConfirm={queueName}
                title={`Purge queue '${queueName}'?`}
                description={`This will permanently remove ${fmtNum(q.pending)} pending jobs. Processing and delayed jobs are not affected.`}
                confirmLabel="Purge pending"
                onConfirm={() => purge.mutate(queueName)}
              />
            </>
          )
        }
      />
      <div className="mb-3 grid grid-cols-3 gap-2 md:grid-cols-6">
        <StatCard label="Pending" value={q?.pending} />
        <StatCard label="Processing" value={q?.processing} tone="info" />
        <StatCard label="Delayed" value={q?.delayed} />
        <StatCard label="Failed" value={q?.failed} tone={q?.failed ? "danger" : undefined} />
        <StatCard label="Completed" value={q?.completed} tone="success" />
        <StatCard label="Active workers" value={q?.workers} />
      </div>
      <Tabs defaultValue="jobs">
        <TabsList className="h-8">
          <TabsTrigger value="jobs" className="text-xs">
            Jobs{" "}
            {jobs && <span className="ml-1 font-mono text-muted-foreground">{jobs.length}</span>}
          </TabsTrigger>
          <TabsTrigger value="delayed" className="text-xs">
            Delayed
          </TabsTrigger>
          <TabsTrigger value="failures" className="text-xs">
            Failures
          </TabsTrigger>
          <TabsTrigger value="metrics" className="text-xs">
            Metrics
          </TabsTrigger>
        </TabsList>
        <TabsContent value="jobs">
          <Section title="Jobs (sample of tracked jobs)" flush>
            <JobsTable jobs={jobs} />
          </Section>
        </TabsContent>
        <TabsContent value="delayed">
          <Section title="Delayed / scheduled" flush>
            <DelayedTable rows={delayed} showQueue={false} />
          </Section>
        </TabsContent>
        <TabsContent value="failures">
          <Section title="Recent failures" flush>
            <JobsTable jobs={failures} />
          </Section>
        </TabsContent>
        <TabsContent value="metrics">
          <Section title="Throughput · last 1h" actions={<Legend series={THROUGHPUT} />}>
            <TimeChart data={tp} series={THROUGHPUT} height={220} />
          </Section>
        </TabsContent>
      </Tabs>
    </>
  );
}
