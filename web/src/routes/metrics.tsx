import { useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { useThroughput } from "@/api/queries";
import type { MetricsRange } from "@/api/endpoints";
import { Legend, TimeChart, type SeriesDef } from "@/components/charts";
import { PageHeader, Section } from "@/components/shared";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/metrics")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "Metrics — ForgeQueue Console" },
      {
        name: "description",
        content: "Throughput, depth, latency, retries, DLQ growth and lease expirations over time.",
      },
      { property: "og:title", content: "Metrics — ForgeQueue Console" },
      {
        property: "og:description",
        content: "Throughput, depth, latency, retries, DLQ growth and lease expirations over time.",
      },
    ],
  }),
  component: Metrics,
});

const RANGES: MetricsRange[] = ["5m", "15m", "1h", "6h", "24h", "7d"];
const s = (key: SeriesDef["key"], label: string, color: string): SeriesDef => ({
  key,
  label,
  color,
});
const CHARTS: { title: string; series: SeriesDef[]; area?: boolean }[] = [
  { title: "Jobs enqueued", series: [s("enqueued", "enqueued", "var(--chart-1)")], area: true },
  { title: "Jobs completed", series: [s("completed", "completed", "var(--chart-2)")], area: true },
  { title: "Jobs failed", series: [s("failed", "failed", "var(--chart-3)")], area: true },
  { title: "Queue depth (total pending)", series: [s("depth", "depth", "var(--chart-1)")] },
  {
    title: "Processing latency (avg ms, enqueue→claim)",
    series: [s("latencyMs", "latency_ms", "var(--chart-4)")],
  },
  { title: "Job execution latency (avg ms)", series: [s("execMs", "exec_ms", "var(--chart-5)")] },
  { title: "Retries", series: [s("retries", "retries", "var(--chart-4)")] },
  { title: "DLQ size", series: [s("dlq", "dlq", "var(--chart-3)")] },
  { title: "Worker count", series: [s("workers", "workers", "var(--chart-2)")] },
  { title: "Lease expirations", series: [s("leaseExpirations", "expired", "var(--chart-3)")] },
];

function Metrics() {
  const [range, setRange] = useState<MetricsRange>("1h");
  const { data } = useThroughput(range);
  return (
    <>
      <PageHeader
        title="Metrics"
        description="Per-interval aggregates from the metrics endpoint."
        actions={
          <div
            className="flex items-center rounded-sm border p-px"
            role="radiogroup"
            aria-label="Time range"
          >
            {RANGES.map((r) => (
              <button
                key={r}
                role="radio"
                aria-checked={range === r}
                onClick={() => setRange(r)}
                className={cn(
                  "rounded-[2px] px-2 py-0.5 font-mono text-xs",
                  range === r
                    ? "bg-accent text-foreground"
                    : "text-muted-foreground hover:text-foreground",
                )}
              >
                {r}
              </button>
            ))}
          </div>
        }
      />
      <div className="grid gap-3 md:grid-cols-2">
        {CHARTS.map((c) => (
          <Section key={c.title} title={c.title} actions={<Legend series={c.series} />}>
            <TimeChart data={data} series={c.series} area={c.area} height={150} />
          </Section>
        ))}
      </div>
    </>
  );
}
