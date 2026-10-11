import { useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { useDelayed } from "@/api/queries";
import { DelayedTable } from "@/components/job-tables";
import { PageHeader, Section } from "@/components/shared";
import { FilterSelect, Toolbar } from "@/components/toolbar";

export const Route = createFileRoute("/delayed")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "Delayed / Scheduled — ForgeQueue Console" },
      {
        name: "description",
        content: "Jobs waiting on a scheduled time or retry backoff, ordered by run time.",
      },
      { property: "og:title", content: "Delayed / Scheduled — ForgeQueue Console" },
      {
        property: "og:description",
        content: "Jobs waiting on a scheduled time or retry backoff, ordered by run time.",
      },
    ],
  }),
  component: DelayedPage,
});

function DelayedPage() {
  const { data } = useDelayed();
  const [queue, setQueue] = useState("");
  const [task, setTask] = useState("");
  const rows = data?.filter((d) => (!queue || d.queue === queue) && (!task || d.task === task));
  const uniq = (k: "queue" | "task") => [...new Set(data?.map((d) => d[k]) ?? [])].sort();
  return (
    <>
      <PageHeader
        title="Delayed / Scheduled"
        description={data ? `${data.length} jobs in the delayed ZSET, soonest first.` : "Loading…"}
      />
      <Section title="Scheduled jobs" flush>
        <Toolbar>
          <FilterSelect label="Queue" value={queue} onChange={setQueue} options={uniq("queue")} />
          <FilterSelect label="Task" value={task} onChange={setTask} options={uniq("task")} />
        </Toolbar>
        <DelayedTable rows={rows} />
      </Section>
    </>
  );
}
