import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { format } from "date-fns";
import type { MetricsPoint } from "@/types/forgequeue";

export interface SeriesDef {
  key: keyof MetricsPoint;
  label: string;
  color: string;
}

export const THROUGHPUT: SeriesDef[] = [
  { key: "enqueued", label: "enqueued", color: "var(--chart-1)" },
  { key: "completed", label: "completed", color: "var(--chart-2)" },
  { key: "failed", label: "failed", color: "var(--chart-3)" },
];

const axis = {
  stroke: "var(--muted-foreground)",
  fontSize: 10,
  tickLine: false,
  axisLine: false,
} as const;

export function TimeChart({
  data,
  series,
  height = 180,
  area,
}: {
  data: MetricsPoint[] | undefined;
  series: SeriesDef[];
  height?: number;
  area?: boolean | undefined;
}) {
  if (!data) return <div className="animate-pulse rounded-sm bg-muted/50" style={{ height }} />;
  const C = area ? AreaChart : LineChart;
  return (
    <div style={{ height }}>
      <ResponsiveContainer width="100%" height="100%">
        <C data={data} margin={{ top: 4, right: 4, bottom: 0, left: -18 }}>
          <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" vertical={false} />
          <XAxis
            dataKey="time"
            {...axis}
            tickFormatter={(t) => format(new Date(t), "HH:mm")}
            minTickGap={40}
          />
          <YAxis {...axis} width={44} />
          <Tooltip
            contentStyle={{
              background: "var(--popover)",
              border: "1px solid var(--border)",
              borderRadius: 4,
              fontSize: 11,
              fontFamily: "var(--font-mono)",
            }}
            labelFormatter={(t) => format(new Date(t), "yyyy-MM-dd HH:mm:ss")}
          />
          {series.map((s) =>
            area ? (
              <Area
                key={s.key}
                type="monotone"
                dataKey={s.key}
                name={s.label}
                stroke={s.color}
                fill={s.color}
                fillOpacity={0.12}
                strokeWidth={1.5}
                isAnimationActive={false}
              />
            ) : (
              <Line
                key={s.key}
                type="monotone"
                dataKey={s.key}
                name={s.label}
                stroke={s.color}
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
              />
            ),
          )}
        </C>
      </ResponsiveContainer>
    </div>
  );
}

export function Legend({ series }: { series: SeriesDef[] }) {
  return (
    <div className="flex items-center gap-2.5 text-[11px] text-muted-foreground">
      {series.map((s) => (
        <span key={s.key} className="flex items-center gap-1 font-mono">
          <span className="h-0.5 w-2.5" style={{ background: s.color }} />
          {s.label}
        </span>
      ))}
    </div>
  );
}
