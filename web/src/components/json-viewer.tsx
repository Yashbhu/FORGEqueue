import { useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { CopyButton } from "./shared";

function Node({
  k,
  v,
  depth,
  open,
}: {
  k?: string | undefined;
  v: unknown;
  depth: number;
  open: boolean;
}) {
  const [expanded, setExpanded] = useState(open || depth < 2);
  const isObj = v !== null && typeof v === "object";
  const keyEl = k !== undefined && <span className="text-info">"{k}"</span>;
  if (!isObj) {
    const cls =
      typeof v === "string"
        ? "text-success"
        : typeof v === "number"
          ? "text-warning"
          : "text-muted-foreground";
    return (
      <div className="group flex items-center gap-1" style={{ paddingLeft: depth * 12 }}>
        {keyEl}
        {keyEl && ": "}
        <span className={cn("break-all", cls)}>{JSON.stringify(v)}</span>
        <span className="opacity-0 group-hover:opacity-100">
          <CopyButton value={typeof v === "string" ? v : JSON.stringify(v)} label="Copy value" />
        </span>
      </div>
    );
  }
  const entries = Array.isArray(v)
    ? v.map((x, i) => [String(i), x] as const)
    : Object.entries(v as object);
  const [o, c] = Array.isArray(v) ? ["[", "]"] : ["{", "}"];
  return (
    <div>
      <button
        type="button"
        className="flex items-center gap-0.5 hover:bg-accent/50"
        style={{ paddingLeft: depth * 12 }}
        onClick={() => setExpanded(!expanded)}
        aria-expanded={expanded}
      >
        {expanded ? <ChevronDown className="size-3" /> : <ChevronRight className="size-3" />}
        {keyEl}
        {keyEl && ": "}
        {o}
        {!expanded && (
          <span className="text-muted-foreground">
            {" "}
            {entries.length} … {c}
          </span>
        )}
      </button>
      {expanded && (
        <>
          {entries.map(([ck, cv]) => (
            <Node
              key={ck}
              k={Array.isArray(v) ? undefined : ck}
              v={cv}
              depth={depth + 1}
              open={open}
            />
          ))}
          <div style={{ paddingLeft: depth * 12 + 14 }}>{c}</div>
        </>
      )}
    </div>
  );
}

export function JsonViewer({ value }: { value: unknown }) {
  const [raw, setRaw] = useState(false);
  const [gen, setGen] = useState(0);
  const [allOpen, setAllOpen] = useState(false);
  const text = JSON.stringify(value, null, 2);
  return (
    <div className="rounded-sm border bg-background">
      <div className="flex items-center gap-1 border-b px-1.5 py-1 text-[11px]">
        {(["tree", "raw"] as const).map((m) => (
          <button
            key={m}
            type="button"
            onClick={() => setRaw(m === "raw")}
            className={cn(
              "rounded-sm px-1.5 py-0.5",
              (m === "raw") === raw
                ? "bg-accent text-foreground"
                : "text-muted-foreground hover:text-foreground",
            )}
          >
            {m === "tree" ? "Pretty" : "Raw"}
          </button>
        ))}
        {!raw && (
          <button
            type="button"
            className="ml-1 text-muted-foreground hover:text-foreground"
            onClick={() => (setAllOpen(!allOpen), setGen(gen + 1))}
          >
            {allOpen ? "Collapse all" : "Expand all"}
          </button>
        )}
        <span className="ml-auto flex items-center gap-1 text-muted-foreground">
          {text?.length ?? 0} B <CopyButton value={text ?? ""} label="Copy JSON" />
        </span>
      </div>
      <div className="max-h-80 overflow-auto p-2 font-mono text-[12px] leading-5">
        {raw ? <pre>{text}</pre> : <Node key={gen} v={value} depth={0} open={allOpen} />}
      </div>
    </div>
  );
}
