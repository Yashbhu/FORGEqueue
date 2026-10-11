import { useMemo, useState, type ReactNode } from "react";
import { ArrowDown, ArrowUp, ChevronLeft, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { usePrefs } from "@/lib/prefs";
import { Checkbox } from "@/components/ui/checkbox";
import { Button } from "@/components/ui/button";
import { EmptyState } from "./shared";

export interface Column<T> {
  key: string;
  header: ReactNode;
  cell: (row: T) => ReactNode;
  sort?: (row: T) => string | number;
  className?: string;
  align?: "right";
}

interface Props<T> {
  rows: T[] | undefined;
  columns: Column<T>[];
  getId: (row: T) => string;
  selected?: Set<string>;
  onSelectedChange?: (s: Set<string>) => void;
  onRowClick?: (row: T) => void;
  rowClassName?: (row: T) => string | undefined;
  pageSizes?: number[];
  emptyTitle?: string;
  emptyDescription?: string;
  defaultSort?: { key: string; dir: "asc" | "desc" };
  maxHeight?: string;
}

export function DataTable<T>({
  rows,
  columns,
  getId,
  selected,
  onSelectedChange,
  onRowClick,
  rowClassName,
  pageSizes = [25, 50, 100],
  emptyTitle = "No rows",
  emptyDescription,
  defaultSort,
  maxHeight = "70vh",
}: Props<T>) {
  const { density } = usePrefs();
  const [sort, setSort] = useState(defaultSort);
  const [size, setSize] = useState<number>(pageSizes[0] ?? 25);
  const [page, setPage] = useState(0);

  const sorted = useMemo(() => {
    if (!rows) return [];
    const col = columns.find((c) => c.key === sort?.key);
    if (!col?.sort) return rows;
    const f = col.sort;
    return [...rows].sort((a, b) => {
      const x = f(a),
        y = f(b);
      const r = x < y ? -1 : x > y ? 1 : 0;
      return sort!.dir === "asc" ? r : -r;
    });
  }, [rows, sort, columns]);

  const pages = Math.max(1, Math.ceil(sorted.length / size));
  const p = Math.min(page, pages - 1);
  const view = sorted.slice(p * size, p * size + size);
  const selectable = !!onSelectedChange && !!selected;
  const allOnPage = view.length > 0 && view.every((r) => selected?.has(getId(r)));

  const toggle = (id: string) => {
    const s = new Set(selected);
    if (s.has(id)) {
      s.delete(id);
    } else {
      s.add(id);
    }
    onSelectedChange?.(s);
  };

  return (
    <div className="min-w-0">
      <div
        className="overflow-auto"
        style={{ maxHeight, ["--cell-py" as string]: density === "compact" ? "0.3rem" : "0.55rem" }}
      >
        <table className="dense-table w-full border-collapse text-[13px]">
          <thead className="sticky top-0 z-10 bg-card">
            <tr className="border-b">
              {selectable && (
                <th className="w-7">
                  <Checkbox
                    aria-label="Select all on page"
                    checked={allOnPage}
                    onCheckedChange={(v) => {
                      const s = new Set(selected);
                      view.forEach((r) => (v ? s.add(getId(r)) : s.delete(getId(r))));
                      onSelectedChange!(s);
                    }}
                  />
                </th>
              )}
              {columns.map((c) => (
                <th
                  key={c.key}
                  scope="col"
                  className={cn(c.align === "right" && "text-right", c.className)}
                  aria-sort={
                    sort?.key === c.key
                      ? sort.dir === "asc"
                        ? "ascending"
                        : "descending"
                      : undefined
                  }
                >
                  {c.sort ? (
                    <button
                      type="button"
                      className="inline-flex items-center gap-0.5 uppercase hover:text-foreground"
                      onClick={() =>
                        setSort((s) => ({
                          key: c.key,
                          dir: s?.key === c.key && s.dir === "desc" ? "asc" : "desc",
                        }))
                      }
                    >
                      {c.header}
                      {sort?.key === c.key &&
                        (sort.dir === "asc" ? (
                          <ArrowUp className="size-3" />
                        ) : (
                          <ArrowDown className="size-3" />
                        ))}
                    </button>
                  ) : (
                    c.header
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows === undefined
              ? Array.from({ length: 6 }, (_, i) => (
                  <tr key={i}>
                    <td colSpan={columns.length + (selectable ? 1 : 0)}>
                      <div className="h-3.5 animate-pulse rounded-sm bg-muted" />
                    </td>
                  </tr>
                ))
              : view.map((r) => {
                  const id = getId(r);
                  return (
                    <tr
                      key={id}
                      onClick={onRowClick ? () => onRowClick(r) : undefined}
                      className={cn(
                        onRowClick && "cursor-pointer",
                        selected?.has(id) && "bg-accent/60",
                        rowClassName?.(r),
                      )}
                    >
                      {selectable && (
                        <td onClick={(e) => e.stopPropagation()}>
                          <Checkbox
                            aria-label={`Select ${id}`}
                            checked={selected!.has(id)}
                            onCheckedChange={() => toggle(id)}
                          />
                        </td>
                      )}
                      {columns.map((c) => (
                        <td
                          key={c.key}
                          className={cn(
                            "whitespace-nowrap",
                            c.align === "right" && "text-right font-mono text-xs tabular-nums",
                            c.className,
                          )}
                        >
                          {c.cell(r)}
                        </td>
                      ))}
                    </tr>
                  );
                })}
          </tbody>
        </table>
        {rows && rows.length === 0 && (
          <EmptyState title={emptyTitle} description={emptyDescription} />
        )}
      </div>
      {rows && rows.length > (pageSizes[0] ?? 25) && (
        <div className="flex items-center justify-end gap-2 border-t px-2 py-1 text-xs text-muted-foreground">
          <label className="flex items-center gap-1">
            Rows
            <select
              className="rounded-sm border bg-background px-1 py-0.5 text-xs"
              value={size}
              onChange={(e) => (setSize(Number(e.target.value)), setPage(0))}
            >
              {pageSizes.map((s) => (
                <option key={s}>{s}</option>
              ))}
            </select>
          </label>
          <span className="font-mono">
            {p * size + 1}–{Math.min(sorted.length, (p + 1) * size)} of {sorted.length}
          </span>
          <Button
            size="icon"
            variant="ghost"
            className="size-6"
            aria-label="Previous page"
            disabled={p === 0}
            onClick={() => setPage(p - 1)}
          >
            <ChevronLeft className="size-3.5" />
          </Button>
          <Button
            size="icon"
            variant="ghost"
            className="size-6"
            aria-label="Next page"
            disabled={p >= pages - 1}
            onClick={() => setPage(p + 1)}
          >
            <ChevronRight className="size-3.5" />
          </Button>
        </div>
      )}
    </div>
  );
}
