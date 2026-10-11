const nf = new Intl.NumberFormat();

export const fmtNum = (n: number) => nf.format(n);

export function fmtRate(perMin: number) {
  if (perMin >= 1000) return `${(perMin / 1000).toFixed(1)}k/min`;
  return `${perMin}/min`;
}

export function fmtDuration(ms: number | null | undefined) {
  if (ms == null) return "—";
  if (ms < 0) ms = 0;
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)}s`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ${Math.floor((ms % 60_000) / 1000)}s`;
  if (ms < 86_400_000)
    return `${Math.floor(ms / 3_600_000)}h ${Math.floor((ms % 3_600_000) / 60_000)}m`;
  return `${Math.floor(ms / 86_400_000)}d ${Math.floor((ms % 86_400_000) / 3_600_000)}h`;
}

export const shortId = (id: string, n = 8) => (id.length > n ? id.slice(0, n) : id);
