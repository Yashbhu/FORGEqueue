import {
  keepPreviousData,
  useMutation,
  useQuery,
  useQueryClient,
  type QueryKey,
} from "@tanstack/react-query";
import { toast } from "sonner";
import { refreshMs, usePrefs } from "@/lib/prefs";
import { api, type MetricsRange } from "./endpoints";

export const keys = {
  stats: ["system", "stats"] as const,
  queues: ["queues"] as const,
  queue: (n: string) => ["queue", n] as const,
  queueJobs: (n: string) => ["queue", n, "jobs"] as const,
  job: (id: string) => ["job", id] as const,
  workers: ["workers"] as const,
  worker: (id: string) => ["worker", id] as const,
  dlq: ["dlq"] as const,
  delayed: ["delayed"] as const,
  throughput: (r: MetricsRange, q?: string) => ["metrics", "throughput", r, q ?? "all"] as const,
};

function usePoll() {
  const { autoRefresh, apiBase } = usePrefs();
  return { interval: refreshMs(autoRefresh), scope: apiBase };
}

function useQ<T>(key: QueryKey, fn: () => Promise<T>) {
  const { interval, scope } = usePoll();
  return useQuery({
    queryKey: [...key, scope],
    queryFn: fn,
    refetchInterval: interval,
    placeholderData: keepPreviousData,
    staleTime: 2_000,
    gcTime: 60_000,
    retry: 1,
  });
}

export const useStats = () => useQ(keys.stats, api.stats);
export const useQueues = () => useQ(keys.queues, api.queues);
export const useQueue = (n: string) => useQ(keys.queue(n), () => api.queue(n));
export const useQueueJobs = (n: string) => useQ(keys.queueJobs(n), () => api.queueJobs(n));
export const useJob = (id: string) => useQ(keys.job(id), () => api.job(id));
export const useWorkers = () => useQ(keys.workers, api.workers);
export const useWorker = (id: string) => useQ(keys.worker(id), () => api.worker(id));
export const useDlq = () => useQ(keys.dlq, api.dlq);
export const useDelayed = () => useQ(keys.delayed, api.delayed);
export const useThroughput = (r: MetricsRange, q?: string) =>
  useQ(keys.throughput(r, q), () => api.throughput(r, q));

/** Generic action mutation: runs fn for each id, invalidates everything, toasts. */
export function useAction<A>(fn: (arg: A) => Promise<unknown>, label: (arg: A) => string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: fn,
    onSuccess: (_d, arg) => {
      toast.success(label(arg));
      qc.invalidateQueries();
    },
    onError: (e) => toast.error(e instanceof Error ? e.message : "Action failed"),
  });
}

export function useBulk(fn: (id: string) => Promise<unknown>, verb: string) {
  return useAction(
    (ids: string[]) => Promise.all(ids.map(fn)),
    (ids) => `${verb} ${ids.length} job${ids.length === 1 ? "" : "s"}`,
  );
}
