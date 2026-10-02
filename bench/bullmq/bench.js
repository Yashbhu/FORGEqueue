// Same-machine BullMQ comparison for the ForgeQueue benchmarks.
//
// Mirrors BenchmarkWorkerThroughputPreloaded as closely as BullMQ allows:
//  - all JOBS are inserted before timing starts (preloaded drain),
//  - a single worker processes at the given concurrency,
//  - the processor is a no-op,
//  - timing stops when every job reached the completed state.
//
// BullMQ has no batch claim: with concurrency 4 it pulls jobs one at a time,
// which is where the round-trip cost comparison comes from.
const { Queue, Worker } = require('bullmq');
const IORedis = require('ioredis');

const JOBS = parseInt(process.env.JOBS || '50000', 10);
const CONCURRENCY = parseInt(process.env.CONCURRENCY || '4', 10);

const connection = { host: '127.0.0.1', port: 6379, maxRetriesPerRequest: null };

async function main() {
  const redis = new IORedis(connection);
  const queue = new Queue('bench', { connection });

  // Clean slate so leftover state from a previous run can't skew counts.
  await redis.flushdb();

  const jobs = Array.from({ length: JOBS }, (_, i) => ({
    name: 'noop',
    data: { id: i },
  }));

  console.log(`enqueueing ${JOBS} jobs...`);
  await queue.addBulk(jobs);

  console.log(`starting worker (concurrency=${CONCURRENCY})...`);
  const startedAt = Date.now();

  const worker = new Worker('bench', async () => {}, {
    connection,
    concurrency: CONCURRENCY,
    drainDelay: 50,
  });

  const deadline = Date.now() + 180000;
  let done = 0;
  for (;;) {
    const counts = await queue.getJobCounts('wait', 'active', 'completed');
    done = counts.completed;
    const left = counts.wait + counts.active;
    if (left === 0 && done === JOBS) break;
    if (Date.now() > deadline) {
      throw new Error(
        `timeout: wait=${counts.wait} active=${counts.active} completed=${done}`,
      );
    }
    await new Promise((r) => setTimeout(r, 100));
  }

  const elapsedMs = Date.now() - startedAt;
  const perSecond = Math.round((JOBS / elapsedMs) * 1000);

  console.log(
    JSON.stringify({ jobs: JOBS, elapsedMs, jobsPerSec: perSecond }),
  );

  await worker.close();
  await queue.close();
  await redis.quit();
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});