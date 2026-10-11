# ForgeQueue Console

Web console for the [ForgeQueue](../README.md) distributed task queue. It reads
through the gRPC gateway's HTTP/JSON API and surfaces queues, workers, delayed
jobs, the dead letter queue and per-interval metrics.

## Development

You need Node.js and npm — [install with nvm](https://github.com/nvm-sh/nvm#installing-and-updating).

```sh
npm install
npm run dev
```

The dev server proxies to the gateway at `VITE_API_BASE_URL` (see `.env.example`).

## Scripts

| Script | Description |
| --- | --- |
| `npm run dev` | Start the Vite dev server |
| `npm run build` | Production build (Cloudflare Worker via nitro) |
| `npm run preview` | Preview a built bundle |
| `npm run lint` | ESLint |
| `npm run test` | Vitest |

## Built with

- TanStack Start / Router
- TypeScript
- React
- Tailwind CSS
- shadcn/ui

## Architecture notes

See [AGENTS.md](./AGENTS.md).
