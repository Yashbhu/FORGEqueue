# syntax=docker/dockerfile:1

FROM oven/bun:1 AS build
WORKDIR /src

COPY web/package.json web/bun.lock web/bunfig.toml ./
RUN bun install --frozen-lockfile

COPY web/ ./

# The API base is baked at build time. Keep it relative so the browser hits the
# web origin and the ingress can route /api to the apiserver service.
ARG VITE_API_BASE_URL=/api
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
ARG VITE_APP_VERSION=v1.0.0
ENV VITE_APP_VERSION=$VITE_APP_VERSION
ENV NITRO_PRESET=node-server
RUN bun run build

FROM node:22-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=3000
COPY --from=build /src/.output ./.output
USER node
EXPOSE 3000
CMD ["node", ".output/server/index.mjs"]
