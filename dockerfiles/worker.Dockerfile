# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY proto ./proto
ARG VERSION=v1.0.0
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w \
      -X forgequeue/internal/version.Version=${VERSION} \
      -X forgequeue/internal/version.Commit=${COMMIT} \
      -X forgequeue/internal/version.Date=${BUILD_DATE}" \
    -o /out/worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/worker /worker
USER nonroot:nonroot
EXPOSE 9100
ENTRYPOINT ["/worker"]
