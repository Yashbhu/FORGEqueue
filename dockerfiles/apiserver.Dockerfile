# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY proto ./proto
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags="-s -w" \
    -o /out/apiserver ./cmd/apiserver

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/apiserver /apiserver
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/apiserver"]
