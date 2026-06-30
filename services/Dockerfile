# Demo image for lib-service-discovery (svc-a / svc-b / svc-c share one binary).
# lib-observability resolves from the public Go proxy, so no build secret is needed.
# syntax=docker/dockerfile:1
FROM golang:1.26 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /bin/services ./services

FROM gcr.io/distroless/static-debian12
COPY --from=build /bin/services /services
ENTRYPOINT ["/services"]
