# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM golang:1.26-bookworm AS build
WORKDIR /src

# Cache module downloads separately from source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/operator ./cmd/operator

# ---- Runtime stage ----
# Distroless static: no shell, no package manager, non-root by default.
# The agent only needs CA roots for HTTPS to the control plane, which
# distroless includes.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/operator /operator
USER nonroot:nonroot
ENTRYPOINT ["/operator"]
