# ---- Build nvidia-mig-parted ----
FROM golang:1.25 AS mig-parted-builder

# Defaults so plain `docker build` works (buildx will override)
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG MIG_PARTED_VERSION=latest

ENV GOBIN=/out

# Ensure module fetch works reliably
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN CGO_ENABLED=1 go install github.com/NVIDIA/mig-parted/cmd/nvidia-mig-parted@${MIG_PARTED_VERSION}


# ---- Build ome-mig-manager ----
FROM golang:1.25 AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd/ cmd/
COPY internal/ internal/
COPY pkg/ pkg/

ARG GIT_TAG
ARG GIT_COMMIT

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
    -ldflags "-s -w -X github.com/sgl-project/ome/pkg/version.GitVersion=${GIT_TAG} -X github.com/sgl-project/ome/pkg/version.GitCommit=${GIT_COMMIT}" \
    -o /out/ome-mig-manager ./cmd/mig-manager


# ---- Runtime image (glibc new enough, with ldconfig) ----
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      libc6 \
      libc-bin \
      libgcc-s1 \
      libstdc++6 \
      libssl3 \
      bash \
      coreutils && \
    rm -rf /var/lib/apt/lists/*

COPY --from=mig-parted-builder /out/nvidia-mig-parted /usr/local/bin/nvidia-mig-parted
COPY --from=builder /out/ome-mig-manager /usr/local/bin/ome-mig-manager

# Copy-only-NVML bootstrap (avoid poisoning with host libc)
RUN cat > /usr/local/bin/nvml-bootstrap.sh <<'EOF' && chmod +x /usr/local/bin/nvml-bootstrap.sh
#!/usr/bin/env bash
set -euo pipefail

NVML_DST="/opt/nvidia-libs"
mkdir -p "${NVML_DST}"

NVML_SRC=""
if compgen -G "/host/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.*" > /dev/null; then
  NVML_SRC="$(ls -1 /host/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.* | head -n1)"
elif compgen -G "/usr/local/nvidia/lib64/libnvidia-ml.so.*" > /dev/null; then
  NVML_SRC="$(ls -1 /usr/local/nvidia/lib64/libnvidia-ml.so.* | head -n1)"
fi

if [[ -z "${NVML_SRC}" ]]; then
  echo "WARNING: NVML library not found. Expected one of:" >&2
  echo "  /host/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.*" >&2
  echo "  /usr/local/nvidia/lib64/libnvidia-ml.so.*" >&2
  exec /usr/local/bin/ome-mig-manager
fi

cp -f "${NVML_SRC}" "${NVML_DST}/"
NVML_BASENAME="$(basename "${NVML_SRC}")"
ln -sf "${NVML_DST}/${NVML_BASENAME}" "${NVML_DST}/libnvidia-ml.so.1"
ln -sf "${NVML_DST}/libnvidia-ml.so.1" "${NVML_DST}/libnvidia-ml.so"

echo "${NVML_DST}" > /etc/ld.so.conf.d/zz-nvidia-nvml.conf
ldconfig

exec /usr/local/bin/ome-mig-manager
EOF

ENTRYPOINT ["/usr/local/bin/nvml-bootstrap.sh"]
