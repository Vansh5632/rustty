# ============================================================
# Stage 1: Build the release binary
# ============================================================
FROM rust:1.82-bookworm AS builder

# Install system libraries required by ring and wasmtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# ---- Cache dependencies first (Docker layer caching) ----
# Copy only manifests + lock so dependency layer is cached when src changes
COPY Cargo.toml Cargo.lock ./
COPY core/Cargo.toml core/Cargo.toml
COPY schema/Cargo.toml schema/Cargo.toml
COPY storage/Cargo.toml storage/Cargo.toml
COPY query/Cargo.toml query/Cargo.toml
COPY wasm/Cargo.toml wasm/Cargo.toml
COPY server/Cargo.toml server/Cargo.toml

# Create stub lib.rs / main.rs for each crate so cargo can resolve the workspace
RUN mkdir -p src core/src schema/src storage/src query/src wasm/src server/src && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > core/src/lib.rs && \
    echo "" > schema/src/lib.rs && \
    echo "" > storage/src/lib.rs && \
    echo "" > query/src/lib.rs && \
    echo "" > wasm/src/lib.rs && \
    echo "fn main() {}" > server/src/main.rs

# Pre-build dependencies (this layer is cached unless Cargo.toml/lock changes)
RUN cargo build --release 2>/dev/null || true

# ---- Now copy the real source and rebuild ----
COPY . .
# Touch all source files so cargo rebuilds them (not the cached deps)
RUN find . -name "*.rs" -exec touch {} +
RUN cargo build --release --bin Rustdb

# ============================================================
# Stage 2: Minimal runtime image
# ============================================================
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for security
RUN groupadd -r rustdb && useradd -r -g rustdb -m rustdb

# Copy the release binary
COPY --from=builder /build/target/release/Rustdb /usr/local/bin/rustdb

# Create the default data directory
RUN mkdir -p /data && chown rustdb:rustdb /data

USER rustdb
WORKDIR /home/rustdb

# Data directory as a volume for persistence
VOLUME ["/data"]

ENV RUSTDB_DATA_DIR=/data

ENTRYPOINT ["rustdb"]
CMD ["--data-dir", "/data"]
