FROM rust:slim-bookworm AS chef
# `rm -f /etc/apt/apt.conf.d/docker-clean`: Debian's slim images ship that file
# with `APT::Keep-Downloaded-Packages "false";`, which makes apt delete
# downloaded `.deb` files right after install. That defeats the cache mount on
# `/var/cache/apt` — every rebuild re-downloads. Remove the config so packages
# stay in the cache mount across builds.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean # && \
#    apt-get update && apt-get install --no-install-recommends -y \
#    protobuf-compiler \
#    && rm -rf /var/lib/apt/lists/*
    # add pkg-config + libssl-dev here too if any crate pulls in
    # native-tls/openssl-sys instead of rustls
WORKDIR /app

COPY rust-toolchain.toml rust-toolchain.toml
RUN rustup show

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo install cargo-chef --locked


FROM chef AS planner
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo chef prepare --recipe-path recipe.json


FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
ENV SQLX_OFFLINE=true
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --recipe-path recipe.json

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build \
        --bin gateway \
        --bin core \
        --bin analytics \
    && mkdir -p /app/bin \
    && cp target/debug/gateway target/debug/core target/debug/analytics /app/bin/


FROM gcr.io/distroless/cc-debian13 AS gateway
COPY --from=builder /app/bin/gateway /usr/local/bin/gateway
USER nonroot:nonroot
WORKDIR /usr/local/bin
ENTRYPOINT ["/usr/local/bin/gateway"]


FROM gcr.io/distroless/cc-debian13 AS core
COPY --from=builder /app/bin/core /usr/local/bin/core
USER nonroot:nonroot
WORKDIR /usr/local/bin
ENTRYPOINT ["/usr/local/bin/core"]


FROM gcr.io/distroless/cc-debian13 AS analytics
COPY --from=builder /app/bin/analytics /usr/local/bin/analytics
USER nonroot:nonroot
WORKDIR /usr/local/bin
ENTRYPOINT ["/usr/local/bin/analytics"]
