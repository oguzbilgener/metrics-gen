FROM rust:1.81-alpine as base
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN apk update && \
    apk add --no-cache openssh git build-base musl-dev openssl perl protobuf-dev && \
    mkdir -p ~/.ssh && \
    ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts
WORKDIR /usr/src/app

# ----------------------------------------
FROM base as dev
RUN apk update && \
    apk add --no-cache bash && \
    rustup component add clippy

# ----------------------------------------
FROM base as chef
# Cargo chef is used to speed up our builds
RUN cargo install cargo-chef

# ----------------------------------------
# Work out which dependencies we need with cargo-chef.
# See https://github.com/LukeMathWalker/cargo-chef#how-to-use.
FROM chef AS planner
COPY . .
RUN --mount=type=ssh \
    cargo chef prepare --recipe-path recipe.json

# ----------------------------------------
FROM chef AS build
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
# Build dependencies.
COPY --from=planner /usr/src/app/recipe.json recipe.json
RUN --mount=type=ssh \
    cargo chef cook --release --recipe-path recipe.json

# Build the application
COPY . .
RUN --mount=type=ssh \
    RUST_BACKTRACE=1 cargo build --release

# ----------------------------------------

FROM alpine:3.17
COPY --from=build /usr/src/app/target/x86_64-unknown-linux-musl/release/metrics-gen /metrics-gen

ENTRYPOINT ["/metrics-gen"]
CMD ["-c", "/etc/metrics-gen/config.yaml", "realtime"]


