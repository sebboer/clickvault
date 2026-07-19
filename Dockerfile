FROM rust:1.97-alpine AS chef
RUN apk add --no-cache musl-dev && cargo install cargo-chef
WORKDIR /build

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/target/release/clickvault /usr/local/bin/clickvault

ENTRYPOINT ["clickvault"]
