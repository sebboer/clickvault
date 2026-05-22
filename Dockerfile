FROM rust:1.95-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/

RUN cargo build --release

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/target/release/clickvault /usr/local/bin/clickvault

ENTRYPOINT ["clickvault"]
