ARG RUST_VERSION=1.94
ARG ALPINE_VERSION=3.20

FROM rust:$RUST_VERSION-alpine$ALPINE_VERSION AS builder
RUN apk add --no-cache bash build-base mold musl-dev openssl-dev yq

FROM builder AS core-builder
WORKDIR /usr/src/same
ENV RUSTFLAGS="-Ctarget-feature=-crt-static -Clink-arg=-fuse-ld=mold"
COPY . .
RUN cargo -v build --release

# Bundle Stage
FROM alpine:$ALPINE_VERSION
RUN apk add --no-cache ca-certificates libgcc
COPY --from=core-builder /usr/src/same/target/release/same /usr/bin/same
ENTRYPOINT ["/usr/bin/same"]