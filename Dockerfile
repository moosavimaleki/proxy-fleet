# Reproducible Rust build; the runtime image deliberately contains only the
# fleet binary, Xray, Git/SSH and diagnostics required by the service.
FROM rust:1.85-bookworm AS builder

WORKDIR /build
COPY src/Cargo.toml src/Cargo.lock ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs && cargo build --release
COPY src/src ./src
COPY src/assets ./assets
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime
ARG XRAY_VERSION=26.7.11

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip git openssh-client sqlite3 \
    && curl -fsSL -o /tmp/xray.zip "https://github.com/XTLS/Xray-core/releases/download/v${XRAY_VERSION}/Xray-linux-64.zip" \
    && unzip -q /tmp/xray.zip xray -d /usr/local/bin \
    && chmod 0755 /usr/local/bin/xray \
    && rm -f /tmp/xray.zip \
    && apt-get purge -y --auto-remove unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/proxy-fleet /usr/local/bin/proxy-fleet

EXPOSE 8080
EXPOSE 20000-24999
EXPOSE 25000-25999

CMD ["/usr/local/bin/proxy-fleet", "--config", "/app/config/config.yml"]
