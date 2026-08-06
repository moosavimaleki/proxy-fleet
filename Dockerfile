# Reproducible Rust build; the runtime image deliberately contains only the
# fleet binary, Xray, Git/SSH and diagnostics required by the service.
ARG RUNTIME_BASE=runtime-network
ARG XRAY_VERSION=26.3.27
FROM rust:1.85-bookworm AS builder

WORKDIR /build
COPY src/Cargo.toml src/Cargo.lock ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs && cargo build --release
COPY src/src ./src
COPY src/assets ./assets
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime-network
ARG XRAY_VERSION

# Debian's default HTTP mirror is routinely blocked or intercepted on Iranian
# networks.  Force HTTPS before the first index fetch; this is deterministic
# and leaves no host-specific mirror hard-coded in the image.
RUN set -eux; \
    for source in /etc/apt/sources.list /etc/apt/sources.list.d/debian.sources; do \
        if [ -f "$source" ]; then sed -i 's|http://deb.debian.org|https://deb.debian.org|g' "$source"; fi; \
    done; \
    apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip git openssh-client sqlite3 \
    && curl -fsSL -o /tmp/xray.zip "https://github.com/XTLS/Xray-core/releases/download/v${XRAY_VERSION}/Xray-linux-64.zip" \
    && unzip -q /tmp/xray.zip xray -d /usr/local/bin \
    && chmod 0755 /usr/local/bin/xray \
    && rm -f /tmp/xray.zip \
    && apt-get purge -y --auto-remove unzip \
    && rm -rf /var/lib/apt/lists/*

FROM src-config-orchestrator:latest AS runtime-cached

# A normal installation uses runtime-network.  The local fallback is used by
# compose on this host because BuildKit cannot reach Debian mirrors, while the
# existing production image already contains curl, git, SQLite and Xray.
FROM ${RUNTIME_BASE} AS runtime
ARG XRAY_VERSION

RUN /usr/local/bin/xray version | grep -F "Xray ${XRAY_VERSION}" \
    && groupadd --gid 1000 app \
    && useradd --uid 1000 --gid app --create-home --shell /usr/sbin/nologin app \
    && install -d -o app -g app -m 0700 /home/app/.ssh

WORKDIR /app
COPY --from=builder /build/target/release/proxy-fleet /usr/local/bin/proxy-fleet
USER app

EXPOSE 8080
EXPOSE 20000-24999
EXPOSE 25000-25999

CMD ["/usr/local/bin/proxy-fleet", "--config", "/app/config/config.yml"]
