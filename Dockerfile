FROM src-config-orchestrator-base:latest

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY . /app

EXPOSE 8080
EXPOSE 20000-24999
EXPOSE 25000-25999

CMD ["python", "-m", "submanager.main", "--config", "/app/config/config.yml"]
