FROM src-config-orchestrator-base:latest

WORKDIR /app

COPY . /app

EXPOSE 8080
EXPOSE 20000-24999
EXPOSE 25000-25999

CMD ["python", "-m", "submanager.main", "--config", "/app/config/config.yml"]
