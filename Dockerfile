        FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

        RUN apt-get update && apt-get install -y curl gcc python3-dev libsqlite3-dev && \
            rm -rf /var/lib/apt/lists/*

        # Copy from the cache instead of linking since it's a mounted volume
        ENV UV_LINK_MODE=copy

        WORKDIR /app

        # COPY requirements.txt .

        # Create virtual environment first and install dependencies into it
        RUN --mount=type=cache,target=/root/.cache/uv \
            --mount=type=bind,source=uv.lock,target=uv.lock \
            --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
            uv venv && \
            . .venv/bin/activate && \
            uv sync --frozen --no-install-project --no-dev

        COPY pyproject.toml uv.lock .

        # Install the project and uvicorn in the virtual environment
        RUN . .venv/bin/activate && \
            uv sync --frozen --no-dev && \
            uv add uvicorn && \
            uv add alembic

        # Reset the entrypoint, don't invoke `uv`
        ENTRYPOINT []

        EXPOSE 8000

        ENV PATH="/app/.venv/bin:$PATH"
        ENV REDIS_HOST=host.docker.internal
        ENV DB_HOST=host.docker.internal

        # VOLUME ["/app/laravel", "/data", "/ams", "/raw-ams-cache", "/duckdb", "/exports", "/postgres-dumps"]

        # Copy application code
        COPY . .
        COPY . .

        COPY start.sh .
        RUN chmod +x start.sh
        CMD ["./start.sh"]
        # CMD ["alembic", "upgrade", "head", "&&", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
