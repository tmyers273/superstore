FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl gcc python3-dev libsqlite3-dev && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONPATH="/app:$PYTHONPATH"
ENV REDIS_HOST=host.docker.internal
ENV DB_HOST=host.docker.internal
ENV DATA_DIR=/data

VOLUME /data
VOLUME /ingest
VOLUME /done

# Expose port
EXPOSE 8000

# Copy and prepare start script
COPY start.sh .
RUN chmod +x start.sh
CMD ["uv", "run", "./start.sh"]
