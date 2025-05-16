FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl gcc python3-dev libsqlite3-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies directly
RUN pip install uvicorn python-dotenv fastapi polars duckdb sqlalchemy datafusion pyarrow matplotlib pytest

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
CMD ["./start.sh"]
