#!/bin/bash
set -e  # Exit immediately if any command fails

echo "Running database migrations..."
# Run Alembic migrations to ensure database is up to date
uv run alembic upgrade head

# Get number of workers from environment variable, default to 1 for debugging
WORKERS=${UVICORN_WORKER_COUNT:-1}

echo "Starting uvicorn server with $WORKERS workers..."

# Limiting the number of requests per worker to stop any memory leaks from growing too much
exec python -m uvicorn server:app --log-level debug --access-log --host 0.0.0.0 --port 8000 --workers $WORKERS --limit-max-requests 1000
