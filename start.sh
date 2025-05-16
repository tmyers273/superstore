#!/bin/bash
set -e  # Exit immediately if any command fails

# Get number of workers from environment variable, default to 64 if not set
WORKERS=${UVICORN_WORKER_COUNT:-8}

echo "Starting uvicorn server with $WORKERS workers..."

ls

# Limiting the number of requests per worker to stop any memory leaks from growing too much
exec uvicorn server:app --log-level debug --access-log --host 0.0.0.0 --port 8000 --workers $WORKERS --limit-max-requests 1000
