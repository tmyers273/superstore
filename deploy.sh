#!/bin/bash
set -euo pipefail

export REGISTRY=ghcr.io/merchjar
export TAG=latest

# Required environment variables
if [ -z "${REGISTRY:-}" ] || [ -z "${TAG:-}" ]; then
    echo "Error: REGISTRY and TAG environment variables must be set"
    echo "Usage: REGISTRY=my.registry.com/myapp TAG=v1.2.3 ./deploy.sh"
    exit 1
fi

# Determine current active deployment
CURRENT_CONTAINER=$(docker compose ps --status running | grep -E 'blue-web|green-web' | awk '{print $1}' || true)
echo "CURRENT_CONTAINER: $CURRENT_CONTAINER"
if [[ $CURRENT_CONTAINER == *"blue"* ]]; then
    NEW_COLOR="green"
else
    NEW_COLOR="blue"
fi

echo "Deploying ${REGISTRY}:${TAG} to ${NEW_COLOR} environment..."

# Pull new image
docker compose pull "${NEW_COLOR}-web"

# Deploy new version
docker compose up --detach --wait --no-deps "${NEW_COLOR}-web"

# Stop old deployment
if [ -n "$CURRENT_CONTAINER" ]; then
    if [[ "$CURRENT_CONTAINER" == *"blue"* ]]; then
        OLD_SERVICE="blue-web"
    else
        OLD_SERVICE="green-web"
    fi
    
    echo "Draining connections for service: $OLD_SERVICE"
    
    # Send SIGTERM to allow graceful shutdown
    docker compose kill -s SIGTERM "$OLD_SERVICE"
    
    # Wait for connections to drain
    DRAIN_TIMEOUT=120
    echo "Waiting up to ${DRAIN_TIMEOUT} seconds for connections to drain..."
    
    # Wait for connections to drain
    start_time=$(date +%s)
    while docker compose ps "$OLD_SERVICE" --status running | grep -q "$OLD_SERVICE"; do
        current_time=$(date +%s)
        elapsed=$((current_time - start_time))
        
        if [ $elapsed -ge $DRAIN_TIMEOUT ]; then
            echo "Timeout reached after ${elapsed} seconds"
            break
        fi
        sleep 0.25
    done
    
    # If container is still running after timeout, force stop it
    if docker compose ps "$OLD_SERVICE" --status running | grep -q "$OLD_SERVICE"; then
        echo "Container did not stop gracefully within timeout, forcing stop..."
        docker compose stop "$OLD_SERVICE"
    else
        echo "Container stopped gracefully"
    fi
fi

# Optional: cleanup old images
docker image prune -f