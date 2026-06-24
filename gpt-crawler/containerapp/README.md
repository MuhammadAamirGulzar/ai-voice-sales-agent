# Containerized Crawler Runtime

This directory packages the crawler service into a container image for reproducible execution.

## Prerequisites

- Docker installed and running

## Build and Run

From this directory:

```bash
./run.sh
```

The script builds the image and launches the crawler using the mounted `data/` config.

## Configuration

- Update `data/config.ts` before running.
- Keep output artifacts in mounted data paths only.

## Production Guidance

- Pin image versions in deployment manifests.
- Run with resource limits to prevent uncontrolled crawl jobs.
- Route logs to centralized aggregation in hosted environments.
