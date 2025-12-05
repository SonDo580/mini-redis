# Go Redis

A Redis-like server implemented in Go.

## Guide

- Guidelines for self-implementation:

  - https://app.codecrafters.io/courses/redis/overview
  - https://github.com/codecrafters-io/build-your-own-redis/tree/main/stage_descriptions

- Follow-along guide for a kickstart: https://www.build-redis-from-scratch.dev/

## Differences from real Redis

- Use `goroutine-per-client` model, not single-threaded event loop.

## Prerequisites

- Go 1.24+
- redis-cli _(for testing)_

## Usage

### Run server

```bash
# Clone the repository
git clone git@github.com:SonDo580/go-redis.git

# Run the server
go run .
```

### Connect with `redis-cli`

```bash
redis-cli -p 6379
```
