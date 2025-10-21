# power-queues
A production-ready, **Redis-backed queue runner** with visibility timeouts, delayed scheduling, chainable queues, retries with exponential backoff + jitter, and heartbeat renewals — built on top of a thin ```PowerRedis``` abstraction.

This module exposes a base abstract class PowerQueue which you extend and implement. It supports:

- FIFO reservation (`LMOVE LEFT->RIGHT` preferred, `RPOPLPUSH` fallback) via Lua scripts or client loop.
- **Visibility timeout (VT)** with periodic heartbeat (ZSET scores store deadlines).
- **Delayed tasks** (ZSET with future timestamps) and promotion to ready list.
- **Retry policy** with exponential backoff + jitter and a **fail** queue after max attempts.
- **Chain mode**: automatically forward a task through a list of queues and call lifecycle hooks.
- Iteration loop with promote/requeue passes, concurrency slicing, and status counters with TTL.

It provides the core logic for **visibility timeouts**, **task retries**, **delayed scheduling**, **chain forwarding**, and **concurrent workers** — without any external dependencies.

> Built on top of [**PowerRedis**](https://github.com/ihor-bielchenko/power-redis)  
> Uses utilities from [**full-utils**](https://github.com/ihor-bielchenko/full-utils)

## API (with examples)
Below are brief excerpts. Full JSDoc: power-queues.docs.ihor.bielchenko.com.

## Overview

PowerQueue is not a “ready-made” message broker like BullMQ — it’s a **foundation** for building your own  
custom Redis-based queues with full control over task lifecycle and execution flow.

It manages:

- `LIST` for **ready tasks** (RPUSH producers, LMOVE/RPOPLPUSH consumers)
- `LIST` for **processing tasks** (temporary visibility list)
- `ZSET` for **visibility timeouts** (invisible tasks until acknowledged or expired)
- `ZSET` for **delayed scheduling** (future tasks activation)

Ideal for:
- high-performance Redis-based backends,
- telemetry and distributed job systems,
- retryable or delayed workloads,
- cron-like task promotion and requeueing.


---

## Key Features

- **Visibility timeout** — automatic requeue if worker crashes or fails to ack in time  
- **Delayed tasks** — schedule jobs for future execution (e.g., 5 minutes later)  
- **Concurrent processing** — process multiple jobs in parallel  
- **Retry logic** — exponential backoff with configurable limits  
- **Lifecycle hooks** — extend and customize every phase  
- **Task chaining** — forward results between queues  
- **Lua-optimized reservation** — atomic batch pulls via LMOVE or RPOPLPUSH fallback  
- **Zero dependencies** — built purely on Node.js + PowerRedis + full-utils

## Installation

```bash
npm install power-queue
# or
yarn add power-queue
```
PowerQueue works with any Redis client compatible with IORedis interface.

## Example Usage
### 1. Create your custom queue
```javascript
import { PowerQueue } from 'power-queue';

class EmailQueue extends PowerQueue {
  /**
   * Process a single task payload.
   */
  async execute(task) {
    console.log('📨 Sending email:', task.payload);

    // Simulate async work
    await this.wait(200);

    // Optionally return result
    return { status: 'sent', timestamp: Date.now() };
  }
}

export const emailQueue = new EmailQueue({
  redis: { host: '127.0.0.1', port: 6379 },
});

```
### 2. Enqueue a new task
```javascript
await emailQueue.enqueue('emails', {
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Hello and thanks for joining!',
});

```
### 3. Start processing
```javascript
emailQueue.start('emails');

```
That’s it — PowerQueue will:
- Pull tasks from Redis LIST atomically
- Move them into processing + visibility sets
- Retry or requeue on error or timeout
- Handle parallel processing with full isolation

## Configuration Options
Each property is strongly typed and fully documented in TypeDoc.
| Property               | Type                                | Default | Description                                    |
| ---------------------- | ----------------------------------- | ------- | ---------------------------------------------- |
| `iterationTimeout`     | `number`                            | `1000`  | Delay (ms) between polling loops               |
| `portionLength`        | `number`                            | `1000`  | Max number of tasks per batch pull             |
| `expireStatusSec`      | `number`                            | `300`   | TTL for progress tracking                      |
| `maxAttempts`          | `number`                            | `1`     | Max retry attempts before marking as failed    |
| `concurrency`          | `number`                            | `32`    | Parallel runners per queue                     |
| `visibilityTimeoutSec` | `number`                            | `60`    | How long a task stays invisible before requeue |
| `retryBaseSec`         | `number`                            | `1`     | Base delay for exponential backoff             |
| `retryMaxSec`          | `number`                            | `3600`  | Max retry delay                                |
| `runners`              | `Map<string, { running: boolean }>` | —       | Active queue execution flags                   |
| `processingRaw`        | `Map<string, string>`               | —       | Tracks raw Redis entries for acknowledgment    |
| `heartbeatTimers`      | `Map<string, NodeJS.Timeout>`       | —       | Maintains active visibility extensions         |


## Design Principles
### Minimalism
Only core Redis primitives are used — no unnecessary abstractions or event emitters.

### Predictability
All queue state is transparent and Redis-inspectable:
- ```LIST``` for ready and processing states
- ```ZSET``` for timeouts and scheduling

### Resilience
Every step (reserve, ack, retry, promote) can recover after process restart.

### Extensibility
Implement custom behaviors by overriding:
```javascript
async beforeExecute(task) {}
async afterExecute(task, result) {}
async onError(task, error) {}
async onRetry(task, delaySec) {}
```

## License
Use freely in your own projects. Add proper notices if you publish a package (MIT/Apache-2.0, etc.).