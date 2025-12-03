# power-queues

## A lightweight, scalable, and high-performance queue engine for **Node.js** built on **Redis Streams** + **Lua scripts**.

The library is designed for real-world distributed systems that require high throughput, idempotent task execution, automatic recovery, and predictable performance under heavy load.

Unlike traditional Redis-based queues that rely on lists or complex abstractions, **power-queues** focuses on low-level control, atomic operations, and minimal overhead, making it ideal for high-load backends, microservices, schedulers, telemetry pipelines, and data-processing clusters.

Extends **[power-redis](https://www.npmjs.com/package/power-redis)**.

<p align="center">
	<img src="https://img.shields.io/badge/redis-streams-red?logo=redis" />
	<img src="https://img.shields.io/badge/nodejs-queue-green?logo=node.js" />
	<img src="https://img.shields.io/badge/typescript-ready-blue?logo=typescript" />
	<img src="https://img.shields.io/badge/license-MIT-lightgrey" />
	<img src="https://img.shields.io/badge/status-production-success" />
</p>

## 📚 Documentation

Full documentation is available here:  
👉 **https://power-queues.docs.ihor.bielchenko.com**

## 📦 Installation

``` bash
npm install power-queues
```
OR
```bash
yarn add power-queues
```

## 🧪 Basic usage

``` ts
const queue = new PowerQueues({
	stream: 'email',
	group: 'workers',
});

await queue.loadScripts(true);

await queue.addTasks('email', [
	{ payload: { type: 'welcome', userId: 42 } },
	{ payload: { type: 'hello', userId: 51 } }
]);
```

Worker:

``` ts
class EmailWorker extends PowerQueues {
	async onExecute(id, payload) {
		await sendEmail(payload);
	}
}
```

## ⚖️ power-queues vs Existing Solutions

|Feature               |power-queues    |BullMQ      |Bee-Queue   |Custom Streams|
|----------------------|----------------|----------- |------------|--------------|
|Bulk XADD (Lua)       |✅ Yes          |❌ No       |❌ No       |Rare          |
|Idempotent workers    |✅ Built-in     |Partial     |❌ No       |❌ No         |
|Stuck-task recovery   |✅ Advanced     |Basic       |❌ No       |Manual        |
|Heartbeats            |✅ Yes          |Limited     |❌ No       |Manual        |
|Retry logic           |✅ Flexible     |Good        |Basic       |Manual        |
|DLQ                   |✅ Native       |Basic       |❌ No       |Manual        |
|Pure Streams          |✅ Yes          |Partial     |❌ No       |Yes           |
|Lua optimization      |✅ Strong       |Minimal     |❌ No       |Manual        |
|Throughput            |🔥 Very high    |High        |Medium      |Depends       |
|Overhead              |Low             |Medium      |Low         |Very high     |

## 🚀 Key Features & Advantages

### ✔ Ultra‑Fast Bulk XADD (Lua‑Powered)
- Adds thousands of messages per second using optimized Lua scripts.
- Minimizes round‑trips to Redis.
- Supports batching based on:
	- number of tasks
	- number of Redis arguments (safe upper bound)
- Outperforms typical list‑based queues and generic abstractions.

### ✔ Built‑in Idempotent Workers
Every task can carry an `idemKey`, guaranteeing **exactly‑once execution** even under:
- worker crashes
- network interruptions
- duplicate task submissions
- process restarts

Idempotency includes:
- Lock key
- Start key
- Done key
- TTL‑managed execution lock
- Automatic release on failure
- Heartbeat mechanism
- Waiting on TTL for contended executions

This makes the engine ideal for:
- payment processing
- external API calls
- high‑value jobs
- distributed pipelines

### ✔ Stuck Task Recovery (Advanced Stream Scanning)
If a worker crashes mid‑execution, **power-queues** automatically detects:
- abandoned tasks
- stalled locks
- unfinished start keys

The engine then recovers these tasks back to active processing safely
and efficiently.

### ✔ High‑Throughput Workers
- Batch execution support
- Parallel or sequential processing mode
- Configurable worker loop interval
- Individual and batch‑level error hooks
- Safe retry flow with per‑task attempt counters

### ✔ Native DLQ (Dead‑Letter Queue)
When retries reach the configured limit:
- the task is moved into `${stream}:dlq`
- includes: payload, attempt count, job, timestamp, error text
- fully JSON‑safe

Perfect for monitoring or later re‑processing.

### ✔ Zero‑Overhead Serialization
**power-queues** uses:
- safe JSON encoding
- optional "flat" key/value task format
- predictable and optimized payload transformation

This keeps Redis memory layout clean and eliminates overhead.

### ✔ Complete Set of Lifecycle Hooks
You can extend any part of the execution flow:
- `onSelected`
- `onExecute`
- `onSuccess`
- `onError`
- `onRetry`
- `onBatchError`
- `onReady`

This allows full integration with:
- monitoring systems
- logging pipelines
- external APM tools
- domain logic

### ✔ Atomic Script Loading + NOSCRIPT Recovery
Scripts are:
- loaded once
- cached
- auto‑reloaded if Redis restarts
- executed safely via SHA‑based calls

Ensures resilience in failover scenarios.

### ✔ Job Progress Tracking
Optional per‑job counters:
- `job:ok`
- `job:err`
- `job:ready`

Useful for UI dashboards and real‑time job progress visualization.

## 🧩 Extensibility

**power-queues** is ideal for building:
- task schedulers
- distributed cron engines
- ETL pipelines
- telemetry processors
- notification workers
- device monitoring systems
- AI job pipelines
- high-frequency background jobs

## 🧱 Reliability First

Every part of the engine is designed to prevent:
- double execution
- stuck tasks
- orphan locks
- lost messages
- zombie workers
- script desynchronization

The heartbeat + TTL strategy guarantees that no task is "lost" even in
chaotic cluster environments.

## 📜 License  
MIT - free for commercial and private use.
