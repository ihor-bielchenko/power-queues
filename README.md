# power-queues
## High-Performance Redis Streams Queue for Node.js

Ultra-fast, fault-tolerant, Lua-optimized distributed task queue built on Redis Streams.  
Supports **bulk XADD**, **idempotent jobs**, **retries**, **DLQ**, **stuck-task recovery**, **batching**, and **consumer groups**.  
Designed for large-scale microservices, telemetry pipelines, and high-load systems.

<p align="center">
  <img src="https://img.shields.io/badge/redis-streams-red?logo=redis" />
  <img src="https://img.shields.io/badge/nodejs-queue-green?logo=node.js" />
  <img src="https://img.shields.io/badge/typescript-ready-blue?logo=typescript" />
  <img src="https://img.shields.io/badge/nestjs-support-ea2845?logo=nestjs" />
  <img src="https://img.shields.io/badge/license-MIT-lightgrey" />
  <img src="https://img.shields.io/badge/status-production-success" />
</p>

---

## 📚 Documentation

Full documentation is available here:  
👉 **https://power-queues.docs.ihor.bielchenko.com**

---

## 🚀 Features

- ⚡ **Bulk XADD** — send thousands of tasks in a single Redis call  
- 🔁 **Retries & attempt tracking**  
- 🧠 **Idempotent job execution** (Lua locks, TTL, start/done keys)  
- 🧹 **Stuck task recovery** (XAUTOCLAIM + Lua-based recovery)  
- 🌀 **Consumer groups + batching**  
- 📥 **Dead Letter Queue (DLQ)**  
- 🔐 **Stream trimming, approx/exact maxlen, minid window**  
- 🧱 **Fully async, high-throughput, production-ready**  

---

## 📦 Installation

```bash
npm install power-queues
```

---

## 🧪 Quick Start

```ts
import { QueueService } from './queue.service';

const queue = new QueueService();

// Add tasks
await queue.addTasks('my_queue', [
  { payload: { foo: 'bar' } },
  { payload: { a: 1, b: 2 } },
]);

// Run worker
queue.runQueue();
```

---

## 🔧 Add Tasks (Bulk)

```ts
await queue.addTasks('mass_polling', largeArray, {
  approx: true,
  minidWindowMs: 30000,
  maxlen: largeArray.length,
});
```

---

## 🏗️ Worker Hooks

You can override:

- `onExecute`
- `onSuccess`
- `onError`
- `onRetry`
- `onBatchError`
- `onSelected`
- `onReady`

Example:

```ts
async onExecute(id, payload) {
  console.log('executing', id, payload);
}
```

---

## 🧱 Architecture Overview

```
Producer → Redis Stream → Consumer Group → Worker → DLQ (optional)
```

- Redis Streams store tasks  
- Lua scripts handle trimming, idempotency, stuck recovery  
- Workers fetch tasks via XREADGROUP or Lua select  
- Tasks executed, ACKed, or sent to DLQ  

---

## 🗄️ Dead Letter Queue (DLQ)

Failed tasks after `workerMaxRetries` automatically go to:

```
<stream>:dlq
```

---

## 🧩 Idempotency

Guaranteed by 3 keys:

- `doneKey`
- `lockKey`
- `startKey`

This prevents double-execution during retries, crashes, or concurrency.

---

## 🚀 Performance

- 10,000+ XADDs/sec  
- Bulk mode: 50,000 operations in one request  
- Extremely low CPU usage due to Lua trimming  

---

## 🏷️ SEO Keywords

```
redis streams, redis queue, task queue, job queue, nodejs queue, nestjs queue,
bulk xadd, distributed queue system, background jobs, retries, dlq,
idempotency, redis lua scripts, microservices, high-performance queue,
high-throughput, batching, concurrency control
```

---

## 📜 License

MIT
