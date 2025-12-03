# power-queues - High‑Performance Redis Streams Queue Engine for Node.js

Production‑ready, lightweight and highly scalable
queue engine built directly on **Redis Streams + Lua scripts**.
It is designed for real‑world distributed systems that require **high
throughput**, **idempotent task execution**, **automatic recovery**, and
**predictable performance under heavy load**.

Unlike traditional Redis‑based queues that rely on lists or heavy
abstractions, power-queues focuses on **low‑level control**, **atomic
operations**, and **minimal overhead**, making it ideal for high‑load
backends, microservices, schedulers, telemetry pipelines, and data
processing clusters.

<p align="center">
  <img src="https://img.shields.io/badge/redis-streams-red?logo=redis" />
  <img src="https://img.shields.io/badge/nodejs-queue-green?logo=node.js" />
  <img src="https://img.shields.io/badge/typescript-ready-blue?logo=typescript" />
  <img src="https://img.shields.io/badge/license-MIT-lightgrey" />
  <img src="https://img.shields.io/badge/status-production-success" />
</p>

---

## 📚 Documentation

Full documentation is available here:  
👉 **https://power-queues.docs.ihor.bielchenko.com**

---

## 🚀 Key Features

### **1. Ultra‑Fast Bulk XADD (Lua‑Powered)**

-   Adds thousands of messages per second using optimized Lua scripts.
-   Minimizes round‑trips to Redis.
-   Supports batching based on:
    -   number of tasks
    -   number of Redis arguments (safe upper bound)
-   Outperforms typical list‑based queues and generic abstractions.

---

### **2. Built‑in Idempotent Workers**

Every task can carry an `idemKey`, guaranteeing **exactly‑once
execution** even under: - worker crashes
- network interruptions
- duplicate task submissions
- process restarts

Idempotency includes: - Lock key
- Start key
- Done key
- TTL‑managed execution lock
- Automatic release on failure
- Heartbeat mechanism
- Waiting on TTL for contended executions

This makes the engine ideal for: - payment processing
- external API calls
- high‑value jobs
- distributed pipelines

---

### **3. Stuck Task Recovery (Advanced Stream Scanning)**

If a worker crashes mid‑execution, power-queues automatically detects: -
abandoned tasks
- stalled locks
- unfinished start keys

The engine then recovers these tasks back to active processing safely
and efficiently.

---

### **4. High‑Throughput Workers**

-   Batch execution support
-   Parallel or sequential processing mode
-   Configurable worker loop interval
-   Individual and batch‑level error hooks
-   Safe retry flow with per‑task attempt counters

---

### **5. Native DLQ (Dead‑Letter Queue)**

When retries reach the configured limit: - the task is moved into
`${stream}:dlq`
- includes: payload, attempt count, job, timestamp, error text
- fully JSON‑safe

Perfect for monitoring or later re‑processing.

---

### **6. Zero‑Overhead Serialization**

power-queues uses: - safe JSON encoding
- optional "flat" key/value task format
- predictable and optimized payload transformation

This keeps Redis memory layout clean and eliminates overhead.

---

### **7. Complete Set of Lifecycle Hooks**

You can extend any part of the execution flow:

-   `onSelected`
-   `onExecute`
-   `onSuccess`
-   `onError`
-   `onRetry`
-   `onBatchError`
-   `onReady`

This allows full integration with: - monitoring systems
- logging pipelines
- external APM tools
- domain logic

---

### **8. Atomic Script Loading + NOSCRIPT Recovery**

Scripts are: - loaded once
- cached
- auto‑reloaded if Redis restarts
- executed safely via SHA‑based calls

Ensures resilience in failover scenarios.

---

### **9. Job Progress Tracking**

Optional per‑job counters: - `job:ok` - `job:err` - `job:ready`

Useful for UI dashboards and real‑time job progress visualization.

---

## 📦 Installation

``` bash
npm install power-queues
```
OR
```bash
yarn add power-queues
```
---

## 🧪 Quick Start

``` ts
const queue = new PowerQueues({
  stream: "email",
  group: "workers",
});

await queue.loadScripts(true);

await queue.addTasks("email", [
  { payload: { type: "welcome", userId: 42 } },
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

---

## ⚙ power-queues vs Existing Solutions

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

## 🛠 When to Choose power-queues

Use this engine if you need:

### **✔ High performance under load**

Millions of tasks per hour? No problem.

### **✔ Strong idempotent guarantees**

Exactly‑once processing for critical operations.

### **✔ Low‑level control without heavy abstractions**

No magic, no hidden states - everything is explicit.

### **✔ Predictable behavior in distributed environments**

Even with frequent worker restarts.

### **✔ Production‑grade reliability**

Backpressure, recovery, retries, dead-lettering - all included.

---

## 🏗️ Project Structure & Architecture

-   Redis Streams for messaging
-   Lua scripts for atomic operations
-   JavaScript/TypeScript API
-   Full worker lifecycle management
-   Configurable backpressure & contention handling
-   Optional job‑level progress tracking

---

## 🧩 Extensibility

power-queues is ideal for building:

-   task schedulers
-   distributed cron engines
-   ETL pipelines
-   telemetry processors
-   notification workers
-   device monitoring systems
-   AI job pipelines
-   high-frequency background jobs

---

## 🧱 Reliability First

Every part of the engine is designed to prevent:

-   double execution
-   stuck tasks
-   orphan locks
-   lost messages
-   zombie workers
-   script desynchronization

The heartbeat + TTL strategy guarantees that no task is "lost" even in
chaotic cluster environments.

---

## 🏷️ SEO‑Optimized Keywords (Non‑Spam)

power-queues is relevant for:

-   Redis Streams queue engine
-   Node.js stream-based queue
-   idempotent task processing
-   high‑performance job queue for Node.js
-   Redis Lua queue
-   distributed worker engine
-   scalable background jobs
-   enterprise-grade Redis queue
-   microservices task runner
-   fault-tolerant queue for Node.js

---

## 📝 License

MIT - free for commercial and private use.

---

## ⭐ Why This Project Exists

Most Node.js queue libraries are: - too slow
- too abstract
- not idempotent
- not safe for financial or mission‑critical workloads

power-queues was built to solve real production problems where: -
*duplicate tasks cost money*,
- *workers are unstable*,
- *tasks must survive restarts*,
- *performance matters at scale*.

If these things matter to you - this engine will feel like home.
