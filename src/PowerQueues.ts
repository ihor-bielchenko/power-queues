import type {
	JsonPrimitiveOrUndefined, 
	IORedisLike, 
} from 'power-redis';
import type {
	AddTasksOptions, 
	Task,
	SavedScript,
	IdempotencyKeys,
} from './types';
import { PowerRedis } from 'power-redis';
import { 
	isObjFilled,
	isObj,
	isArrFilled, 
	isArr,
	isStrFilled,
	isNumNZ,
	jsonDecode,
	wait,
} from 'full-utils';
import { v4 as uuid } from 'uuid';
import {
	XAddBulk,
	Approve,
	IdempotencyAllow,
	IdempotencyStart,
	IdempotencyDone,
	IdempotencyFree,
	SelectStuck,
} from './scripts';

class Base {
}

export class PowerQueues extends PowerRedis {
	/**
	 * A controller used to manage cancellation signals inside the queue worker.
	 *
	 * @remarks
	 * PowerQueues uses an internal {@link AbortController} to safely stop
	 * long-running operations (such as waiting, heartbeats, or worker loops)
	 * without forcing the process to crash.
	 *
	 * When `abort()` is triggered:
	 * - all functions that listen to `abort.signal` will stop as soon as possible
	 * - pending delays (in `waitAbortable`) are cleared
	 * - background loops (like `consumerLoop`) exit gracefully
	 *
	 * This provides a clean and predictable shutdown mechanism for the worker.
	 *
	 * @example
	 * ```ts
	 * // Stop the queue worker from outside
	 * queues.abort.abort();
	 * ```
	 *
	 * @property abort.signal
	 * The {@link AbortSignal} passed to internal async waits and loops.
	 * It allows those operations to check if the worker has been requested to stop.
	 *
	 * @see AbortController
	 * @see https://developer.mozilla.org/en-US/docs/Web/API/AbortController
	 */
	public abort = new AbortController();

	/**
	 * The underlying Redis client instance used by PowerQueues.
	 *
	 * @remarks
	 * This property must be assigned by the user before running any queue
	 * operations. It provides the low-level Redis commands that all internal
	 * features rely on: XADD, XREADGROUP, XACK, Lua scripts, key operations,
	 * TTL updates, etc.
	 *
	 * PowerQueues does **not** create its own Redis connection.  
	 * Instead, it expects an external instance (e.g., from `ioredis`,
	 * `@nestjs-labs/nestjs-ioredis`, or any custom wrapper) that implements
	 * the {@link IORedisLike} interface.
	 *
	 * If `redis` is not set or is disconnected, the queue worker will not be
	 * able to read or write tasks.
	 *
	 * @example
	 * ```ts
	 * const queues = new MyQueueClass();
	 * queues.redis = redisService.getClient(); // must assign before use
	 * await queues.runQueue();
	 * ```
	 *
	 * @property redis
	 * The active Redis connection implementing the {@link IORedisLike} API.
	 * Used internally for all queue operations, Lua scripts, and stream commands.
	 */
	public redis!: IORedisLike;

	/**
	 * In-memory registry of Lua scripts used by PowerQueues.
	 *
	 * @remarks
	 * PowerQueues loads its Lua scripts (XAddBulk, Approve, idempotency helpers,
	 * SelectStuck, etc.) into Redis and keeps their metadata in this map.
	 *
	 * Each entry is stored under a human-readable key (for example `"XAddBulk"`)
	 * and contains a {@link SavedScript} object:
	 *
	 * - `codeBody`  – the original Lua source code
	 * - `codeReady` – the SHA1 returned by `SCRIPT LOAD` (used with `EVALSHA`)
	 *
	 * This allows the queue to:
	 * - load scripts only once per process,
	 * - reuse `EVALSHA` for better performance,
	 * - automatically reload scripts if Redis loses them (e.g. after restart).
	 *
	 * You normally do not modify this map directly.  
	 * It is managed internally by methods like {@link loadScripts},
	 * {@link saveScript}, and {@link runScript}.
	 *
	 * @example
	 * ```ts
	 * // After loadScripts() is called:
	 * console.log(Object.keys(queues.scripts));
	 * // -> ["XAddBulk", "Approve", "IdempotencyAllow", ...]
	 * ```
	 *
	 * @property scripts
	 * A dictionary of loaded Lua scripts, indexed by script name.
	 */
	public readonly scripts: Record<string, SavedScript> = {};

	/**
	 * Maximum number of tasks allowed in a single XADD batch.
	 *
	 * @remarks
	 * When adding many tasks at once, PowerQueues groups them into batches
	 * before sending them to Redis. This improves throughput and reduces
	 * network overhead while preventing oversized Redis commands.
	 *
	 * `addingBatchTasksCount` defines the **upper limit** of how many tasks
	 * can be bundled into one batch during `addTasks()` → `xaddBatch()` calls.
	 *
	 * A lower value = safer for memory and network stability.  
	 * A higher value = fewer XADD calls, higher throughput, but larger payloads.
	 *
	 * Default value `800` is tuned for:
	 * - high-performance ingestion
	 * - safe XADD command size
	 * - good balance between speed and memory usage
	 *
	 * @example
	 * ```ts
	 * // Allow larger batches when inserting many tasks at once
	 * queues.addingBatchTasksCount = 2000;
	 * ```
	 *
	 * @property addingBatchTasksCount
	 * Maximum number of task objects combined into one batch before XADD.
	 */
	public readonly addingBatchTasksCount: number = 800;

	/**
	 * Maximum total number of Redis field/value pairs allowed in a single batch.
	 *
	 * @remarks
	 * When PowerQueues builds XADD batches, it must ensure that the final Redis
	 * command does not become too large. Every task contributes multiple
	 * field/value pairs (for example: `payload`, `createdAt`, `job`, `idemKey`, etc.).
	 *
	 * `addingBatchKeysLimit` defines the **hard limit** for how many
	 * total key/value tokens may be included in one batch before the batch
	 * is split into smaller ones.
	 *
	 * This prevents:
	 * - oversized XADD commands,
	 * - excessive memory usage,
	 * - Redis "argument list too long" / "command too large" issues,
	 * - network frame fragmentation.
	 *
	 * Combined with {@link addingBatchTasksCount}, this creates a dual safety
	 * mechanism: limit by number of tasks **and** limit by number of fields.
	 *
	 * Default value `10000` is optimized for:
	 * - stable high-throughput ingestion,
	 * - minimal risk of large-packet bottlenecks,
	 * - predictable batch sizing under varied payloads.
	 *
	 * @example
	 * ```ts
	 * // Reduce batch size for very large payloads
	 * queues.addingBatchKeysLimit = 3000;
	 * ```
	 *
	 * @property addingBatchKeysLimit
	 * Maximum number of field/value pairs allowed in one XADD batch.
	 */
	public readonly addingBatchKeysLimit: number = 10000;

	/**
	 * Time (in milliseconds) that an idempotent task lock remains valid.
	 *
	 * @remarks
	 * When a worker starts executing a task that uses idempotency (`idemKey`),
	 * PowerQueues creates a short-lived "execution lock" in Redis.  
	 * This lock prevents multiple workers from processing the same logical task
	 * at the same time.
	 *
	 * `workerExecuteLockTimeoutMs` defines how long this lock stays active
	 * unless refreshed by the heartbeat mechanism.
	 *
	 * Internally it is used in:
	 * - {@link idempotencyAllow}
	 * - {@link idempotencyStart}
	 * - {@link sendHeartbeat}
	 * - {@link heartbeat}
	 *
	 * If the worker crashes, disconnects, or hangs, the lock expires
	 * automatically, allowing another worker to take over safely.
	 *
	 * Default value `180000` (3 minutes) provides:
	 * - enough time to complete typical async work,
	 * - automatic recovery without manual cleanup,
	 * - protection against double-processing.
	 *
	 * @example
	 * ```ts
	 * // For very long-running jobs (e.g. external API pipelines)
	 * queues.workerExecuteLockTimeoutMs = 600000; // 10 minutes
	 * ```
	 *
	 * @property workerExecuteLockTimeoutMs
	 * Execution lock expiration time for idempotent tasks, in milliseconds.
	 */
	public readonly workerExecuteLockTimeoutMs: number = 180000;

	/**
	 * The time (in **milliseconds**) to keep the *idempotency completion marker* (`doneKey`)
	 * alive after a task has successfully finished.
	 *
	 * ### What this timeout does
	 * When a task with an idempotency key finishes, Redis stores a small “done” flag
	 * so that repeated executions of the same task can be skipped immediately.
	 *
	 * `workerCacheTaskTimeoutMs` defines **how long this flag remains valid**.
	 *
	 * After the timeout expires:
	 * - the task is considered “not completed” again,
	 * - a future worker may reprocess the same idempotency key,
	 * - this allows long-running or retried tasks to be re-executed if needed.
	 *
	 * ### Why it exists
	 * In distributed systems multiple workers may attempt to process the same
	 * task. This timeout creates a small **safety cache window** that prevents
	 * duplicate work while still allowing re-execution in controlled scenarios.
	 *
	 * ### Best practices
	 * - Use values between **30s–5min** depending on how often retries or
	 *   duplicate messages naturally happen in your system.
	 * - Too small → might allow premature reprocessing.
	 * - Too large → may block legitimate re-execution for too long.
	 *
	 * @default 60000 (1 minute)
	 */
	public readonly workerCacheTaskTimeoutMs: number = 60000;

	/**
	 * Maximum number of task IDs acknowledged (XACK) in a single approval batch.
	 *
	 * @remarks
	 * After tasks are processed successfully, PowerQueues must acknowledge them
	 * in Redis using `XACK`, and optionally delete them via `XDEL`.
	 *
	 * To avoid sending extremely large XACK/XDEL commands, the IDs are processed
	 * in batches. `approveBatchTasksCount` defines the maximum number of IDs
	 * included in one batch when calling {@link approve}.
	 *
	 * Why batching matters:
	 * - prevents Redis from receiving oversized argument lists  
	 * - improves throughput when approving many tasks at once  
	 * - reduces memory spikes in the Redis command parser  
	 *
	 * Internally, the value is clamped to:
	 * - **minimum:** 500  
	 * - **maximum:** 4000  
	 *
	 * This ensures safe, stable performance even if the user sets unusual values.
	 *
	 * Default value `2000` provides:
	 * - fast acknowledgements
	 * - safe command sizes
	 * - good balance for high-volume queues
	 *
	 * @example
	 * ```ts
	 * // For very large task flows, increase the approval batch size
	 * queues.approveBatchTasksCount = 3500;
	 * ```
	 *
	 * @property approveBatchTasksCount
	 * Maximum number of task IDs processed in one XACK batch before splitting.
	 */
	public readonly approveBatchTasksCount: number = 2000;

	/**
	 * Whether successfully executed tasks should be removed from the Redis stream.
	 *
	 * @remarks
	 * By default, PowerQueues does **not** delete processed tasks from the stream.
	 * Redis Streams can act as an immutable log, which is useful for debugging,
	 * auditing, replaying, or monitoring.
	 *
	 * When `removeOnExecuted` is set to `true`, PowerQueues will delete each
	 * acknowledged entry using `XDEL` immediately after `XACK`.
	 *
	 * Benefits of keeping tasks (default: `false`):
	 * - historical visibility into what tasks were processed  
	 * - easier debugging and replay  
	 * - safer in multi-worker environments  
	 *
	 * Benefits of removing tasks (`true`):
	 * - smaller Redis memory footprint  
	 * - faster XREADGROUP / XAUTCLAIM operations  
	 * - prevents streams from growing indefinitely  
	 *
	 * This setting affects only **successfully executed** tasks.
	 * Failed tasks may still go to the DLQ depending on retries.
	 *
	 * @example
	 * ```ts
	 * // Enable automatic deletion of executed tasks:
	 * queues.removeOnExecuted = true;
	 * ```
	 *
	 * @property removeOnExecuted
	 * If `true`, XDEL is called after XACK to remove executed task entries.
	 */
	public readonly removeOnExecuted: boolean = false;

	/**
	 * Enables parallel (batched) execution of tasks instead of processing them one-by-one.
	 *
	 * @remarks
	 * By default, PowerQueues processes tasks **sequentially** inside `execute()`.
	 * This ensures predictable behavior and simpler idempotency handling.
	 *
	 * When `executeBatchAtOnce` is set to `true`, each task in the batch is executed
	 * **concurrently** using `Promise.all`, which can significantly increase throughput
	 * for CPU-light or I/O-heavy workloads.
	 *
	 * **However, enabling this requires caution:**
	 * - Task handlers (`onExecute`) must be thread-safe and free of shared mutable state.
	 * - Heavy concurrency may increase contention during idempotency checks.
	 * - Error handling remains per-task, but failures may occur in parallel.
	 *
	 * Recommended when:
	 * - workers perform non-blocking async operations (DB queries, HTTP calls, etc.)
	 * - your environment benefits from parallelism (Node.js event loop + await)
	 *
	 * Not recommended when:
	 * - tasks modify shared external resources
	 * - tasks rely on strict ordering
	 * - workers perform heavy CPU tasks (event loop stall risk)
	 *
	 * Default value `false` ensures:
	 * - safe and predictable execution flow  
	 * - simpler debugging  
	 * - minimal race conditions  
	 *
	 * @example
	 * ```ts
	 * // Enable concurrent execution of a batch of tasks
	 * queues.executeBatchAtOnce = true;
	 * ```
	 *
	 * @property executeBatchAtOnce
	 * If `true`, tasks in a batch are executed concurrently rather than sequentially.
	 */
	public readonly executeBatchAtOnce: boolean = false;

	/**
	 * Enables storing execution statistics for each job in Redis.
	 *
	 * @remarks
	 * When this option is `true`, PowerQueues records simple counters in Redis
	 * that reflect the status of tasks belonging to the same logical job
	 * (identified by the auto-generated `job` ID added during `addTasks()`).
	 *
	 * These counters are updated during execution:
	 *
	 * - `ok`   → incremented when a task finishes successfully  
	 * - `err`  → incremented when a task exhausts all retries and fails  
	 * - `ready` → incremented for every processed task (success or fail)  
	 *
	 * This creates a lightweight job-level progress tracking mechanism.
	 * Counters are automatically expired using `executeJobStatusTtlMs`.
	 *
	 * Useful for:
	 * - tracking large batch imports  
	 * - showing job progress in dashboards  
	 * - monitoring worker throughput  
	 * - debugging stuck or failing tasks  
	 *
	 * Not recommended when:
	 * - you do not need job-level metrics  
	 * - Redis key count must remain minimal  
	 * - you have extremely high throughput and prefer zero-overhead writes  
	 *
	 * Default value `false` disables status tracking entirely.
	 *
	 * @example
	 * ```ts
	 * // Enable job progress tracking
	 * queues.executeJobStatus = true;
	 * ```
	 *
	 * @property executeJobStatus
	 * Enables recording per-job execution counters (ok / err / ready).
	 */
	public readonly executeJobStatus: boolean = false;

	/**
	 * Time-to-live (in milliseconds) for job-level execution status counters.
	 *
	 * @remarks
	 * When {@link executeJobStatus} is enabled, PowerQueues creates small Redis
	 * counters for each job:
	 *
	 * - `...:ok`    — number of successfully executed tasks  
	 * - `...:err`   — number of tasks that failed after all retries  
	 * - `...:ready` — number of tasks processed (success + fail)  
	 *
	 * These counters should not stay in Redis forever, so each increment call
	 * also sets a TTL. `executeJobStatusTtlMs` defines how long the job metrics
	 * remain available before they expire automatically.
	 *
	 * Why a TTL?
	 * - prevents Redis key buildup  
	 * - keeps monitoring data fresh  
	 * - avoids manual cleanup  
	 * - ensures dashboards always reflect recent jobs  
	 *
	 * Default value `300000` (5 minutes) is suitable for:
	 * - short-lived jobs  
	 * - quick diagnostics after ingestion  
	 * - lightweight progress tracking  
	 *
	 * Increase the TTL if:
	 * - jobs take longer to complete  
	 * - you need to inspect job history over time  
	 *
	 * @example
	 * ```ts
	 * // Keep job progress data for 30 minutes
	 * queues.executeJobStatusTtlMs = 1800000;
	 * ```
	 *
	 * @property executeJobStatusTtlMs
	 * TTL (milliseconds) applied to job status counters created when job tracking is enabled.
	 */
	public readonly executeJobStatusTtlMs: number = 300000;

	/**
	 * Logical identifier of the machine or environment running this consumer.
	 *
	 * @remarks
	 * PowerQueues includes the consumer identity in the Redis consumer name,
	 * which is formatted as:
	 *
	 * ```
	 * {consumerHost}:{process.pid}
	 * ```
	 *
	 * This helps distinguish between different workers in Redis Stream groups,
	 * especially when multiple machines or Docker containers process the same queue.
	 *
	 * `consumerHost` does **not** need to be the real hostname.  
	 * It can be any identifier meaningful to your infrastructure:
	 *
	 * - `'api-server-1'`
	 * - `'worker-eu-west'`
	 * - `'node-A'`
	 * - `process.env.HOSTNAME`
	 *
	 * A good value improves:
	 * - debugging (which worker processed what)
	 * - monitoring of worker activity
	 * - load-balancing visibility
	 *
	 * Default value `'host'` ensures a valid fallback, but in production you
	 * should override it with an actual machine/service identifier.
	 *
	 * @example
	 * ```ts
	 * queues.consumerHost = process.env.HOSTNAME || 'worker-1';
	 * ```
	 *
	 * @property consumerHost
	 * Base string used to form the Redis consumer name for this worker.
	 */
	public readonly consumerHost: string = 'host';

	/**
	 * The Redis Stream key used as the main task queue.
	 *
	 * @remarks
	 * All tasks added through `addTasks()` are ultimately written into this Redis
	 * Stream. Workers read from it using `XREADGROUP`, `XAUTOCLAIM`, and other
	 * stream-related commands.
	 *
	 * You should treat `stream` as the "queue name"—it uniquely identifies
	 * where tasks for this worker group are stored in Redis.
	 *
	 * Common examples:
	 * - `'emails'`
	 * - `'jobs:process'`
	 * - `'telemetry:ingest'`
	 * - `'queue:payments'`
	 *
	 * Changing `stream` changes the entire queue channel for this worker.
	 *
	 * **Important notes:**
	 * - The stream is created automatically if it does not exist (via `MKSTREAM`)
	 * - Worker groups are associated with this stream via {@link group}
	 * - DLQ (dead-letter queue) is based on `stream + ':dlq'`
	 *
	 * Default value `'stream'` is a placeholder and should be replaced
	 * in real applications.
	 *
	 * @example
	 * ```ts
	 * queues.stream = 'jobs:image-processing';
	 * ```
	 *
	 * @property stream
	 * The Redis Stream key where tasks are stored and consumed.
	 */
	public readonly stream: string = 'stream';

	/**
	 * The Redis consumer group name used by this worker.
	 *
	 * @remarks
	 * Redis Streams require consumers to join a *consumer group* in order to use
	 * features like:
	 * - `XREADGROUP` (reading pending + new messages)
	 * - `XAUTOCLAIM` (claiming stuck tasks)
	 * - `XACK` (acknowledging processed tasks)
	 *
	 * The `group` value defines the name of that consumer group.
	 *
	 * All workers that share the same:
	 * - {@link stream}  
	 * - {@link group}  
	 *
	 * will cooperatively process tasks from the same queue, while Redis ensures:
	 * - no task is delivered to more than one worker at the same time  
	 * - pending messages can be recovered if a worker dies  
	 *
	 * **You must ensure that each queue has a unique group name per stream.**
	 *
	 * PowerQueues automatically creates the group (via `XGROUP CREATE`)
	 * during `runQueue()`, if it does not already exist.
	 *
	 * Default value `'group'` is only a placeholder and should be replaced
	 * with something meaningful in production.
	 *
	 * Common naming examples:
	 * - `'workers'`
	 * - `'payments'`
	 * - `'image-processors'`
	 * - `'api-service-A'`
	 *
	 * @example
	 * ```ts
	 * queues.group = 'email-workers';
	 * ```
	 *
	 * @property group
	 * The Redis consumer group name associated with this worker.
	 */
	public readonly group: string = 'group';

	/**
	 * Maximum number of tasks a worker attempts to fetch in a single read cycle.
	 *
	 * @remarks
	 * PowerQueues reads tasks from Redis Streams in batches to improve throughput
	 * and reduce the number of network round-trips.  
	 * `workerBatchTasksCount` defines how many tasks the worker tries to pull
	 * from Redis at once using:
	 *
	 * - `XREADGROUP` for fresh tasks  
	 * - `XAUTOCLAIM` for stuck/pending tasks  
	 *
	 * A higher value increases throughput but may:
	 * - increase memory usage per iteration  
	 * - delay acknowledgements for long batches  
	 * - increase execution latency per task  
	 *
	 * A lower value:
	 * - decreases memory usage  
	 * - improves responsiveness  
	 * - reduces risk of long-running batch execution  
	 *
	 * Default value `200` is tuned to balance:
	 * - fast message consumption  
	 * - low event-loop blocking  
	 * - efficient recovery of stuck tasks  
	 *
	 * Adjust depending on worker workload:
	 * - **Increase** if your tasks are light or mostly I/O bound  
	 * - **Decrease** if tasks are heavy or your system is resource-limited  
	 *
	 * @example
	 * ```ts
	 * // Process up to 500 tasks in each fetch cycle
	 * queues.workerBatchTasksCount = 500;
	 * ```
	 *
	 * @property workerBatchTasksCount
	 * Target maximum number of tasks fetched from Redis in one worker cycle.
	 */
	public readonly workerBatchTasksCount: number = 200;

	/**
	 * Time (in milliseconds) after which a pending task is considered “stuck”.
	 *
	 * @remarks
	 * PowerQueues uses Redis Streams’ pending entries list (PEL) to detect tasks
	 * that were delivered to a worker but never acknowledged—usually because the
	 * worker crashed, froze, or disconnected.
	 *
	 * `recoveryStuckTasksTimeoutMs` defines how long a task must remain idle
	 * (unacknowledged) before it is treated as "stuck" and eligible for recovery
	 * using `XAUTOCLAIM`.
	 *
	 * The logic is handled inside the {@link SelectStuck} Lua script, where
	 * this value becomes the `min-idle-time` parameter:
	 *
	 * - If a task's idle time ≥ `recoveryStuckTasksTimeoutMs`,  
	 *   PowerQueues attempts to claim it for the current worker.
	 *
	 * Why this matters:
	 * - prevents orphaned tasks  
	 * - ensures reliable failover  
	 * - avoids task starvation  
	 * - keeps queues healthy even when workers die unexpectedly  
	 *
	 * Default value `60000` (60 seconds) is balanced for:
	 * - typical async worker workloads  
	 * - ensuring fast recovery without aggressive contention  
	 *
	 * Increase this if:
	 * - tasks normally run for a long time  
	 * - slow workers are expected  
	 *
	 * Decrease this if:
	 * - you want very fast failover  
	 * - workers are extremely lightweight  
	 *
	 * @example
	 * ```ts
	 * // Treat tasks as stuck if not acknowledged for 2 minutes
	 * queues.recoveryStuckTasksTimeoutMs = 120000;
	 * ```
	 *
	 * @property recoveryStuckTasksTimeoutMs
	 * Minimum idle time before a task is considered stuck and reclaimed.
	 */
	public readonly recoveryStuckTasksTimeoutMs: number = 60000;

	/**
	 * Delay (in milliseconds) between worker polling cycles when no tasks are available.
	 *
	 * @remarks
	 * When the worker calls `select()` and finds **no tasks** (neither stuck nor fresh),
	 * it waits before checking Redis again.  
	 * `workerLoopIntervalMs` defines how long the worker should pause between polls.
	 *
	 * This applies only when the queue is idle.  
	 * When tasks are available, the loop runs continuously without this delay.
	 *
	 * Why this interval exists:
	 * - reduces unnecessary Redis calls during idle periods  
	 * - prevents CPU spin-loops  
	 * - stabilizes load in low-traffic queues  
	 *
	 * Default value `5000` (5 seconds) provides a good balance between:
	 * - responsiveness (fast enough to detect new tasks)
	 * - efficiency (low overhead during quiet periods)
	 *
	 * Recommended adjustments:
	 * - **Lower** (500–1500 ms) for real-time or high-frequency task ingestion  
	 * - **Higher** (5–15 seconds) for slow background jobs or cost-sensitive systems  
	 *
	 * @example
	 * ```ts
	 * // Make the worker more responsive in a busy system
	 * queues.workerLoopIntervalMs = 1000; // 1 second
	 * ```
	 *
	 * @property workerLoopIntervalMs
	 * Time the worker waits before rechecking Redis when no tasks were received.
	 */
	public readonly workerLoopIntervalMs: number = 5000;

	/**
	 * Maximum time (in milliseconds) the worker is allowed to spend searching
	 * for stuck tasks during a single recovery cycle.
	 *
	 * @remarks
	 * Before reading fresh tasks, PowerQueues first tries to recover “stuck”
	 * tasks using the {@link SelectStuck} Lua script.  
	 * This script scans the pending entries list (PEL) and may perform several
	 * iterations of `XAUTOCLAIM`.
	 *
	 * `workerSelectionTimeoutMs` limits how long this recovery step is allowed
	 * to run before giving up and moving on.
	 *
	 * Why this matters:
	 * - prevents long blocking calls during heavy recovery situations  
	 * - ensures the worker stays responsive even with large queues  
	 * - avoids event-loop stalls caused by large PEL scans  
	 *
	 * The timeout is soft: the Lua script checks elapsed time and
	 * voluntarily exits when this limit is reached.
	 *
	 * Default value `80` ms is tuned to:
	 * - allow enough time to reclaim stuck messages  
	 * - prevent noticeable delays in normal task processing  
	 *
	 * Increase this if:
	 * - you expect many stuck tasks (unstable workers)
	 * - your Redis instance handles large streams
	 *
	 * Decrease this if:
	 * - minimal latency is critical  
	 * - your environment is extremely CPU-sensitive  
	 *
	 * @example
	 * ```ts
	 * // Allow up to 150 ms for stuck-task recovery
	 * queues.workerSelectionTimeoutMs = 150;
	 * ```
	 *
	 * @property workerSelectionTimeoutMs
	 * Maximum allowed duration (in milliseconds) for stuck-task recovery on each cycle.
	 */
	public readonly workerSelectionTimeoutMs: number = 80;

	/**
	 * Maximum number of retry attempts allowed for a failing task.
	 *
	 * @remarks
	 * When a task throws an error inside `onExecute`, PowerQueues:
	 * 1. increments the task's retry counter using `incrAttempts()`  
	 * 2. calls user-defined hooks (`onRetry`, then `onError`)  
	 * 3. decides whether to retry or send the task to the DLQ  
	 *
	 * `workerMaxRetries` defines how many times a task may be retried before it
	 * is considered permanently failed.
	 *
	 * Behaviour summary:
	 * - `attempt < workerMaxRetries` → task is retried  
	 * - `attempt >= workerMaxRetries` → task is moved to DLQ  
	 *
	 * **Note:**  
	 * Retries happen immediately within the same worker cycle (no delay),
	 * unless the user chooses to reinsert tasks manually.
	 *
	 * Default value `1` means:
	 * - one initial attempt  
	 * - one retry on failure  
	 * - next failure → DLQ  
	 *
	 * Increase this if:
	 * - tasks frequently fail due to external API timeouts  
	 * - retrying often resolves transient issues  
	 *
	 * Decrease this if:
	 * - retries are expensive  
	 * - failing fast is more desirable  
	 *
	 * @example
	 * ```ts
	 * // Allow tasks to retry up to 5 times before going to DLQ
	 * queues.workerMaxRetries = 5;
	 * ```
	 *
	 * @property workerMaxRetries
	 * Maximum number of retries allowed before a task is sent to the DLQ.
	 */
	public readonly workerMaxRetries: number = 1;

	/**
	 * Time-to-live (in milliseconds) for the retry-attempt counter of each task.
	 *
	 * @remarks
	 * Every time a task fails, PowerQueues stores its retry count in Redis under a
	 * per-task key (generated by {@link attemptsKey}).  
	 * This counter allows the worker to determine:
	 *
	 * - how many times the task has already failed  
	 * - whether it should retry or send the task to the DLQ  
	 *
	 * `workerClearAttemptsTimeoutMs` specifies how long this retry counter should
	 * remain in Redis before it expires automatically.
	 *
	 * Why this matters:
	 * - prevents retry counters from building up indefinitely  
	 * - ensures memory is cleaned for tasks long after completion  
	 * - avoids stale retry state affecting future tasks with the same ID  
	 *
	 * Default value `86400000` (24 hours) works well for most workloads because:
	 * - it keeps retry info long enough for debugging  
	 * - it automatically cleans up old state overnight  
	 *
	 * You may want to **reduce** this value if:
	 * - you process millions of tasks daily  
	 * - strict memory efficiency is required  
	 *
	 * Or **increase** it if:
	 * - you run long-lived jobs  
	 * - you need extended visibility into retry behavior  
	 *
	 * @example
	 * ```ts
	 * // Keep retry counters for 6 hours instead of 24
	 * queues.workerClearAttemptsTimeoutMs = 21600000;
	 * ```
	 *
	 * @property workerClearAttemptsTimeoutMs
	 * TTL for retry-attempt counters stored for failing tasks (in milliseconds).
	 */
	public readonly workerClearAttemptsTimeoutMs: number = 86400000;

	/**
	 * Hook that runs **after tasks are read from Redis** but **before they are executed**.
	 *
	 * You can override this method to:
	 * - filter tasks (drop some of them),
	 * - reorder tasks (change processing order),
	 * - enrich/normalize payloads,
	 * - add logging/metrics.
	 *
	 * Each item in `data` is a tuple with the following structure:
	 * - `[0] id: string` – Redis stream entry ID (e.g. `"1719935620000-0"`).
	 * - `[1] payload: any[]` – decoded task payload.  
	 *   - For JSON payloads this is usually the parsed object or its fields.
	 * - `[2] createdAt: number` – UNIX timestamp in milliseconds when the task was created.
	 * - `[3] job: string` – job identifier used to group tasks added in one batch.
	 * - `[4] idemKey: string` – idempotency key used by the worker to avoid duplicate execution.
	 *
	 * Important notes:
	 * - If you **return a non-empty array**, it will be passed to the internal `execute()` method.
	 * - If you **return `undefined`, `null` or an empty array**, the original `data` array
	 *   will be used instead.
	 * - If you want to **remove only some tasks**, return a filtered array with the
	 *   remaining tuples.
	 * - If you need to change a task, you can either:
	 *   - mutate the objects inside `data` in place, or
	 *   - return a new array with modified tuples.
	 *
	 * Keep this method fast and non-blocking: it is called on every worker loop
	 * iteration before executing tasks.
	 *
	 * @param data - Array of raw task tuples selected from the stream for this iteration.
	 * @returns
	 * - The array of tasks to execute (filtered/reordered/modified), or
	 * - `undefined` / `null` / empty array to use the original `data` as-is.
	 *
	 * @example
	 * // Example: filter out tasks with invalid payloads and log them
	 * async onSelected(
	 *   data: Array<[string, any[], number, string, string]>,
	 * ) {
	 *   const valid: Array<[string, any[], number, string, string]> = [];
	 *
	 *   for (const [id, payload, createdAt, job, idemKey] of data) {
	 *     const isOk = payload && typeof payload === 'object' && payload['type'];
	 *
	 *     if (isOk) {
	 *       valid.push([id, payload, createdAt, job, idemKey]);
	 *     } else {
	 *       // you can send invalid tasks to DLQ / log / metrics here
	 *       // await this.addTasks(`${this.stream}:invalid`, [{ payload: { id, payload, createdAt, job, idemKey } }]);
	 *     }
	 *   }
	 *
	 *   return valid;
	 * }
	 */
	async onSelected(data: Array<[ string, any[], number, string, string ]>) {
		return data;
	}

	/**
	 * Main **task handler** – this is where you put your business logic.
	 *
	 * The worker calls `onExecute()` **for every task that should be processed**:
	 * - For normal tasks (without idempotency key) it is invoked directly from `executeProcess()`.
	 * - For idempotent tasks it is invoked inside the idempotency flow (`idempotency()`).
	 *
	 * If this method:
	 * - **resolves successfully** (no error thrown) – the task is considered **done**:
	 *   - `onSuccess()` will be called,
	 *   - the task will be **ACKed** and optionally **removed** from the stream,
	 *   - attempts counter is **not** increased.
	 * - **throws an error** – the task is considered **failed**:
	 *   - the attempts counter is incremented,
	 *   - `onRetry()` and `onError()` are called,
	 *   - if attempts exceed `workerMaxRetries`, the task is moved to **DLQ** (`<stream>:dlq`).
	 *
	 * Parameters:
	 * - `id` – Redis stream ID of this entry (e.g. `"1719935620000-0"`).
	 * - `payload` – task payload already decoded by the queue:
	 *   - If you pushed an object `{ foo: 'bar' }` – here you usually get the same object.
	 *   - If JSON decode failed, you may get a raw string or other value.
	 * - `createdAt` – UNIX timestamp in milliseconds when the task was originally created.
	 * - `job` – job identifier for the batch this task was added with (used for grouping/statistics).
	 * - `key` – idempotency key (empty string if idempotency is not used for this task).
	 * - `attempt` – current attempt number **before** this execution:
	 *   - `0` – first try,
	 *   - `1` – second try, etc.
	 *
	 * Important:
	 * - Always treat this method as **async and potentially retried**:
	 *   - Write idempotent business logic where possible (safe to run twice).
	 *   - Use `key` + `id` if you need extra deduplication on your side.
	 * - Do **not** manually ACK or delete messages in Redis here – the queue
	 *   does this for you via `Approve` script.
	 * - Long-running tasks are supported – the worker periodically updates
	 *   lock/start keys via the heartbeat mechanism while this method runs.
	 *
	 * @param id - Redis stream entry ID of the task.
	 * @param payload - Decoded task payload (business data to process).
	 * @param createdAt - Task creation time in milliseconds since UNIX epoch.
	 * @param job - Job identifier used to group tasks added in one batch.
	 * @param key - Idempotency key for this task (empty string if not used).
	 * @param attempt - Current attempt number (0 for first execution).
	 *
	 * @example
	 * // Example: simple HTTP call with retry-friendly error handling
	 * async onExecute(
	 *   id: string,
	 *   payload: any,
	 *   createdAt: number,
	 *   job: string,
	 *   key: string,
	 *   attempt: number,
	 * ) {
	 *   // 1. Validate payload shape
	 *   if (!payload || typeof payload.url !== 'string') {
	 *     // Throwing will mark this attempt as failed and trigger retry/DLQ logic
	 *     throw new Error('Invalid payload: "url" is required');
	 *   }
	 *
	 *   // 2. Add some logging/metrics (optional)
	 *   // console.log('[queue] executing task', { id, job, attempt, url: payload.url });
	 *
	 *   // 3. Execute business logic (can be any async operation)
	 *   const response = await fetch(payload.url, {
	 *     method: 'POST',
	 *     body: JSON.stringify(payload.body ?? {}),
	 *     headers: { 'Content-Type': 'application/json' },
	 *   });
	 *
	 *   if (!response.ok) {
	 *     // Non-2xx status: let the queue retry this task
	 *     throw new Error(`Request failed with status ${response.status}`);
	 *   }
	 *
	 *   // 4. Optionally process response or update other systems
	 *   // const data = await response.json();
	 *   // await this.someService.saveResult(id, data);
	 *
	 *   // No return value is required – resolving without error means "success".
	 * }
	 */
	async onExecute(id: string, payload: any, createdAt: number, job: string, key: string, attempt: number) {
	}

	/**
	 * Hook that runs **after the batch of tasks has been executed**, but **before ACK**.
	 *
	 * Call order inside the worker loop:
	 * 1. Tasks are selected from Redis (`select()` / `selectStuck()` / `selectFresh()`).
	 * 2. `onSelected()` is called (you can filter/reorder tasks there).
	 * 3. Each task is processed via `executeProcess()` → `onExecute()`, idempotency, retries, DLQ, etc.
	 * 4. **`onReady()` is called with the same `tasks` array used for execution.**
	 * 5. Successfully processed task IDs are ACKed via `approve()` (XACK + optional XDEL).
	 *
	 * Use this method for **post-batch actions**, for example:
	 * - logging summary of the batch,
	 * - updating metrics (processed count, errors, latency),
	 * - emitting events/notifications,
	 * - flushing any in-memory buffers used during `onExecute()`.
	 *
	 * Each item in `data` is a tuple with this structure:
	 * - `[0] id: string` – Redis stream entry ID (e.g. `"1719935620000-0"`).
	 * - `[1] payload: any[]` – decoded and normalized payload used by `onExecute()`.
	 * - `[2] createdAt: number` – UNIX timestamp in milliseconds when the task was created.
	 * - `[3] job: string` – job identifier that groups tasks added in one batch.
	 * - `[4] idemKey: string` – idempotency key (can be an empty string).
	 *
	 * Important:
	 * - This method is called **even if some tasks failed** inside the batch:
	 *   - failures are already handled (retries, DLQ) by the time `onReady()` runs.
	 * - If `onReady()` throws, the error is caught and passed to `onBatchError()`.
	 * - Do **not** modify Redis stream directly here (no manual XACK/XDEL) –
	 *   the queue will ACK successful IDs right after this hook.
	 *
	 * Keep this method **fast and side-effect friendly**: it should not block
	 * the worker for a long time, but it is a good place for final "batch-level"
	 * operations.
	 *
	 * @param data - Array of task tuples that were just processed in this batch.
	 *
	 * @example
	 * // Example: simple batch logging
	 * async onReady(
	 *   data: Array<[string, any[], number, string, string]>,
	 * ) {
	 *   const total = data.length;
	 *   const jobs = new Set<string>();
	 *
	 *   for (const [, , , job] of data) {
	 *     if (job) jobs.add(job);
	 *   }
	 *
	 *   // You can send this to your logger / metrics system
	 *   // console.log('[queue] batch ready', { total, jobs: Array.from(jobs) });
	 * }
	 */
	async onReady(data: Array<[ string, any[], number, string, string ]>) {
	}

	/**
	 * Hook that runs **after a task has been executed successfully**, but **before it is ACKed**.
	 *
	 * Call order for a successful task (simplified):
	 * 1. Task is selected from Redis (`select()`).
	 * 2. `onExecute()` runs your main business logic.
	 * 3. If `onExecute()` finishes without throwing:
	 *    - internal `success()` is called,
	 *    - job-level counters may be updated,
	 *    - **`onSuccess()` is called (this method).**
	 * 4. The task ID is added to the list of successfully processed IDs.
	 * 5. Later, the batch of IDs is ACKed (and optionally deleted) by `approve()`.
	 *
	 * Use this method when you want to:
	 * - log successful executions,
	 * - send events/notifications (e.g. WebSocket, message bus),
	 * - update external metrics / audit logs,
	 * - trigger follow-up actions that should only happen **if the task really succeeded**.
	 *
	 * Parameters:
	 * - `id` – Redis stream entry ID (e.g. `"1719935620000-0"`).
	 * - `payload` – the same decoded payload that was passed to `onExecute()`.
	 * - `createdAt` – UNIX timestamp in milliseconds when the task was created.
	 * - `job` – job identifier used to group tasks added in one batch.
	 * - `key` – idempotency key (empty string if idempotency was not used).
	 *
	 * Notes:
	 * - This hook is **not** retried: if it throws, the error is handled via `onBatchError()`,
	 *   but the main task is still considered successful (it already passed `onExecute()`).
	 * - Keep this method **side-effect safe**: avoid throwing unless it is really necessary.
	 * - Do **not** ACK, delete, or requeue messages here – the queue handles that automatically.
	 *
	 * @param id - Redis stream entry ID of the successfully processed task.
	 * @param payload - Decoded payload that was handled in `onExecute()`.
	 * @param createdAt - Task creation time in milliseconds since UNIX epoch.
	 * @param job - Job identifier for this task batch.
	 * @param key - Idempotency key used for this task (or empty string).
	 *
	 * @example
	 * // Example: log success and send a simple metric
	 * async onSuccess(
	 *   id: string,
	 *   payload: any,
	 *   createdAt: number,
	 *   job: string,
	 *   key: string,
	 * ) {
	 *   const durationMs = Date.now() - createdAt;
	 *
	 *   // 1. Log success (to console, logger, etc.)
	 *   // console.log('[queue] success', { id, job, key, durationMs });
	 *
	 *   // 2. Update metrics / monitoring
	 *   // await this.metrics.inc('queue_task_success_total', { job });
	 *   // await this.metrics.observe('queue_task_duration_ms', durationMs, { job });
	 *
	 *   // 3. Optionally notify other services
	 *   // await this.events.emit('task.success', { id, job, payload });
	 * }
	 */
	async onSuccess(id: string, payload: any, createdAt: number, job: string, key: string) {
	}

	/**
	 * Batch-level error hook.
	 *
	 * This method is called when **something goes wrong on the batch level**, for example:
	 * - an error is thrown inside `execute()` while processing a batch,
	 * - an error happens in `onReady()` (post-batch hook),
	 * - an unexpected error is thrown inside the main consumer loop.
	 *
	 * It is a good place to:
	 * - log unexpected errors in one central place,
	 * - send alerts/notifications to monitoring,
	 * - decide whether to stop the worker (`this.abort.abort()`),
	 * - inspect which tasks were affected (when `tasks` is provided).
	 *
	 * Parameters:
	 * - `err` – the error object that was thrown (can be anything: Error, string, etc.).
	 * - `tasks` – **optional** array of task tuples from the current batch:
	 *   - Only passed when the error is related to a specific batch
	 *     (e.g. during `execute()` or `onReady()`).
	 *   - May be `undefined` when the error happens before tasks are selected.
	 *
	 * When present, each item in `tasks` has the structure:
	 * - `[0] id: string` – Redis stream entry ID.
	 * - `[1] payload: any[]` – decoded payload passed later to `onExecute()`.
	 * - `[2] createdAt: number` – creation time in ms since UNIX epoch.
	 * - `[3] job: string` – job identifier for the batch.
	 * - `[4] idemKey: string` – idempotency key (can be empty string).
	 *
	 * Important:
	 * - **Do not rethrow** from this method – the error is already caught.
	 *   If you throw again, it will only add noise and not help recovery.
	 * - Tasks from the batch may be in different states:
	 *   - some may already be processed and ACKed,
	 *   - some may still be pending in the stream and will be picked up later.
	 * - Use this hook only for **side effects** (logging, metrics, alerts),
	 *   not for low-level queue control.
	 *
	 * @param err - The error that occurred during batch processing.
	 * @param tasks - Optional list of tasks related to this error (if available).
	 *
	 * @example
	 * // Example: log batch error and stop worker on critical failures
	 * async onBatchError(
	 *   err: any,
	 *   tasks?: Array<[string, any[], number, string, string]>,
	 * ) {
	 *   const message = err instanceof Error ? err.message : String(err);
	 *
	 *   // 1. Basic logging
	 *   // console.error('[queue] batch error', { message, tasksCount: tasks?.length ?? 0 });
	 *
	 *   // 2. Send error to monitoring/alerting system
	 *   // await this.alerts.notify('queue.batch_error', {
	 *   //   message,
	 *   //   tasksCount: tasks?.length ?? 0,
	 *   // });
	 *
	 *   // 3. Optional: stop worker on very serious errors
	 *   // if (message.includes('OutOfMemory')) {
	 *   //   this.abort.abort();
	 *   // }
	 * }
	 */
	async onBatchError(err: any, tasks?: Array<[ string, any[], number, string, string ]>) {
	}

	/**
	 * Hook that runs when a **single task fails** during execution.
	 *
	 * This method is called **after**:
	 * - `onExecute()` throws an error,
	 * - the attempt counter is increased,
	 * - `onRetry()` is called,
	 * - **but before** the queue decides whether the task will be retried again
	 *   or moved to DLQ (dead-letter queue).
	 *
	 * You can use this hook for:
	 * - logging a failure of an individual task,
	 * - sending alerts/metrics for failed attempts,
	 * - storing error details in your system,
	 * - instrumenting monitoring dashboards,
	 * - collecting statistics for debugging or analytics.
	 *
	 * Parameters:
	 * - `err` – Error thrown from `onExecute()` (may be any type: Error instance, string, etc.).
	 * - `id` – Redis stream entry ID of the failed task.
	 * - `payload` – Decoded payload object used in `onExecute()`.
	 * - `createdAt` – Creation timestamp of the task in milliseconds.
	 * - `job` – Job identifier that groups tasks added in the same batch.
	 * - `key` – Idempotency key (empty if not used).
	 * - `attempt` – Attempt number **after incrementing**, so:
	 *   - `1` = first retry,
	 *   - `2` = second retry, etc.
	 *
	 * Notes:
	 * - Do **not** rethrow errors from this method — the queue has already handled the failure.
	 * - The task may still be retried after this hook if `attempt < workerMaxRetries`.
	 * - When `attempt >= workerMaxRetries`:
	 *   - The queue moves the task to DLQ (`<stream>:dlq`),
	 *   - `onError()` is still called before that happens.
	 * - Do **not** ACK/Delete or requeue messages here — the queue will handle that.
	 *
	 * @param err - The error that occurred during execution.
	 * @param id - Redis stream entry ID.
	 * @param payload - Decoded payload originally passed to `onExecute()`.
	 * @param createdAt - Task creation time in milliseconds.
	 * @param job - Batch/job identifier for the task.
	 * @param key - Idempotency key (or empty string).
	 *
	 * @example
	 * // Example: basic logging + metrics
	 * async onError(
	 *   err: any,
	 *   id: string,
	 *   payload: any,
	 *   createdAt: number,
	 *   job: string,
	 *   key: string,
	 *   attempt: number,
	 * ) {
	 *   const msg = err instanceof Error ? err.message : String(err);
	 *
	 *   // 1. Log failure
	 *   // console.warn('[queue] task failed', {
	 *   //   id, job, key, attempt, error: msg, payload,
	 *   // });
	 *
	 *   // 2. Send failure metric
	 *   // await this.metrics.inc('queue_task_fail_total', { job, attempt });
	 *
	 *   // 3. Optional: store failure details for debugging
	 *   // await this.audit.save({
	 *   //   id, job, attempt, error: msg, payload, createdAt,
	 *   // });
	 * }
	 */
	async onError(err: any, id: string, payload: any, createdAt: number, job: string, key: string) {
	}

	/**
	 * Hook that runs when a **task fails but will be retried**.
	 *
	 * This method is called immediately after:
	 * - `onExecute()` throws an error,
	 * - the internal attempt counter is incremented,
	 * - **before** `onError()` and before deciding whether the task goes to DLQ.
	 *
	 * Use this hook for logic that is **specifically about retries**, such as:
	 * - logging retry attempts separately from final failures,
	 * - applying exponential backoff logic (if you want custom behavior),
	 * - collecting retry metrics,
	 * - tracking repeated failures,
	 * - triggering "early warning" alerts when retry counts start increasing.
	 *
	 * Parameters:
	 * - `err` – The error thrown by `onExecute()` (any type).
	 * - `id` – Redis stream entry ID of the task.
	 * - `payload` – Decoded payload passed to `onExecute()`.
	 * - `createdAt` – Timestamp (ms) when the task was originally created.
	 * - `job` – Job identifier grouping tasks from the same batch.
	 * - `key` – Idempotency key (empty string if not used).
	 * - `attempt` – Current retry index **after incrementing**, so:
	 *   - `1` = first retry,
	 *   - `2` = second retry,
	 *   - etc.
	 *
	 * Notes:
	 * - This method is only called for tasks that **will be retried**.
	 *   (If `attempt >= workerMaxRetries`, the queue still calls `onRetry()`,
	 *    but the next step is DLQ transfer.)
	 * - Do **not throw** errors here. The retry process is already ongoing.
	 * - Do **not** ACK, delete, or requeue tasks manually — the queue handles everything.
	 * - Keep this hook fast — the worker often processes dozens/hundreds of tasks per batch.
	 *
	 * @param err - The error that triggered a retry.
	 * @param id - Redis stream entry ID.
	 * @param payload - Decoded payload for the task.
	 * @param createdAt - Creation timestamp of the task in ms.
	 * @param job - Job identifier for this task.
	 * @param key - Idempotency key used for this task (or empty).
	 * @param attempt - Attempt number after incrementing.
	 *
	 * @example
	 * // Example: logging retries with exponential backoff info
	 * async onRetry(
	 *   err: any,
	 *   id: string,
	 *   payload: any,
	 *   createdAt: number,
	 *   job: string,
	 *   key: string,
	 *   attempt: number,
	 * ) {
	 *   const errorMsg = err instanceof Error ? err.message : String(err);
	 *
	 *   // 1. Log retry attempt
	 *   // console.log('[queue] retrying task', {
	 *   //   id, job, key, attempt, error: errorMsg,
	 *   // });
	 *
	 *   // 2. Track metrics for retries
	 *   // await this.metrics.inc('queue_task_retry_total', { job, attempt });
	 *
	 *   // 3. Optional: custom backoff hints or adaptive behavior
	 *   // const backoffMs = Math.min(5000, attempt * 250);
	 *   // console.log(`Next retry scheduled in about ~${backoffMs}ms`);
	 * }
	 */
	async onRetry(err: any, id: string, payload: any, createdAt: number, job: string, key: string, attempt: number) {
	}

	/**
	 * Starts the queue worker: initializes the consumer group and begins the
	 * infinite processing loop.
	 *
	 * This is the **entrypoint** for running a worker instance.  
	 * Calling `runQueue()`:
	 * 1. Ensures the Redis consumer group exists (creates it if needed).
	 * 2. Starts the main worker loop via `consumerLoop()`.
	 * 3. The worker will continue running until `this.abort.abort()` is called.
	 *
	 * Typical usage:
	 * ```ts
	 * const queue = new MyQueue(redisClient);
	 * queue.stream = 'events';
	 * queue.group = 'worker-1';
	 * queue.consumerHost = 'api-server';
	 *
	 * queue.runQueue(); // begins processing
	 * ```
	 *
	 * Internals and Flow:
	 * - **createGroup('0-0')**  
	 *   Ensures the Redis Stream Group exists.  
	 *   `'0-0'` means the worker will handle all historical messages that
	 *   haven't been acknowledged yet.
	 *
	 * - **consumerLoop()**  
	 *   Starts the continuous polling loop:
	 *   - reads tasks using `select()` / `selectStuck()` / `selectFresh()`,
	 *   - calls `onSelected()`,
	 *   - executes tasks (idempotent, retries, DLQ),
	 *   - calls `onReady()`,
	 *   - ACKs completed tasks.
	 *
	 * Stopping the worker:
	 * - The worker responds to `this.abort` signal.
	 * - Calling `this.abort.abort()` will break the loop at the next safe point.
	 *
	 * Important Notes:
	 * - This method should be called **once per worker instance**.
	 * - It never resolves unless the worker is stopped.
	 * - Safe to call in background processes, NestJS modules, or standalone runners.
	 * - Does not block the event loop — all work inside is asynchronous.
	 *
	 * @example
	 * // Full example: running a queue worker
	 * async function main() {
	 *   const redis = new IORedis(...);
	 *   const q = new MyQueue(redis);
	 *
	 *   q.stream = 'user:events';
	 *   q.group = 'analytics';
	 *   q.consumerHost = 'analytics-service';
	 *
	 *   console.log('Worker starting...');
	 *   await q.runQueue();       // does not return unless aborted
	 * }
	 * ```
	 */
	async runQueue() {
		await this.createGroup('0-0');
		await this.consumerLoop();
	}

	/**
	 * Main infinite worker loop responsible for:
	 * - selecting tasks from Redis,
	 * - running all lifecycle hooks,
	 * - executing tasks with retry/idempotency,
	 * - acknowledging completed messages.
	 *
	 * This loop continues running **until the worker is aborted**
	 * via `this.abort.abort()`.
	 *
	 * Execution Flow (per iteration):
	 * 1. **Check abort signal**  
	 *    If the worker has been stopped, exit the loop gracefully.
	 *
	 * 2. **Select tasks** via `select()`  
	 *    - First tries to claim *stuck* tasks (`selectStuck()`),
	 *    - Otherwise reads fresh messages (`selectFresh()`),
	 *    - Normalizes entries into tuples:
	 *      `[id, payload, createdAt, job, idemKey]`.
	 *
	 * 3. **onSelected() hook**  
	 *    Allows filtering, reordering, or modifying the selected tasks.
	 *    - If it returns a non-empty array → use that one.
	 *    - Otherwise → use the original tasks.
	 *
	 * 4. **Execute tasks** via `execute()`  
	 *    Handles:
	 *    - retries,
	 *    - DLQ logic,
	 *    - idempotency flow,
	 *    - onExecute / onRetry / onError / onSuccess,
	 *    - batching execution behavior.
	 *
	 * 5. **ACK completed tasks**  
	 *    - Uses Lua script `Approve` to XACK (+ optionally XDEL).
	 *    - Runs in batches for high throughput.
	 *
	 * 6. **Error handling**  
	 *    If anything inside the loop throws:
	 *    - error is passed to `onBatchError()`,
	 *    - worker waits briefly and continues.
	 *
	 * Behavior:
	 * - The loop uses very small waits (`wait(600)` ms) when no tasks are available.
	 * - Designed to be **lightweight**, **efficient**, and **fault-tolerant**.
	 * - Does not block the event loop thanks to async/await.
	 *
	 * Stopping the loop:
	 * - Call `this.abort.abort()`.
	 * - The loop checks for `signal.aborted` on each iteration and stops cleanly.
	 *
	 * Notes:
	 * - You normally never override this method.  
	 *   Use hooks (`onSelected`, `onExecute`, `onReady`, etc.) instead.
	 * - If the queue encounters unexpected Redis failures,
	 *   the loop will continue after logging via `onBatchError()`.
	 * - Safe to run in process managers (PM2, Docker, Kubernetes) as a long-running worker.
	 *
	 * @example
	 * // Typical worker startup
	 * async function start() {
	 *   const worker = new MyQueue(redis);
	 *   worker.stream = 'orders';
	 *   worker.group = 'billing';
	 *
	 *   await worker.runQueue(); // begins consumerLoop()
	 * }
	 */
	async consumerLoop() {
		const signal = this.signal();

		while (!signal?.aborted) {
			try {
				const tasks = await this.select();

				if (!isArrFilled(tasks)) {
					await wait(600);
					continue;
				}
				const tasksP = await this.onSelected(tasks);
				const ids = await this.execute(isArrFilled(tasksP) ? tasksP : tasks);

				if (isArrFilled(ids)) {
					await this.approve(ids);
				}
			}
			catch (err: any) {
				await this.batchError(err);
				await wait(600);
			}
		}
	}

	/**
	 * Adds a list of tasks to a Redis Stream in **batches**, using the `XAddBulk` Lua script.
	 *
	 * This is the main entry point for **producing** tasks into the queue.
	 * It:
	 * - wraps raw items into internal `Task` structures,
	 * - splits them into batches based on:
	 *   - `addingBatchTasksCount` (max tasks per batch),
	 *   - `addingBatchKeysLimit` (max total field/value pairs per batch),
	 * - sends each batch to Redis with a single `EVALSHA XAddBulk ...` call,
	 * - returns all created Stream IDs in the **same order** as input `data`.
	 *
	 * ### Input data format
	 * Each item in `data` is treated as a `Task`:
	 *
	 * - **Object payload (most common case)**  
	 *   ```ts
	 *   { payload: { ...businessFields } }
	 *   ```
	 *   Internally it becomes:
	 *   - `payload` is **JSON-serialized** and stored under `"payload"` field,
	 *   - `createdAt` (ms) is added,
	 *   - `job` (batch ID) is added,
	 *   - `idemKey` is added if `opts.idem === true`.
	 *
	 * - **Flat array of field/value pairs**  
	 *   ```ts
	 *   { flat: ['field1', 'value1', 'field2', 'value2', ...] }
	 *   ```
	 *   Internally it appends:
	 *   - `"createdAt"`, `"job"`,
	 *   - `"idemKey"` if `opts.idem === true`.
	 *
	 * If neither `payload` nor `flat` is present, an error is thrown.
	 *
	 * ### Trimming & stream options (`opts`)
	 * These options are passed to the Lua `XAddBulk` script:
	 *
	 * - `maxlen?: number` – soft or exact limit for stream length:
	 *   - combined with `approx` / `exact` to build `XADD ... MAXLEN [~|=] <maxlen>`.
	 * - `approx?: boolean` – use approximate trimming (`~`, default if `exact` is not set).
	 * - `exact?: boolean` – use exact trimming (`=`). If `true`, `approx` is ignored.
	 * - `trimLimit?: number` – optional `LIMIT <n>` for trimming.
	 * - `nomkstream?: boolean` – if `true`, adds `NOMKSTREAM` so the stream
	 *   is **not** created automatically when it does not exist.
	 * - `minidWindowMs?: number` – use `MINID` trimming instead of `MAXLEN`:
	 *   - compute `now - minidWindowMs` and trim entries older than that ID.
	 * - `minidExact?: boolean` – when using `MINID`, choose exact (`=`) vs approx (`~`) mode.
	 *
	 * ### Status tracking (`opts.status`)
	 * When `opts.status === true`:
	 * - a unique `job` ID is generated (UUID),
	 * - a Redis key `<queueName>:<job>:total` is set to `data.length`,
	 * - TTL for this key is `opts.statusTimeoutMs` (default `300000` ms = 5 minutes).
	 *
	 * This allows external systems to track:
	 * - how many tasks were enqueued for this job,
	 * - how many were later processed (via job-level counters in `success()`).
	 *
	 * ### Idempotency helper (`opts.idem`)
	 * When `opts.idem === true`:
	 * - each task gets an `idemKey` field if not already set:
	 *   - either using `entry.idemKey` or a generated UUID,
	 * - this key is later used in the worker’s idempotency flow.
	 *
	 * Validation & Errors:
	 * - If `data` is empty, it throws: `"Tasks is not filled."`.
	 * - If `queueName` is empty, it throws: `"Queue name is required."`.
	 *
	 * @param queueName - Name of the Redis Stream (queue) to push tasks into.
	 * @param data - Array of task-like objects:
	 *   - `{ payload: object }` or `{ flat: JsonPrimitiveOrUndefined[] }`.
	 * @param opts - Additional options for stream trimming, status tracking and idempotency.
	 *
	 * @returns Promise that resolves to an array of Stream entry IDs (`XADD` IDs),
	 *          in the same order as the input `data` array.
	 *
	 * @example
	 * // Example 1: push simple JSON payloads
	 * await queue.addTasks('events:email', [
	 *   { payload: { type: 'welcome', userId: 'u1' } },
	 *   { payload: { type: 'password_reset', userId: 'u2' } },
	 * ]);
	 *
	 * @example
	 * // Example 2: enable job status tracking and idempotency
	 * const ids = await queue.addTasks('billing:charges', [
	 *   { payload: { userId: 'u1', amount: 100 } },
	 *   { payload: { userId: 'u2', amount: 250 } },
	 * ], {
	 *   status: true,          // set <queueName>:<job>:total
	 *   statusTimeoutMs: 600000,
	 *   idem: true,            // auto attach idemKey to each task
	 *   maxlen: 1_000_000,     // keep last 1M entries
	 *   approx: true,          // use MAXLEN ~
	 * });
	 *
	 * // "ids" now contains the Redis stream IDs for each enqueued task
	 */
	async addTasks(queueName: string, data: any[], opts: AddTasksOptions = {}): Promise<string[]> {
		if (!isArrFilled(data)) {
			throw new Error('Tasks is not filled.');
		}
		if (!isStrFilled(queueName)) {
			throw new Error('Queue name is required.');
		}
		const job = uuid();
		const batches = this.buildBatches(data, job, opts.idem);
		const result: string[] = new Array(data.length);
		const promises: Array<() => Promise<void>> = [];
		let cursor = 0;
			
		for (const batch of batches) {
			const start = cursor;
			const end = start + batch.length;
				
			cursor = end;
			promises.push(async () => {
				const partIds = await this.xaddBatch(queueName, ...this.payloadBatch(batch, opts));
				
				for (let k = 0; k < partIds.length; k++) {
					result[start + k] = partIds[k];
				}
			});
		}
		const runners = Array.from({ length: promises.length }, async () => {
			while (promises.length) {
				const promise = promises.shift();

				if (promise) {
					await promise();
				}
			}
		});

		if (opts.status) {
			await (this.redis as any).set(`${queueName}:${job}:total`, data.length);
			await (this.redis as any).pexpire(`${queueName}:${job}:total`, opts.statusTimeoutMs || 300000);
		}
		await Promise.all(runners);
		return result;
	}

	/**
	 * Preloads Lua scripts into Redis and caches their SHA1 hashes for later use.
	 *
	 * This method should be called **before you start using the queue**, usually
	 * during application startup or worker initialization.
	 *
	 * Behaviour:
	 * - Converts each script name + body into an internal `SavedScript` entry
	 *   via `saveScript(name, code)`.
	 * - Sends the script body to Redis using `SCRIPT LOAD` inside `loadScript(...)`.
	 * - Stores the returned SHA1 hash in `scripts[name].codeReady`.
	 * - Later, all queue operations call scripts by SHA using `EVALSHA`, which is
	 *   faster and more efficient.
	 *
	 * Scripts loaded:
	 *
	 * - When `full === false` (default):
	 *   - Loads only:
	 *     - `"XAddBulk"` – high-throughput bulk `XADD` with trimming options (`MAXLEN`, `MINID`, etc.).
	 *   - This is enough if you only use `addTasks()` as a **producer** and do not
	 *     run workers in this process.
	 *
	 * - When `full === true`:
	 *   - Loads all queue-related scripts:
	 *     - `"XAddBulk"` – bulk enqueue of tasks to a stream.
	 *     - `"Approve"` – ACK + optional delete logic for processed entries.
	 *     - `"IdempotencyAllow"` – checks if a task with the same idempotency key
	 *       can start, based on done/lock/start keys.
	 *     - `"IdempotencyStart"` – marks a task as started and refreshes lock TTL.
	 *     - `"IdempotencyDone"` – marks a task as done and clears idempotency lock.
	 *     - `"IdempotencyFree"` – releases idempotency lock without marking as done.
	 *     - `"SelectStuck"` – finds and claims "stuck" tasks using `XAUTOCLAIM`
	 *       with a time budget and fallback to `XREADGROUP`.
	 *   - Use this in **worker processes** that execute tasks.
	 *
	 * Idempotency and safety:
	 * - Safe to call multiple times: scripts are cached in `this.scripts` and Redis
	 *   will return the same SHA for identical script bodies.
	 * - If Redis is temporarily unavailable, `loadScript()` retries a few times
	 *   before throwing.
	 *
	 * Requirements:
	 * - `this.redis` must be a connected Redis client that supports:
	 *   - `SCRIPT LOAD`
	 *   - `EVALSHA`
	 * - Call this method **before** you start calling operations that use scripts:
	 *   - `addTasks()` → `XAddBulk`
	 *   - worker internals → `Approve`, `Idempotency*`, `SelectStuck`
	 *
	 * @param full - If `true`, loads **all** Lua scripts used by the queue
	 *               (producer + worker). If `false`, loads only `XAddBulk`.
	 *
	 * @example
	 * // Producer-only service (just enqueues tasks)
	 * await queue.loadScripts(false); // or simply: await queue.loadScripts();
	 *
	 * @example
	 * // Full worker service (enqueues + processes tasks)
	 * await queue.loadScripts(true);
	 * await queue.runQueue();
	 */
	async loadScripts(full: boolean = false): Promise<void> {
		const scripts = full
			? [
				[ 'XAddBulk', XAddBulk ],
				[ 'Approve', Approve ],
				[ 'IdempotencyAllow', IdempotencyAllow ],
				[ 'IdempotencyStart', IdempotencyStart ],
				[ 'IdempotencyDone', IdempotencyDone ],
				[ 'IdempotencyFree', IdempotencyFree ],
				[ 'SelectStuck', SelectStuck ]
			]
			: [
				[ 'XAddBulk', XAddBulk ],
			];

		for (const [ name, code ] of scripts) {
			await this.loadScript(this.saveScript(name, code));
		}
	}

	/**
	 * Loads a **single Lua script** into Redis using `SCRIPT LOAD` with small
	 * built-in retry logic, and returns its SHA1 hash.
	 *
	 * This is a low-level helper used by `loadScripts()`:
	 * - It sends the raw Lua `code` to Redis.
	 * - Redis compiles and stores the script.
	 * - Redis returns a SHA1 hash string.
	 * - That SHA is later used with `EVALSHA` for fast execution.
	 *
	 * Retry behaviour:
	 * - The method will try up to **3 times**:
	 *   1. First attempt.
	 *   2. If it fails, waits a short random delay (10–50 ms) and tries again.
	 *   3. If it fails again, waits another short random delay and tries a third time.
	 * - If the **3rd attempt also fails**, the error is rethrown.
	 *
	 * This makes script loading more robust against:
	 * - short connectivity glitches,
	 * - Redis restarts,
	 * - slow network during startup.
	 *
	 * Important:
	 * - `this.redis` must support the `script('LOAD', code)` command
	 *   (ioredis compatible interface).
	 * - This method **does not** store the SHA anywhere by itself:
	 *   - it just returns the string,
	 *   - caller (e.g. `loadScripts()` / `saveScript()`) is responsible for
	 *     caching the SHA in `this.scripts[name].codeReady`.
	 *
	 * @param code - Raw Lua script source to load into Redis.
	 * @returns Promise that resolves to the SHA1 hash string returned by Redis.
	 * @throws Error if all 3 attempts to load the script fail.
	 *
	 * @example
	 * // Typical usage (simplified):
	 * const sha = await this.loadScript(XAddBulk);
	 * this.scripts['XAddBulk'] = { codeBody: XAddBulk, codeReady: sha };
	 */
	private async loadScript(code: string): Promise<string> {
		for (let i = 0; i < 3; i++) {
			try {
				return await (this.redis as any).script('LOAD', code);
			}
			catch (e) {
				if (i === 2) {
					throw e;
				}
				await new Promise((r) => setTimeout(r, 10 + Math.floor(Math.random() * 40)));
			}
		}
		throw new Error('Load lua script failed.');
	}

	/**
	 * Registers a script in the local in-memory cache and returns its body.
	 *
	 * This method:
	 * - Validates that the script body is not empty,
	 * - Saves the script under `this.scripts[name]`,
	 *   storing only its **uncompiled source code** (`codeBody`),
	 * - Does **not** load the script into Redis by itself,
	 * - Returns the raw script body so it can immediately be passed to
	 *   `loadScript()` which compiles and registers it inside Redis.
	 *
	 * Internal usage:
	 * - Called by `loadScripts()`, where each script is:
	 *   1. saved locally via `saveScript(name, codeBody)`,
	 *   2. then passed to `loadScript(codeBody)` to load it into Redis,
	 *   3. and the resulting SHA is stored back into `this.scripts[name].codeReady`.
	 *
	 * Why scripts are cached:
	 * - Redis needs the script SHA to run the script via `EVALSHA`.
	 * - If Redis restarts and forgets the script,
	 *   the queue detects `NOSCRIPT` errors and reloads the script automatically.
	 * - Keeping the source in memory ensures we always know how to reload it.
	 *
	 * Validation:
	 * - If `codeBody` is empty, undefined, or not a valid non-empty string,
	 *   an error `"Script body is empty."` is thrown.
	 *
	 * Note:
	 * - This does *not* compute or store `codeReady` (SHA1).  
	 *   That happens only when the script is actually loaded.
	 *
	 * @param name - Unique script identifier (e.g., `"XAddBulk"`, `"Approve"`).
	 * @param codeBody - Raw Lua script source.
	 *
	 * @returns The same `codeBody` string so it can be passed directly to `loadScript()`.
	 *
	 * @example
	 * // Inside loadScripts():
	 * const scriptBody = this.saveScript('XAddBulk', XAddBulk);
	 * const sha = await this.loadScript(scriptBody);
	 * this.scripts['XAddBulk'].codeReady = sha;
	 */
	private saveScript(name: string, codeBody: string): string {
		if (!isStrFilled(codeBody)) {
			throw new Error('Script body is empty.');
		}
		this.scripts[name] = { codeBody };

		return codeBody;
	}

	/**
	 * Executes a previously registered Lua script by **name** using `EVALSHA`,
	 * automatically loading or reloading it into Redis when needed.
	 *
	 * This is an internal helper used by higher-level queue operations,
	 * for example:
	 * - `xaddBatch()` → `"XAddBulk"`
	 * - `approve()` → `"Approve"`
	 * - idempotency helpers → `"IdempotencyAllow"`, `"IdempotencyStart"`, etc.
	 *
	 * Behaviour:
	 * 1. Looks up the script in `this.scripts[name]`.
	 * 2. If it is missing:
	 *    - If `defaultCode` is provided and non-empty, it will:
	 *      - call `saveScript(name, defaultCode)` to register it in memory.
	 *    - If `defaultCode` is not provided or empty, it throws:
	 *      `"Undefined script "<name>". Save it before executing."`
	 * 3. If `codeReady` (SHA1) is not available yet:
	 *    - calls `loadScript(codeBody)` to load the script into Redis,
	 *    - stores the SHA1 in `this.scripts[name].codeReady`.
	 * 4. Calls `EVALSHA` with:
	 *    - the cached SHA (`codeReady`),
	 *    - `keys.length` as the `numkeys` parameter,
	 *    - all `keys` followed by all `args`.
	 * 5. If Redis returns a `NOSCRIPT` error (e.g. after Redis restart):
	 *    - reloads the script using `loadScript(codeBody)`,
	 *    - retries `EVALSHA` once with the new SHA.
	 *
	 * This makes script execution:
	 * - **Fast** (uses `EVALSHA` instead of `EVAL`),
	 * - **Safe** (self-healing after Redis restart or flush),
	 * - **Convenient** (call by logical name, not by SHA).
	 *
	 * Parameters:
	 * - `name` – Logical name of the script in `this.scripts` (e.g. `"XAddBulk"`).
	 * - `keys` – Array of Redis keys passed to the script:
	 *   - First argument to `EVALSHA` after `numkeys`,
	 *   - The Lua script sees them via the global `KEYS` table.
	 * - `args` – Array of arguments (strings or numbers) passed **after** keys:
	 *   - Lua script sees them via the global `ARGV` table.
	 * - `defaultCode` – Optional raw Lua source:
	 *   - Used only when `this.scripts[name]` is not registered yet.
	 *   - Allows one-line "lazy" definition and execution.
	 *
	 * Return value:
	 * - Whatever the Lua script returns (type depends on script logic).
	 * - The value is passed through from `evalsha` as-is.
	 *
	 * Notes:
	 * - All numbers in `args` are passed to Redis as-is; Redis will convert them to strings.
	 * - For consistent behaviour, prefer passing `String(...)` yourself when in doubt.
	 * - This is a **low-level** method; public API usually calls it indirectly.
	 *
	 * @param name - Name of the script in the local cache (`this.scripts`).
	 * @param keys - Redis keys array, available as `KEYS` inside Lua.
	 * @param args - Arguments array (strings/numbers), available as `ARGV` inside Lua.
	 * @param defaultCode - Optional Lua source used if script is not yet registered.
	 *
	 * @returns A promise resolving to the result of the Lua script.
	 *
	 * @example
	 * // Example: calling XAddBulk indirectly (simplified)
	 * const res = await this.runScript(
	 *   'XAddBulk',
	 *   [queueName],                               // KEYS
	 *   this.payloadBatch(tasks, opts),           // ARGV[]
	 *   XAddBulk,                                 // default code if not yet saved
	 * );
	 */
	private async runScript(name: string, keys: string[], args: (string|number)[], defaultCode?: string) {
		if (!this.scripts[name]) {
			if (!isStrFilled(defaultCode)) {
				throw new Error(`Undefined script "${name}". Save it before executing.`);
			}
			this.saveScript(name, defaultCode);
		}
		if (!this.scripts[name].codeReady) {
			this.scripts[name].codeReady = await this.loadScript(this.scripts[name].codeBody);
		}
		try {
			return await (this.redis as any).evalsha(this.scripts[name].codeReady!, keys.length, ...keys, ...args);
		}
		catch (err: any) {
			if (String(err?.message || '').includes('NOSCRIPT')) {
				this.scripts[name].codeReady = await this.loadScript(this.scripts[name].codeBody);

				return await (this.redis as any).evalsha(this.scripts[name].codeReady!, keys.length, ...keys, ...args);
			}
			throw err;
		}
	}

	/**
	 * Low-level helper that sends a **single batch** of tasks to Redis Stream
	 * using the `XAddBulk` Lua script.
	 *
	 * In most cases you do **not** call this method directly.  
	 * Instead, you use the higher-level `addTasks()` API, which:
	 * - builds `Task` objects,
	 * - splits them into batches (`buildBatches()`),
	 * - converts each batch into `batches` arguments via `payloadBatch()`,
	 * - and finally calls `xaddBatch()` for each batch.
	 *
	 * Behaviour:
	 * - Calls `runScript('XAddBulk', [queueName], batches, XAddBulk)`.
	 * - `queueName` becomes `KEYS[1]` inside the Lua script.
	 * - `batches` array is passed as `ARGV` (already prepared by `payloadBatch()`).
	 * - Returns the array of created Stream entry IDs for this batch.
	 *
	 * Expected `batches` format (simplified):
	 * - `payloadBatch()` produces a flat list of strings:
	 *   - global options (maxlen, approx/exact, minidWindowMs, etc.),
	 *   - followed by repeated blocks:
	 *     - `<id>`, `<num_pairs>`, `<field1>`, `<value1>`, ..., `<fieldN>`, `<valueN>`.
	 * - The `XAddBulk` script interprets these values and performs multiple `XADD`
	 *   operations in a tight loop, optionally with trimming.
	 *
	 * Important:
	 * - `queueName` must be the **Redis Stream key** where tasks should be added.
	 * - `batches` must be generated by `payloadBatch()` or follow the same contract.
	 * - If you pass an incorrect ARGV format, the Lua script may fail or behave
	 *   unexpectedly.
	 *
	 * @param queueName - Redis Stream key used as the queue name.
	 * @param batches - Flat list of arguments for the `XAddBulk` Lua script
	 *                  (usually produced by `payloadBatch()`).
	 *
	 * @returns Promise resolving to an array of Redis Stream IDs created by this batch.
	 *
	 * @example
	 * // Typical internal usage (simplified from addTasks):
	 * const argv = this.payloadBatch(batchTasks, opts);
	 * const ids = await this.xaddBatch('my:stream', ...argv);
	 * // "ids" now contains one XADD ID per task in this batch
	 */
	private async xaddBatch(queueName: string, ...batches: string[]): Promise<string[]> {
		return await this.runScript('XAddBulk', [ queueName ], batches, XAddBulk);
	}

	/**
	 * Builds the **argument vector (`ARGV`) payload** for the `XAddBulk` Lua script.
	 *
	 * This method converts a batch of logical `Task` objects into a **flat string array**
	 * that is later passed to Redis as `...ARGV` for `EVALSHA` of `XAddBulk`.
	 *
	 * It does **not** talk to Redis directly – it only prepares data.
	 * The resulting array is consumed by {@link xaddBatch}.
	 *
	 * ### How it works
	 * 1. Adds global stream trim options (MAXLEN / MINID / NOMKSTREAM / LIMIT) to the header:
	 *    - `maxlen` → `XADD ... MAXLEN ~= maxlen`
	 *    - `approx` / `exact` → choose between `~` (approximate) or `=` (exact) trim
	 *    - `nomkstream` → `NOMKSTREAM` flag
	 *    - `trimLimit` → `LIMIT <trimLimit>` for trimming
	 *    - `minidWindowMs` + `minidExact` → `MINID` based trimming (time-based retention)
	 * 2. Encodes each task as:
	 *    - `id` – stream entry ID (e.g. `"*"`, or a specific ID string);
	 *    - `num_pairs` – number of field/value pairs;
	 *    - then `field1, value1, field2, value2, ...` as strings.
	 *
	 * ### Task input rules
	 * Each `Task` must be in one of two shapes:
	 *
	 * - **Object payload form**:
	 *   ```ts
	 *   { job: string; payload: Record<string, JsonPrimitiveOrUndefined>; ... }
	 *   ```
	 *   In this case, all `payload` entries are flattened into `[field, value, ...]`.
	 *
	 * - **Flat array form**:
	 *   ```ts
	 *   { job: string; flat: JsonPrimitiveOrUndefined[]; ... }
	 *   ```
	 *   Here `flat` is assumed to already contain interleaved field/value pairs.
	 *
	 * For `flat`:
	 * - Its length **must be even**, otherwise an error is thrown:
	 *   `"Property "flat" must contain an even number of realKeysLength (field/value pairs)."`
	 * - It must contain **at least one pair** – otherwise an error is thrown:
	 *   `"Task "flat" must contain at least one field/value pair."`
	 *
	 * If a task has neither a valid `payload` object nor a valid `flat` array,
	 * an error is thrown:
	 * - `"Task must have "payload" or "flat"."`
	 *
	 * ### ID and value encoding
	 * - `entry.id` is used if present; otherwise `"*"` (server-generated ID).
	 * - All field values are converted to strings:
	 *   - `null` / `undefined` → `""` (empty string),
	 *   - non-string primitives (numbers, booleans, etc.) → `String(value)`.
	 *
	 * ### Batching behaviour
	 * - This method assumes that `data` is a **single batch** (typically produced by `buildBatches`),
	 *   so it only encodes that batch into the `ARGV` format.
	 * - It **does not mutate** the input tasks (only reads from them).
	 *
	 * ### Options (`AddTasksOptions`)
	 * - `maxlen`        – Maximum allowed stream length (used with `MAXLEN`).
	 * - `approx`        – If `true` or `undefined`, use `~` (approximate) trimming (default);
	 *                      if `false` and `exact` is not set, no trim operator is sent.
	 * - `exact`         – If `true`, use `=` (exact) trimming and ignore `approx`.
	 * - `nomkstream`    – If `true`, pass `NOMKSTREAM` to avoid creating the stream automatically.
	 * - `trimLimit`     – Optional `LIMIT` for trimming operations; `0` means "no limit".
	 * - `minidWindowMs` – If > 0, switch from `MAXLEN` to **time-based retention** with `MINID`
	 *                      (keep entries newer than `<now - minidWindowMs>`).
	 * - `minidExact`    – When using `MINID`, if `true` use `=`, otherwise `~`.
	 *
	 * Note: `minidWindowMs` has higher priority than `maxlen`. If `minidWindowMs > 0`,
	 * the header will use `MINID` and **ignore** `MAXLEN`.
	 *
	 * ### Return value
	 * Returns a string array representing `ARGV` for the `XAddBulk` Lua script:
	 *
	 * ```text
	 * [
	 *   maxlen, approxFlag, n, exactFlag, nomkstreamFlag, trimLimit,
	 *   minidWindowMs, minidExactFlag,
	 *   id1, pairs1, field1_1, value1_1, field1_2, value1_2, ...,
	 *   id2, pairs2, field2_1, value2_1, ...
	 * ]
	 * ```
	 *
	 * @param data - Batch of tasks to be encoded into the Redis `XADD` payload.
	 *               Usually this is a single batch from {@link buildBatches}.
	 * @param opts - Stream trimming and behaviour options (MAXLEN / MINID / NOMKSTREAM / LIMIT).
	 * @returns String array that will be spread as `...ARGV` for the `XAddBulk` Lua script.
	 *
	 * @throws If a task has invalid `flat` length, no `payload`/`flat`, or contains 0 pairs.
	 *
	 * @example
	 * // Minimal batch with object payloads
	 * const argv = this.payloadBatch([
	 *   { job: 'email', payload: { to: 'user@example.com', subject: 'Hi' } },
	 *   { job: 'email', payload: { to: 'admin@example.com', subject: 'Report' } },
	 * ], { maxlen: 10000 });
	 *
	 * // Then:
	 * await this.xaddBatch('queues:emails', ...argv);
	 */
	private payloadBatch(data: Array<Task>, opts: AddTasksOptions): string[] {
		const maxlen = Math.max(0, Math.floor(opts?.maxlen ?? 0));
		const approx = opts?.exact ? 0 : (opts?.approx !== false ? 1 : 0);
		const exact = opts?.exact ? 1 : 0;
		const nomkstream = opts?.nomkstream ? 1 : 0;
		const trimLimit = Math.max(0, Math.floor(opts?.trimLimit ?? 0));
		const minidWindowMs = Math.max(0, Math.floor(opts?.minidWindowMs ?? 0));
		const minidExact = opts?.minidExact ? 1 : 0;
		const argv: string[] = [
			String(maxlen),
			String(approx),
			String(data.length),
			String(exact), 
			String(nomkstream),
			String(trimLimit),
			String(minidWindowMs),
			String(minidExact),
		];

		for (const item of data) {
			const entry: any = item;
			const id = entry.id ?? '*';
			let flat: JsonPrimitiveOrUndefined[];

			if ('flat' in entry && isArrFilled(entry.flat)) {
				flat = entry.flat;
					
				if (flat.length % 2 !== 0) {
					throw new Error('Property "flat" must contain an even number of realKeysLength (field/value pairs).');
				}
			}
			else if ('payload' in entry && isObjFilled(entry.payload)) {
				flat = [];
		
				for (const [ k, v ] of Object.entries(entry.payload)) {
					flat.push(k, v as any);
				}
			}
			else {
				throw new Error('Task must have "payload" or "flat".');
			}
			const pairs = flat.length / 2;

			if (isNumNZ(pairs)) {
				throw new Error('Task "flat" must contain at least one field/value pair.');
			}
			argv.push(String(id));
			argv.push(String(pairs));

			for (const token of flat) {
				argv.push(!token
					? ''
					: isStrFilled(token)
						? token
						: String(token));
			}
		}
		return argv;
	}

	/**
	 * Splits an array of logical tasks into **batches** that are safe to send to Redis
	 * in a single `XADD` bulk call.
	 *
	 * It also **normalizes** each task by:
	 * - injecting `createdAt` (if missing),
	 * - injecting `job` (the shared job/batch ID),
	 * - optionally injecting `idemKey` for idempotency.
	 *
	 * The output is a 2D array: `Task[][]`, where each inner array is a batch.
	 * These batches are later passed to {@link payloadBatch} and {@link xaddBatch}.
	 *
	 * ### Why batching is needed
	 * We want to:
	 * - avoid sending too many tasks in a single Lua execution;
	 * - avoid hitting Redis protocol / command argument limits;
	 * - keep each batch **reasonably small**, both in number of tasks and number of fields.
	 *
	 * This method uses two internal limits:
	 *
	 * - {@link addingBatchTasksCount} – max number of tasks per batch;
	 * - {@link addingBatchKeysLimit}  – max total number of field/value tokens per batch.
	 *
	 * Once either limit would be exceeded by adding the next task, a **new batch** is started.
	 *
	 * ### What this method does to each task
	 *
	 * For every input `task`:
	 *
	 * 1. Ensures `createdAt`:
	 *    - If `task.createdAt` is present, it is used as-is.
	 *    - Otherwise, `Date.now()` is used.
	 *
	 * 2. Wraps object payloads:
	 *    - If `task.payload` is a plain object (`isObj(entry.payload)`):
	 *      ```ts
	 *      entry = {
	 *        ...task,
	 *        payload: {
	 *          payload: JSON.stringify(task.payload),
	 *          createdAt,        // number (ms)
	 *          job,              // string (shared job ID)
	 *          idemKey?: string, // only if idem === true
	 *        },
	 *      };
	 *      ```
	 *    - This means Redis always stores a small envelope with:
	 *      - `payload`  – original payload serialized as JSON string;
	 *      - `createdAt` – timestamp in ms;
	 *      - `job` – job ID;
	 *      - `idemKey` – optional idempotency key when `idem` is `true`.
	 *
	 * 3. Extends flat payloads:
	 *    - If `task.flat` is an array:
	 *      ```ts
	 *      entry.flat.push('createdAt', String(createdAt));
	 *      entry.flat.push('job', job);
	 *      if (idem) {
	 *        entry.flat.push('idemKey', entry.idemKey || uuid());
	 *      }
	 *      ```
	 *    - Here we directly append new field/value pairs to the existing flat structure.
	 *
	 * 4. If `idem` (idempotency) is enabled:
	 *    - For object payloads, a new `idemKey` field is added to `payload`:
	 *      - Uses `task.idemKey` if provided,
	 *      - otherwise generates one via `uuid()`.
	 *    - For flat payloads, `idemKey` is appended to `flat` similarly.
	 *    - This key is later used by idempotency logic in {@link idempotency}.
	 *
	 * ### Batch splitting logic
	 *
	 * For each normalized `entry`:
	 *
	 * 1. Calculate how many tokens (field/value + meta) this task will consume using {@link keysLength}:
	 *    ```ts
	 *    const reqKeysLength = this.keysLength(entry);
	 *    ```
	 *
	 * 2. If there is already at least one task in the current `batch`, we check:
	 *    - `batch.length >= this.addingBatchTasksCount`, **or**
	 *    - `realKeysLength + reqKeysLength > this.addingBatchKeysLimit`
	 *
	 *    If either is true:
	 *    - push the current batch to `batches`,
	 *    - start a new batch,
	 *    - reset `realKeysLength`.
	 *
	 * 3. Push the current `entry` into the (possibly new) `batch` and update `realKeysLength`.
	 *
	 * 4. After the loop finishes, if the last `batch` is non-empty, it is also pushed into `batches`.
	 *
	 * ### Return value
	 *
	 * Returns an array of batches:
	 *
	 * ```ts
	 * [
	 *   [ entry1, entry2, ... ], // batch #1
	 *   [ entryX, entryY, ... ], // batch #2
	 *   ...
	 * ]
	 * ```
	 *
	 * Each `entry` is a `Task`-like object, but with `payload`/`flat` enriched
	 * with `createdAt`, `job` and optionally `idemKey`.
	 *
	 * ### When to use
	 *
	 * This method is used internally by {@link addTasks}:
	 *
	 * ```ts
	 * const job = uuid();
	 * const batches = this.buildBatches(data, job, opts.idem);
	 * for (const batch of batches) {
	 *   const argv = this.payloadBatch(batch, opts);
	 *   await this.xaddBatch(queueName, ...argv);
	 * }
	 * ```
	 *
	 * As a user of `PowerQueues`, you usually **do not call this directly**,
	 * but understanding it helps to reason about:
	 * - how many tasks go into one Redis script call,
	 * - how `createdAt`, `job`, and `idemKey` appear inside stream entries.
	 *
	 * @param tasks - Original list of tasks that should be added to the stream.
	 * @param job   - Shared job/batch ID used to tag all tasks in this operation.
	 * @param idem  - If `true`, generate/use `idemKey` for idempotent processing.
	 * @returns An array of task batches, respecting `addingBatchTasksCount`
	 *          and `addingBatchKeysLimit`.
	 */
	private buildBatches(tasks: Task[], job: string, idem?: boolean): Task[][] {
		const batches: Task[][] = [];
		let batch: Task[] = [],
			realKeysLength = 0;

		for (let task of tasks) {
			const createdAt = task?.createdAt || Date.now();
			let entry: any = task;

			if (isObj(entry.payload)) {
				entry = { 
					...entry, 
					payload: { 
						payload: JSON.stringify(entry.payload),  
						createdAt,
						job,
					}, 
				};

				if (idem) {
					entry.payload['idemKey'] = entry?.idemKey || uuid();
				}
			}
			else if (Array.isArray(entry.flat)) {
				entry.flat.push('createdAt');
				entry.flat.push(String(createdAt));
				entry.flat.push('job');
				entry.flat.push(job);

				if (idem) {
					entry.flat.push('idemKey');
					entry.flat.push(entry?.idemKey || uuid());
				}
			}
			const reqKeysLength = this.keysLength(entry);
			
			if (batch.length && (batch.length >= this.addingBatchTasksCount || realKeysLength + reqKeysLength > this.addingBatchKeysLimit)) {
				batches.push(batch); 
				batch = []; 
				realKeysLength = 0;
			}
			batch.push(entry);
			realKeysLength += reqKeysLength;
		}
		if (batch.length) {
			batches.push(batch);
		}
		return batches;
	}

	/**
	 * Calculates how many **tokens** (strings) a task will contribute to a Redis
	 * `XADD` command when encoded by {@link payloadBatch}.
	 *
	 * A *token* is any string argument that will eventually be sent to Redis:
	 * - the entry ID,
	 * - the number of field/value pairs,
	 * - each field name,
	 * - each field value.
	 *
	 * This method provides a rough but safe estimate used by {@link buildBatches}
	 * to ensure that a batch does not exceed {@link addingBatchKeysLimit}.
	 *
	 * ### How the count is computed
	 *
	 * The structure of every encoded task in `payloadBatch` is:
	 *
	 * ```
	 * [ id, num_pairs, field1, value1, field2, value2, ... ]
	 *   ↑    ↑
	 *   │    └── token #2: number of pairs (as string)
	 *   └── token #1: entry ID
	 * ```
	 *
	 * After these two tokens, each **field/value** pair contributes exactly **2 tokens**.
	 *
	 * So the final count is:
	 *
	 * ```
	 * 2 + (field_count × 2)
	 * ```
	 *
	 * Where:
	 * - The first `2` are: `id` + `num_pairs`
	 * - For object payloads, `field_count` is the number of keys in the payload object.
	 * - For flat payloads, `field_count` is `flat.length / 2`.
	 *
	 * ### What this method inspects
	 *
	 * It checks the task in the following order:
	 *
	 * 1. **Flat payload form** (`task.flat` exists and is non-empty):
	 *    - `flat.length` already contains *both* fields and values.
	 *    - Field count = `flat.length`.
	 *    - But since each logical pair is 2 elements, the token count becomes:
	 *      ```ts
	 *      return 2 + task.flat.length;
	 *      ```
	 *
	 * 2. **Object payload form** (`task.payload` is a plain object):
	 *    - Field count = number of object keys.
	 *    - Token count = `2 + Object.keys(payload).length * 2`
	 *
	 * 3. **Fallback** — rarely used:
	 *    - If neither `flat` nor valid `payload` exists, fall back to:
	 *      ```ts
	 *      return 2 + Object.keys(task as any).length * 2;
	 *      ```
	 *    - This is mainly defensive code and should rarely trigger.
	 *
	 * ### Why this matters
	 *
	 * `keysLength` is used internally by {@link buildBatches} to ensure that:
	 * - one batch will never send too many tokens to Redis at once,
	 * - bulk operations remain performant and safe,
	 * - we avoid hitting the Redis protocol argument limits or lua stack limits.
	 *
	 * Because each task has a different payload size, this dynamic calculation
	 * helps build balanced batches.
	 *
	 * ### Example
	 *
	 * ```ts
	 * const task = {
	 *   payload: {
	 *     to: 'user@example.com',
	 *     subject: 'Hello',
	 *     createdAt: 1700000000000,
	 *     job: 'abc123',
	 *   }
	 * };
	 *
	 * // Object payload has 4 fields → 4 × 2 = 8 tokens
	 * // Plus 2 header tokens → total = 10
	 * keysLength(task) === 10;
	 * ```
	 *
	 * @param task - A normalized task from {@link buildBatches}. Can use either
	 *               `payload` (object form) or `flat` (array form).
	 * @returns The total number of string tokens this task will produce during encoding.
	 */
	private keysLength(task: Task): number {
		if ('flat' in task && Array.isArray(task.flat) && task.flat.length) {
			return 2 + task.flat.length;
		}
		if ('payload' in task && isObj((task as any).payload)) {
			return 2 + Object.keys((task as any).payload).length * 2;
		}
		return 2 + Object.keys(task as any).length * 2;
	}

	/**
	 * Builds a **Redis key** used to track how many times a specific message
	 * (stream entry) has been retried by the worker.
	 *
	 * The key format is:
	 *
	 * ```text
	 * q:<sanitized-stream-name>:attempts:<sanitized-id>
	 * ```
	 *
	 * Where:
	 * - `<sanitized-stream-name>` is `this.stream` with all characters
	 *   except letters, digits, `_`, `:`, and `-` replaced by `_`.
	 * - `<sanitized-id>` is the Redis entry ID (e.g. `1700000000000-0`)
	 *   sanitized in the same way.
	 *
	 * Sanitizing ensures:
	 * - the key is always a **safe Redis key** (no spaces, weird symbols, etc.);
	 * - all attempt counters follow a predictable pattern that can be inspected
	 *   or cleaned up manually if needed.
	 *
	 * This helper is used internally by:
	 * - {@link incrAttempts} – to increment the retry counter;
	 * - {@link getAttempts} – to read the current retry counter;
	 * - {@link clearAttempts} – to remove the counter once the task is done or dead-lettered.
	 *
	 * ### Example key
	 *
	 * If:
	 * ```ts
	 * this.stream = 'queues:payments';
	 * id          = '1700000000000-0';
	 * ```
	 *
	 * Then:
	 * ```ts
	 * attemptsKey(id) === 'q:queues:payments:attempts:1700000000000-0';
	 * ```
	 *
	 * @param id - Redis stream entry ID for which we track retry attempts.
	 * @returns A namespaced, sanitized Redis key string for the attempts counter.
	 */
	private attemptsKey(id: string): string {
		const safeStream = this.stream.replace(/[^\w:\-]/g, '_');
		const safeId = id.replace(/[^\w:\-]/g, '_');

		return `q:${safeStream}:attempts:${safeId}`;
	}

	/**
	 * Increments the **retry-attempt counter** for a specific task and returns
	 * the updated value.
	 *
	 * This method is used inside the worker's execution flow to track how many
	 * times a task has failed and been retried. Once a task exceeds
	 * `workerMaxRetries`, it is moved to the DLQ (dead-letter queue).
	 *
	 * ### What this method does
	 *
	 * 1. Builds the counter key using {@link attemptsKey}:
	 *    ```ts
	 *    const key = this.attemptsKey(id);
	 *    ```
	 *
	 * 2. Calls `INCR` on that Redis key:
	 *    ```ts
	 *    const attempts = await redis.incr(key);
	 *    ```
	 *
	 *    This yields:
	 *    - `1` for the first failure,
	 *    - `2` for the second,
	 *    - ...and so on.
	 *
	 * 3. Sets a TTL (in milliseconds) on the counter via `PEXPIRE`:
	 *    ```ts
	 *    await redis.pexpire(key, workerClearAttemptsTimeoutMs);
	 *    ```
	 *
	 *    This ensures attempt counters **automatically expire** and do not
	 *    accumulate forever in Redis.
	 *
	 *    The default is:
	 *    - `workerClearAttemptsTimeoutMs = 86400000` (24 hours)
	 *
	 * 4. Returns the incremented value.
	 *
	 * ### Error behaviour
	 *
	 * - If Redis throws an error (rare), the error is swallowed and the method
	 *   returns `0` (meaning "no attempt information available").  
	 * - This prevents a Redis hiccup from crashing the worker.
	 *
	 * ### When it is used
	 *
	 * - Each time {@link onExecute} throws an error.
	 * - Each time idempotent task processing fails inside {@link idempotency}.
	 * - Used together with:
	 *   - {@link getAttempts} – to check the number before executing;
	 *   - {@link clearAttempts} – to reset after DLQ or success.
	 *
	 * ### Example
	 *
	 * ```ts
	 * const attempt = await this.incrAttempts("1700000000000-0");
	 * console.log(attempt); // 1, 2, 3...
	 * ```
	 *
	 * @param id - Redis stream entry ID whose retry attempts must be incremented.
	 * @returns The updated attempt count, or `0` if something went wrong.
	 */
	private async incrAttempts(id: string): Promise<number> {
		try {
			const key = this.attemptsKey(id);
			const attempts = await (this.redis as any).incr(key);

			await (this.redis as any).pexpire(key, this.workerClearAttemptsTimeoutMs);
			return attempts;
		}
		catch (err) {
		}
		return 0;
	}

	/**
	 * Reads the current **retry-attempt counter** for a given task.
	 *
	 * This method is the counterpart of {@link incrAttempts}.  
	 * It is used before executing a task to know how many times it has already failed.
	 *
	 * ### How it works
	 *
	 * 1. Builds the Redis key via {@link attemptsKey}:
	 *    ```ts
	 *    const key = this.attemptsKey(id);
	 *    ```
	 *
	 * 2. Performs a simple `GET`:
	 *    ```ts
	 *    const v = await redis.get(key);
	 *    ```
	 *
	 * 3. Converts the result to a number:
	 *    - If the key does not exist, returns `0`.
	 *    - If the value exists but is not a number, `Number(v || 0)` still returns `0`.
	 *
	 * ### When it is used
	 *
	 * - Inside {@link executeProcess} and {@link idempotency}:
	 *   ```ts
	 *   const attempt = await this.getAttempts(id);
	 *   ```
	 *
	 *   This tells the queue worker whether the task is on its:
	 *   - 0th attempt (first execution),
	 *   - 1st retry,
	 *   - 2nd retry,
	 *   - etc.
	 *
	 * - It is also useful for logging, metrics, or custom retry logic if you override hooks like:
	 *   - {@link onExecute}
	 *   - {@link onRetry}
	 *   - {@link onError}
	 *
	 * ### Error behaviour
	 *
	 * - `GET` can return `null` → method returns `0`.
	 * - All values are safely coerced using `Number(...)`.
	 * - This method does **not** throw.
	 *
	 * ### Example
	 *
	 * ```ts
	 * const n = await this.getAttempts("1700000000000-0");
	 * console.log(n); // 0, 1, 2, 3...
	 * ```
	 *
	 * @param id - Redis stream entry ID whose retry counter should be retrieved.
	 * @returns The current retry-attempt count (defaults to `0`).
	 */
	private async getAttempts(id: string): Promise<number> {
		const key = this.attemptsKey(id);
		const v = await (this.redis as any).get(key);

		return Number(v || 0);
	}

	/**
	 * Deletes the **retry-attempt counter** for a specific task.
	 *
	 * This method is called when:
	 * - a task **succeeds** and should no longer carry retry history;
	 * - a task **reaches max retries** and is sent to the DLQ (dead-letter queue);
	 * - idempotent execution completes and the attempt counter should be reset.
	 *
	 * Clearing the attempts key ensures that:
	 * - Redis does not accumulate stale counter keys,
	 * - a future task with the same entry ID (rare but possible in tests)
	 *   starts with a clean retry count,
	 * - monitoring systems do not misinterpret old failures as new ones.
	 *
	 * ### How it works
	 *
	 * 1. Builds the Redis key from the stream and ID using {@link attemptsKey}:
	 *    ```ts
	 *    const key = this.attemptsKey(id);
	 *    ```
	 *
	 * 2. Executes:
	 *    ```ts
	 *    await redis.del(key);
	 *    ```
	 *
	 * The method is fully safe:
	 * - If the key does not exist → no error.
	 * - If Redis throws (e.g., transient connection issue) → the error is swallowed.
	 *   This prevents a cleanup failure from breaking the queue worker.
	 *
	 * ### When it is invoked
	 * - After a successful execution in {@link executeProcess}.
	 * - After moving a task to the DLQ.
	 * - After idempotency paths that finish with an irreversible result.
	 *
	 * ### Example
	 *
	 * ```ts
	 * await this.clearAttempts("1700000000000-0");
	 * // The task now has no retry history.
	 * ```
	 *
	 * @param id - Redis stream entry ID whose retry counter should be cleared.
	 */
	private async clearAttempts(id: string): Promise<void> {
		const key = this.attemptsKey(id);

		try {
			await (this.redis as any).del(key);
		}
		catch (e) {
		}
	}

	/**
	 * Marks a task as **successfully executed** and triggers success-related hooks
	 * and optional execution-status bookkeeping.
	 *
	 * This method is called **after `onExecute` completes without throwing**.
	 * It is the final step in the normal (non-error) execution path for a task.
	 *
	 * ---
	 * ## What this method does
	 *
	 * ### 1. Updates execution-status counters (optional)
	 * If {@link executeJobStatus} is enabled, this method increments two counters:
	 *
	 * - `<stream>:<job>:ok`  
	 *   → number of successfully completed tasks for this job
	 *
	 * - `<stream>:<job>:ready`  
	 *   → number of tasks that have finished (both success and failure)
	 *
	 * Both counters expire automatically based on:
	 * - {@link executeJobStatusTtlMs} (default: 5 minutes)
	 *
	 * These counters allow users to:
	 * - monitor job progress in real time,
	 * - implement dashboards,
	 * - detect stuck jobs where `ready` < `total`.
	 *
	 * ```ts
	 * prefix = `${this.stream}:${job}:`;
	 * await this.incr(`${prefix}ok`,    this.executeJobStatusTtlMs);
	 * await this.incr(`${prefix}ready`, this.executeJobStatusTtlMs);
	 * ```
	 *
	 * ### 2. Calls the user hook {@link onSuccess}
	 *
	 * This hook is intended for:
	 * - logging,
	 * - emitting events,
	 * - performing follow-up actions (e.g., notifying external systems),
	 * - metrics.
	 *
	 * ```ts
	 * await this.onSuccess(id, payload, createdAt, job, key);
	 * ```
	 *
	 * The hook receives:
	 * - `id`        – Redis stream entry ID  
	 * - `payload`   – parsed payload  
	 * - `createdAt` – timestamp (ms) extracted from the stream  
	 * - `job`       – batch/job identifier  
	 * - `key`       – idempotency key (empty string if not used)
	 *
	 * ---
	 * ## When this method is triggered
	 *
	 * - After each successful execution inside {@link executeProcess}.
	 * - Also used in the idempotent execution path inside {@link idempotency}.
	 *
	 * ---
	 * ## Error handling
	 *
	 * - Errors inside `success()` **do not block** task acknowledgment.
	 * - Failures inside the user hook `onSuccess` are *not thrown upward* —
	 *   this method assumes success handling should not affect the queue’s flow.
	 *
	 * ---
	 * ## Example
	 *
	 * ```ts
	 * // Custom business logic
	 * async onSuccess(id, payload) {
	 *   console.log("Task completed:", id, payload);
	 * }
	 *
	 * // Internally, after onExecute succeeds:
	 * await this.success(id, payload, createdAt, job, "");
	 * ```
	 *
	 * @param id        - Redis stream entry ID
	 * @param payload   - Task payload object (already decoded if JSON)
	 * @param createdAt - Task creation timestamp (ms)
	 * @param job       - Job/batch ID assigned in {@link addTasks}
	 * @param key       - Idempotency key (empty string if not used)
	 */
	private async success(id: string, payload: any, createdAt: number, job: string, key: string) {
		if (this.executeJobStatus) {
			const prefix = `${this.stream}:${job}:`;

			await this.incr(`${prefix}ok`, this.executeJobStatusTtlMs);
			await this.incr(`${prefix}ready`, this.executeJobStatusTtlMs);
		}
		await this.onSuccess(id, payload, createdAt, job, key);
	}

	/**
	 * Handles **batch-level errors** by forwarding them to the user hook
	 * {@link onBatchError}.
	 *
	 * This method is used when something goes wrong at the **"batch" level**,
	 * not at the level of a single task.  
	 * For example:
	 *
	 * - An unhandled error is thrown inside the main consumer loop:
	 *   - network issues,
	 *   - invalid data structure returned from Redis,
	 *   - unexpected error in `select()` or `execute()`.
	 * - An error happens while processing a group of tasks together
	 *   (e.g. inside `execute()` when using `executeBatchAtOnce`).
	 *
	 * In these situations, we:
	 *
	 * 1. **Do not** try to decide per-task what to do.
	 * 2. Simply call the user-provided hook:
	 *    ```ts
	 *    await this.onBatchError(err, tasks);
	 *    ```
	 *
	 * This gives you a single place to:
	 * - log severe errors,
	 * - push alerts to monitoring systems,
	 * - perform diagnostics on the whole batch,
	 * - decide if some external recovery or throttling is needed.
	 *
	 * ### Parameters
	 *
	 * - `err` – The error object that was thrown.  
	 *   Can be anything (`Error`, string, custom object, etc.).
	 *
	 * - `tasks` – Optional array of tasks that were being processed as a batch
	 *   when the error occurred. The structure of each item is:
	 *
	 *   ```ts
	 *   [ id, payload, createdAt, job, idemKey ]
	 *   ```
	 *
	 *   - `id: string`        – Redis stream entry ID  
	 *   - `payload: any[]`    – normalized key/value list for the task  
	 *   - `createdAt: number` – timestamp (ms)  
	 *   - `job: string`       – job/batch ID  
	 *   - `idemKey: string`   – idempotency key (may be empty string)
	 *
	 *   `tasks` may be `undefined` if the error happened **before** we had a list
	 *   of tasks (for example, in `select()`).
	 *
	 * ### Typical usage (override example)
	 *
	 * ```ts
	 * async onBatchError(err, tasks) {
	 *   this.logger.error('Batch failure', {
	 *     error: String(err?.message || err),
	 *     count: tasks?.length ?? 0,
	 *   });
	 * }
	 * ```
	 *
	 * As a user of `PowerQueues`, you usually override {@link onBatchError}
	 * instead of calling this method directly.
	 *
	 * @param err   - The error that occurred during batch processing.
	 * @param tasks - Optional list of tasks being processed when the error happened.
	 */
	private async batchError(err: any, tasks?: Array<[ string, any[], number, string, string ]>) {
		await this.onBatchError(err, tasks);
	}

	/**
	 * Handles **task-level execution errors** by updating job status (optional),
	 * invoking retry/error hooks, and performing error-side effects that should
	 * happen when a single task fails.
	 *
	 * This method is called when:
	 * - `onExecute()` throws an error, **and**  
	 * - the task is *not* using idempotency (idempotent tasks use a separate error path).
	 *
	 * It is part of the normal retry flow inside {@link executeProcess}.
	 *
	 * ---
	 * ## What this method does
	 *
	 * ### 1. Updates execution-status counters (optional)
	 *
	 * Only when **all retries are exhausted** (i.e., `attempt >= workerMaxRetries`)
	 * and {@link executeJobStatus} is enabled:
	 *
	 * - `<stream>:<job>:err`  
	 *   → total number of tasks that ultimately failed.
	 *
	 * - `<stream>:<job>:ready`  
	 *   → number of tasks that finished (success or final failure).
	 *
	 * These ephemeral counters (TTL = {@link executeJobStatusTtlMs}) help track:
	 * - job progress,
	 * - failure rates,
	 * - stuck jobs.
	 *
	 * ### 2. Forwards the error to the user hook {@link onError}
	 *
	 * ```ts
	 * await this.onError(err, id, payload, createdAt, job, key);
	 * ```
	 *
	 * This allows the user to:
	 * - log errors,
	 * - notify monitoring systems,
	 * - inspect failed payloads,
	 * - perform per-task cleanups.
	 *
	 * ### 3. (Earlier in the flow) the retry counter has already been incremented
	 *
	 * This method **does not increment retries** itself — that is done by:
	 *
	 * - {@link incrAttempts}
	 * - {@link attempt} (user hook for retry)
	 *
	 * By the time `error()` is invoked, we already know the final `attempt` number.
	 *
	 * ---
	 * ## Error behaviour
	 *
	 * - Any exception inside the user hook `onError` is caught upstream by callers.
	 * - This method **never throws** by design — task cleanup must not break the worker.
	 *
	 * ---
	 * ## Relation to other methods
	 *
	 * - Called inside {@link executeProcess} when non-idempotent task execution fails.
	 * - For idempotent tasks, error handling is implemented separately inside {@link idempotency}.
	 * - Complements:
	 *   - {@link attempt} — user hook for retry attempts,
	 *   - {@link success} — user hook for successful executions,
	 *   - {@link batchError} — batch-level error handling.
	 *
	 * ---
	 * ## Parameters
	 *
	 * - `err`        — the thrown error (can be any type)
	 * - `id`         — Redis stream entry ID (e.g., `"1700000000000-0"`)
	 * - `payload`    — parsed task payload
	 * - `createdAt`  — timestamp extracted from the entry
	 * - `job`        — job/batch ID assigned in {@link addTasks}
	 * - `key`        — idempotency key (empty string for non-idempotent tasks)
	 * - `attempt`    — current retry number (1 = first failure)
	 *
	 * ---
	 * ## Example override
	 *
	 * ```ts
	 * async onError(err, id, payload, createdAt) {
	 *   this.logger.warn('Task failed:', {
	 *     id,
	 *     message: String(err?.message || err),
	 *     createdAt,
	 *   });
	 * }
	 * ```
	 *
	 * @param err       - Error thrown during task execution.
	 * @param id        - Redis entry ID of the failed task.
	 * @param payload   - Parsed payload object.
	 * @param createdAt - Task creation timestamp (ms).
	 * @param job       - Job/batch identifier.
	 * @param key       - Idempotency key or empty string.
	 * @param attempt   - Current retry attempt count for this task.
	 */
	private async error(err: any, id: string, payload: any, createdAt: number, job: string, key: string, attempt: number) {
		if (this.executeJobStatus && attempt >= this.workerMaxRetries) {
			const prefix = `${this.stream}:${job}:`;

			await this.incr(`${prefix}err`, this.executeJobStatusTtlMs);
			await this.incr(`${prefix}ready`, this.executeJobStatusTtlMs);
		}
		await this.onError(err, id, payload, createdAt, job, key);
	}

	/**
	 * Notifies user code that a **retry attempt** is happening for a failed task.
	 *
	 * This method is a thin wrapper around the {@link onRetry} hook.
	 * It does not contain business logic itself – it only forwards all
	 * necessary context so you can implement custom behaviour on retries.
	 *
	 * ---
	 * ## When this method is called
	 *
	 * It is invoked inside {@link executeProcess} (and the idempotent path) **after**:
	 *
	 * 1. The task has thrown an error in `onExecute`.
	 * 2. The retry counter has been incremented by {@link incrAttempts}.
	 *
	 * At this point:
	 * - The task is considered **failed but retryable**.
	 * - `attempt` already reflects the new attempt number (1 = first retry).
	 *
	 * ---
	 * ## What you can do in `onRetry`
	 *
	 * You usually **override `onRetry`** in a subclass to:
	 *
	 * - log retry attempts with structured data;
	 * - send metrics (e.g., to Prometheus, Datadog, etc.);
	 * - trigger alerts when attempts exceed some threshold;
	 * - apply custom backoff logic (combined with your own scheduling).
	 *
	 * Example override:
	 *
	 * ```ts
	 * async onRetry(err, id, payload, createdAt, job, key, attempt) {
	 *   this.logger.warn('Retrying task', {
	 *     id,
	 *     job,
	 *     attempt,
	 *     error: String(err?.message || err),
	 *   });
	 * }
	 * ```
	 *
	 * Note: `attempt()` itself **does not** change scheduling or delay – it only
	 * calls `onRetry`. Actual retry timing is handled by the queue worker and
	 * the consumer loop.
	 *
	 * ---
	 * ## Parameters
	 *
	 * - `err`       – The error thrown by your `onExecute` handler.
	 * - `id`        – Redis stream entry ID for this task (e.g. `"1700000000000-0"`).
	 * - `payload`   – Task payload (already decoded if it was JSON).
	 * - `createdAt` – Task creation timestamp in milliseconds.
	 * - `job`       – Job/batch ID that groups related tasks.
	 * - `key`       – Idempotency key for this task (may be an empty string).
	 * - `attempt`   – Current retry attempt number (1 for first retry, 2 for second, etc.).
	 *
	 * As a library user, you rarely call this method directly – instead you implement
	 * `onRetry` and let the queue call it for you.
	 *
	 * @param err       - Error that caused the retry.
	 * @param id        - Redis entry ID of the failed task.
	 * @param payload   - Parsed task payload.
	 * @param createdAt - Task creation timestamp (ms).
	 * @param job       - Job/batch identifier.
	 * @param key       - Idempotency key or empty string.
	 * @param attempt   - Current retry attempt count for this task.
	 */
	private async attempt(err: any, id: string, payload: any, createdAt: number, job: string, key: string, attempt: number) {
		await this.onRetry(err, id, payload, createdAt, job, key, attempt);
	}

	/**
	 * Executes a batch of **already-selected tasks** and returns the list of IDs
	 * that were successfully processed and should be **acknowledged** (XACK).
	 *
	 * This is the main worker-side "runner" that:
	 * - calls {@link executeProcess} for each task;
	 * - optionally runs tasks **in parallel** (see {@link executeBatchAtOnce});
	 * - tracks "contended" tasks (idempotency lock contention);
	 * - triggers the {@link onReady} hook after the batch is processed;
	 * - applies a small backoff when most tasks are contended;
	 * - on unexpected errors, forwards everything to {@link batchError}.
	 *
	 * ---
	 * ## Input format
	 *
	 * Each item in `tasks` is a tuple:
	 *
	 * ```ts
	 * [ id, payload, createdAt, job, idemKey ]
	 * ```
	 *
	 * Where:
	 * - `id: string`        – Redis stream entry ID (e.g. `"1700000000000-0"`);
	 * - `payload: any`      – decoded payload object (from the stream);
	 * - `createdAt: number` – timestamp in ms (added when the task was enqueued);
	 * - `job: string`       – job/batch identifier (same for all tasks of one job);
	 * - `idemKey: string`   – idempotency key (empty string if not used).
	 *
	 * This format comes from {@link normalizeEntries}.
	 *
	 * ---
	 * ## Execution strategy
	 *
	 * For each task, `execute()` calls {@link executeProcess}, which:
	 * - runs user code via {@link onExecute};
	 * - handles retries, DLQ, and idempotency;
	 * - returns an object like:
	 *   - `{ id: string }`       – task processed successfully (should be acked);
	 *   - `{ contended: true }`  – idempotency contention, task will be retried later;
	 *   - `{}` / other           – nothing to ack right now.
	 *
	 * Depending on {@link executeBatchAtOnce}:
	 *
	 * - `executeBatchAtOnce === false` (default)  
	 *   → tasks are processed **sequentially**, one after another.
	 *
	 * - `executeBatchAtOnce === true`  
	 *   → tasks are processed **in parallel**:
	 *   - each call to `executeProcess` is wrapped in a small async function;
	 *   - promises are collected into an array;
	 *   - after the loop, `Promise.all` waits for all of them.
	 *
	 * In both modes:
	 * - Successful tasks add their `id` to `result`.
	 * - Contended tasks increment a local `contended` counter.
	 *
	 * ---
	 * ## After per-task execution
	 *
	 * After all tasks have been passed to `executeProcess`:
	 *
	 * 1. If `executeBatchAtOnce` is `true` and there are promises,
	 *    `Promise.all(promises)` is awaited to ensure all tasks finished.
	 *
	 * 2. {@link onReady} is called:
	 *    ```ts
	 *    await this.onReady(tasks);
	 *    ```
	 *    This hook runs **after** the batch has been processed.  
	 *    Typical use cases:
	 *    - logging,
	 *    - metrics / tracing,
	 *    - emitting "batch processed" events.
	 *
	 * 3. **Backoff on contention**:
	 *    If:
	 *    - no tasks succeeded (`result` is empty),
	 *    - more than half of the tasks were contended,
	 *    then the worker will wait a short, randomized delay:
	 *
	 *    ```ts
	 *    await this.waitAbortable(dynamicDelayMs);
	 *    ```
	 *
	 *    This helps in situations where many workers fight over the same
	 *    idempotency keys: we back off to reduce hot contention.
	 *
	 * ---
	 * ## Error handling
	 *
	 * Any error thrown inside:
	 * - `Promise.all(promises)` (parallel mode),
	 * - `onReady(tasks)`,
	 * - `waitAbortable(...)`,
	 *
	 * is caught and passed to {@link batchError}:
	 *
	 * ```ts
	 * await this.batchError(err, tasks);
	 * ```
	 *
	 * This keeps the worker loop resilient:
	 * - a failure in post-processing does **not** crash the whole consumer;
	 * - you get a single place (`onBatchError`) to inspect and log such failures.
	 *
	 * ---
	 * ## Return value
	 *
	 * The method returns an array of IDs:
	 *
	 * ```ts
	 * const idsToAck = await this.execute(tasks);
	 * ```
	 *
	 * These are the tasks that:
	 * - finished successfully (including DLQ finalization),
	 * - are safe to ACK via {@link approve}.
	 *
	 * Tasks that were contended or still in retry flow will **not** appear here.
	 *
	 * @param tasks - Batch of normalized tasks to execute, in the form
	 *                `[id, payload, createdAt, job, idemKey]`.
	 * @returns A list of stream entry IDs that should be acknowledged.
	 */
	private async execute(tasks: Array<[ string, any[], number, string, string ]>): Promise<string[]> {
		const result: string[] = [];
		let contended = 0,
			promises = [];

		for (const [ id, payload, createdAt, job, idemKey ] of tasks) {
			if (this.executeBatchAtOnce) {
				promises.push((async () => {
					const r = await this.executeProcess(id, payload, createdAt, job, idemKey);

					if (r.id) {
						result.push(id);
					}
					else if (r.contended) {
						contended++;
					}
				})());
			}
			else {
				const r = await this.executeProcess(id, payload, createdAt, job, idemKey);

				if (r.id) {
					result.push(id);
				}
				else if (r.contended) {
					contended++;
				}
			}
		}
		try {
			if (this.executeBatchAtOnce && promises.length > 0) {
				await Promise.all(promises);
			}
			await this.onReady(tasks);

			if (!isArrFilled(result) && contended > (tasks.length >> 1)) {
				await this.waitAbortable((15 + Math.floor(Math.random() * 35)) + Math.min(250, 15 * contended + Math.floor(Math.random() * 40)));
			}
		}
		catch (err) {
			await this.batchError(err, tasks);
		}
		return result;
	}

	/**
	 * Executes a **single task** and decides what should happen with it:
	 * - run normally,
	 * - retry later,
	 * - or send to DLQ (dead-letter queue).
	 *
	 * This is the core per-task runner used by {@link execute}.  
	 * It hides all the details of:
	 * - idempotent vs non-idempotent execution,
	 * - retries and attempt counters,
	 * - DLQ routing,
	 * and returns a small result object that tells the caller if the task
	 * can be safely acknowledged (XACK) or not.
	 *
	 * ---
	 * ## Control flow
	 *
	 * ### 1. Idempotent path (when `key` is non-empty)
	 *
	 * If `key` (idemKey) is provided:
	 *
	 * ```ts
	 * if (key) {
	 *   return await this.idempotency(id, payload, createdAt, job, key);
	 * }
	 * ```
	 *
	 * The whole execution is delegated to {@link idempotency}, which:
	 * - coordinates execution between multiple workers using locks,
	 * - ensures the same logical task is processed only once,
	 * - may return:
	 *   - `{ id }`          → task finished (success or DLQ), safe to ACK;
	 *   - `{ contended: true }` → another worker is currently handling it,
	 *                             do **not** ACK, retry later;
	 *   - `{}`              → nothing to do / give up for now.
	 *
	 * The exact semantics are documented in {@link idempotency}.
	 *
	 * ### 2. Non-idempotent path (when `key` is empty)
	 *
	 * If there is **no idempotency key**, the method runs a simpler flow:
	 *
	 * ```ts
	 * try {
	 *   await this.onExecute(id, payload, createdAt, job, key, await this.getAttempts(id));
	 *   await this.success(id, payload, createdAt, job, key);
	 *   return { id };
	 * }
	 * catch (err) {
	 *   // handle failure, retries, DLQ
	 * }
	 * ```
	 *
	 * Steps:
	 *
	 * 1. Reads the current attempt count via {@link getAttempts}.
	 * 2. Calls the user hook {@link onExecute} with all context:
	 *    - this is where your business logic lives.
	 * 3. If `onExecute` finishes without throwing:
	 *    - calls {@link success} to update job status & fire `onSuccess`,
	 *    - returns `{ id }` → caller will ACK this task.
	 *
	 * If `onExecute` **throws**, we enter the `catch` block:
	 *
	 * 1. Increment the retry counter via {@link incrAttempts} → `attempt`.
	 * 2. Notify user code about the retry via {@link attempt} (calls `onRetry`).
	 * 3. Call {@link error} (calls `onError` and optionally updates job status).
	 *
	 * 4. If `attempt >= workerMaxRetries`:
	 *    - the task is considered **permanently failed**,
	 *    - it is moved to a **DLQ stream**:
	 *
	 *      ```ts
	 *      await this.addTasks(`${this.stream}:dlq`, [{
	 *        payload: {
	 *          ...payload,
	 *          error: String(err?.message || err),
	 *          createdAt,
	 *          job,
	 *          id,
	 *          attempt,
	 *        },
	 *      }]);
	 *      ```
	 *
	 *    - the attempt counter is cleared via {@link clearAttempts},
	 *    - the method returns `{ id }` → task can be ACKed (we "finished" it by
	 *      sending to DLQ).
	 *
	 * 5. If `attempt < workerMaxRetries`:
	 *    - the method falls through to the final `return {}`:
	 *      - no `{ id }` is returned,
	 *      - caller will **not** ACK this task,
	 *      - Redis will keep it as pending so it can be retried later.
	 *
	 * ---
	 * ## Return value
	 *
	 * The returned object is intentionally very small:
	 *
	 * - `{ id: string }`  
	 *   → task is **done** (success or DLQ) and should be acknowledged.
	 *
	 * - `{ contended: true }`  
	 *   → (idempotent path only) another worker is handling this logical task;
	 *     do **not** ACK, worker should back off / retry later.
	 *
	 * - `{}`  
	 *   → nothing to ACK right now (e.g. failed but still under retry limit).
	 *
	 * {@link execute} uses this shape to build the `ids` array passed to
	 * {@link approve} (which actually performs `XACK` / `XDEL`).
	 *
	 * ---
	 * ## Parameters
	 *
	 * - `id`        – Redis stream entry ID (e.g. `"1700000000000-0"`).
	 * - `payload`   – decoded payload of the task (object or primitive).
	 * - `createdAt` – timestamp in ms when the task was enqueued.
	 * - `job`       – job/batch ID shared across related tasks.
	 * - `key`       – idempotency key; empty string means "no idempotency".
	 *
	 * As a user, you typically do **not** call `executeProcess` directly –
	 * you override hooks like `onExecute`, `onRetry`, `onError`, `onSuccess`
	 * and let the worker handle the flow.
	 *
	 * @param id        - Stream entry ID for the task.
	 * @param payload   - Task payload, already normalized/decoded.
	 * @param createdAt - Task creation time in milliseconds.
	 * @param job       - Job/batch identifier of this task.
	 * @param key       - Idempotency key (non-empty for idempotent tasks).
	 * @returns An object describing the outcome: `{ id }`, `{ contended: true }`, or `{}`.
	 */
	private async executeProcess(id: string, payload: any, createdAt: number, job: string, key: string): Promise<any> {
		if (key) {
			return await this.idempotency(id, payload, createdAt, job, key);
		}
		else {
			try {
				await this.onExecute(id, payload, createdAt, job, key, await this.getAttempts(id));
				await this.success(id, payload, createdAt, job, key);

				return { id };
			}
			catch (err: any) {
				const attempt = await this.incrAttempts(id);

				await this.attempt(err, id, payload, createdAt, job, key, attempt);
				await this.error(err, id, payload, createdAt, job, key, attempt);

				if (attempt >= this.workerMaxRetries) {
					await this.addTasks(`${this.stream}:dlq`, [{
						payload: {
							...payload,
							error: String(err?.message || err),
							createdAt,
							job,
							id,
							attempt,
						},
					}]);
					await this.clearAttempts(id);
					
					return { id };
				}
			}
		}
		return {};
	}

	/**
	 * Approve a list of **successfully processed stream entries** and
	 * optionally **deletes** them from the Redis stream.
	 *
	 * This method is the final step of the worker pipeline:
	 * it takes IDs returned by {@link execute}, calls the `Approve` Lua script,
	 * and performs batched `XACK` (and optionally `XDEL`) operations.
	 *
	 * ---
	 * ## What this method does
	 *
	 * 1. Validates input:
	 *    - If `ids` is not an array or is empty → returns `0` immediately.
	 *
	 * 2. Computes a safe batch size:
	 *
	 *    ```ts
	 *    const approveBatchTasksCount =
	 *      Math.max(500, Math.min(4000, this.approveBatchTasksCount));
	 *    ```
	 *
	 *    - Ensures the effective batch size is always **between 500 and 4000**.
	 *    - This keeps a good balance between:
	 *      - fewer round-trips to Redis, and
	 *      - not sending excessively large argument lists.
	 *
	 * 3. Splits `ids` into slices (`part`) of at most `approveBatchTasksCount`
	 *    items and, for each slice, calls the `Approve` Lua script via {@link runScript}:
	 *
	 *    ```ts
	 *    const approved = await this.runScript(
	 *      'Approve',
	 *      [ this.stream ],
	 *      [ this.group, this.removeOnExecuted ? '1' : '0', ...part ],
	 *      Approve,
	 *    );
	 *    ```
	 *
	 *    Inside the script (`Approve`):
	 *    - `XACK` is executed for all IDs,
	 *    - if `removeOnExecuted` is `true`, it also attempts to `XDEL` them
	 *      (delete entries from the stream).
	 *
	 * 4. Sums up how many messages were acknowledged:
	 *
	 *    ```ts
	 *    total += Number(approved || 0);
	 *    ```
	 *
	 *    `approved` is the numeric result returned by `XACK` (how many entries
	 *    were actually acknowledged in Redis).
	 *
	 * 5. Returns the **total number of entries acknowledged** across all batches.
	 *
	 * ---
	 * ## `removeOnExecuted` behaviour
	 *
	 * The boolean flag {@link removeOnExecuted} controls whether entries are
	 * deleted from the stream after being acknowledged:
	 *
	 * - `removeOnExecuted === false` (default):
	 *   - Only `XACK` is performed.
	 *   - The entries remain in the stream history but are no longer pending
	 *     for this consumer group.
	 *
	 * - `removeOnExecuted === true`:
	 *   - `XACK` is called,
	 *   - then the script tries `XDEL` (best-effort, falls back to per-ID loop
	 *     if bulk deletion command fails).
	 *
	 * ---
	 * ## Error handling
	 *
	 * - Any internal Redis/script errors are thrown by {@link runScript} and
	 *   handled by the caller (usually the worker loop).
	 * - The Lua script itself is defensive: if bulk `XDEL` fails, it falls back
	 *   to deleting IDs one by one.
	 *
	 * ---
	 * ## When it is used
	 *
	 * Called from {@link consumerLoop} after a batch of tasks has been processed:
	 *
	 * ```ts
	 * const ids = await this.execute(...);
	 * if (isArrFilled(ids)) {
	 *   await this.approve(ids);
	 * }
	 * ```
	 *
	 * As a library user, you usually don’t call `approve()` directly – the
	 * queue worker handles it for you.
	 *
	 * @param ids - Array of Redis stream entry IDs that were successfully
	 *              processed and should be acknowledged (and maybe deleted).
	 * @returns The total number of entries acknowledged by `XACK`.
	 */
	private async approve(ids: string[]) {
		if (!Array.isArray(ids) || !(ids.length > 0)) {
			return 0;
		}
		const approveBatchTasksCount = Math.max(500, Math.min(4000, this.approveBatchTasksCount));
		let total = 0, i = 0;

		while (i < ids.length) {
			const room = Math.min(approveBatchTasksCount, ids.length - i);
			const part = ids.slice(i, i + room);
			const approved = await this.runScript('Approve', [ this.stream ], [ this.group, this.removeOnExecuted ? '1' : '0', ...part ], Approve);

			total += Number(approved || 0);
			i += room;
		}
		return total;
	}

	/**
	 * Ensures **idempotent execution** of a task for a given idempotency key.
	 *
	 * This method is called internally from {@link executeProcess} whenever a task
	 * has a non-empty `key` (idempotency key). It coordinates multiple workers so
	 * that **only one worker actually executes** the task logic for a given key,
	 * while all other workers either:
	 * - **return immediately** if the task was already completed, or
	 * - **back off and retry later** if another worker is currently processing it.
	 *
	 * ### High-level flow
	 * 1. Build Redis idempotency keys via {@link idempotencyKeys}:
	 *    - `doneKey`  – marks that the task for this key has been fully processed.
	 *    - `lockKey`  – a short-lived lock that identifies the "owner" worker.
	 *    - `startKey` – indicates that processing has started and holds a TTL.
	 * 2. Call {@link idempotencyAllow} (Lua `IdempotencyAllow` script):
	 *    - Returns `1` → `doneKey` exists -> task already processed → `{ id }`.
	 *    - Returns `0` → another worker is processing this key:
	 *      - Read TTL of `startKey` (`PTTL`) and wait via {@link waitAbortable}.
	 *      - Return `{ contended: true }` so the caller can treat it as a soft conflict.
	 *    - Returns `2` → current worker is allowed to become the owner.
	 * 3. If allowed, try to become the owner via {@link idempotencyStart}
	 *    (Lua `IdempotencyStart` script). If this fails, another worker won the race:
	 *    - Return `{ contended: true }`.
	 * 4. When the worker becomes the owner:
	 *    - Start a **heartbeat** via {@link heartbeat} which periodically extends
	 *      `lockKey` and `startKey` TTL using {@link sendHeartbeat}. This protects
	 *      long-running jobs from being stolen by other workers while still alive.
	 *    - Execute the user handler {@link onExecute} with the task data.
	 *    - On success:
	 *      - Mark completion through {@link idempotencyDone} (`doneKey` + TTL).
	 *      - Call {@link success} to update job status counters and hooks.
	 *      - Return `{ id }`.
	 *    - On failure:
	 *      - Increment attempts with {@link incrAttempts}.
	 *      - Call {@link attempt} (retry hook) and {@link error} (error hook).
	 *      - If `attempt >= workerMaxRetries`:
	 *        - Push the task into a **dead-letter queue** `${stream}:dlq` with error info.
	 *        - Clear attempts via {@link clearAttempts}.
	 *        - Release the idempotency lock via {@link idempotencyFree}.
	 *        - Return `{ id }` (task is considered finished, but failed).
	 *      - If `attempt < workerMaxRetries`:
	 *        - Only release the lock via {@link idempotencyFree}.
	 *        - The task will be retried later by the queue logic.
	 * 5. Finally, stop the heartbeat so that no more TTL refreshes are sent.
	 *
	 * ### When to use
	 * - This method is not meant to be called directly in user code.
	 * - Instead, pass an `idemKey` in your task payload and let the queue internals
	 *   route execution here. For the same `idemKey`, only one successful execution
	 *   is allowed; all other workers will either return immediately or back off.
	 *
	 * @param id        Redis stream entry ID of the task.
	 * @param payload   Parsed task payload (usually the value previously encoded in `payload`).
	 * @param createdAt Unix timestamp (ms) when the task was created.
	 * @param job       Batch/job identifier used for grouping metrics and status keys.
	 * @param key       **Idempotency key**. All tasks sharing this key are treated as the
	 *                  same logical unit of work (e.g. "sendEmail:user:123").
	 *
	 * @returns A promise resolving to:
	 * - `{ id }` when the task is considered **done** for this worker (either because
	 *   it executed successfully, was already done, or permanently failed and moved to DLQ).
	 * - `{ contended: true }` when another worker is currently processing the same key
	 *   and this worker should treat the task as a soft conflict / backoff.
	 * - `{}` in rare internal fall-through cases (should normally not happen).
	 */
	private async idempotency(id: string, payload: any, createdAt: number, job: string, key: string) {
		const keys = this.idempotencyKeys(key);
		const allow = await this.idempotencyAllow(keys);

		if (allow === 1) {
			return { id };
		}
		else if (allow === 0) {
			let ttl = -2;
						
			try {
				ttl = await (this.redis as any).pttl(keys.startKey);
			}
			catch (err) {
			}
			await this.waitAbortable(ttl);
			return { contended: true };
		}
		if (!(await this.idempotencyStart(keys))) {
			return { contended: true };
		}
		const heartbeat = this.heartbeat(keys) || (() => {});

		try {
			await this.onExecute(id, payload, createdAt, job, key, await this.getAttempts(id));
			await this.idempotencyDone(keys);
			await this.success(id, payload, createdAt, job, key);
			return { id };
		}
		catch (err: any) {
			const attempt = await this.incrAttempts(id);

			try {
				await this.attempt(err, id, payload, createdAt, job, key, attempt);
				await this.error(err, id, payload, createdAt, job, key, attempt);

				if (attempt >= this.workerMaxRetries) {
					await this.addTasks(`${this.stream}:dlq`, [{
						payload: {
							...payload,
							error: String(err?.message || err),
							createdAt,
							job,
							id,
							attempt: 0,
						},
					}]);
					await this.clearAttempts(id);
					await this.idempotencyFree(keys);
						
					return { id };
				}
				await this.idempotencyFree(keys);
			}
			catch (err2: any) {
			}
		}
		finally {
			heartbeat();
		}
	}

	/**
	 * Builds a **stable set of Redis keys** used to coordinate idempotent execution
	 * for a single logical operation (idempotency key).
	 *
	 * This helper does not touch Redis itself. It only **generates key names** and
	 * a unique token that are later used by the idempotency Lua scripts:
	 * - {@link IdempotencyAllow} (via {@link idempotencyAllow})
	 * - {@link IdempotencyStart} (via {@link idempotencyStart})
	 * - {@link IdempotencyDone} (via {@link idempotencyDone})
	 * - {@link IdempotencyFree} (via {@link idempotencyFree})
	 *
	 * ### What it generates
	 * For a given idempotency key and the current queue (`this.stream`), it returns:
	 *
	 * - `prefix`   – common prefix for all idempotency keys of this stream:
	 *   `q:{safeStream}:`
	 * - `doneKey`  – marks that the operation for this key has been **fully processed**:
	 *   `q:{safeStream}:done:{safeKey}`
	 * - `lockKey`  – short-lived **lock owner marker** used to decide which worker
	 *   is allowed to execute the task:
	 *   `q:{safeStream}:lock:{safeKey}`
	 * - `startKey` – indicates that processing has **started**, also used to store
	 *   TTL for waiting workers:
	 *   `q:{safeStream}:start:{safeKey}`
	 * - `token`    – unique random token identifying the current worker instance.
	 *   It is stored in `lockKey` and passed to scripts so that only the owner
	 *   can refresh or release the lock.
	 *
	 * Both `this.stream` and the provided `key` are **sanitized**:
	 * any character that is not a word, colon, or dash (`[^\w:\-]`) is replaced
	 * with an underscore. This makes keys safe and predictable even if the original
	 * names contain spaces or special characters.
	 *
	 * ### Example key layout
	 * For:
	 * - `this.stream = 'orders:new'`
	 * - `key = 'user:42#email'`
	 *
	 * The resulting keys will look like:
	 * - `prefix`   → `q:orders:new:`
	 * - `doneKey`  → `q:orders:new:done:user:42_email`
	 * - `lockKey`  → `q:orders:new:lock:user:42_email`
	 * - `startKey` → `q:orders:new:start:user:42_email`
	 * - `token`    → something like `host:pid:kq3m9f...` (unique per call)
	 *
	 * @param key
	 * Idempotency key that represents the logical operation
	 * (for example: `"sendEmail:user:123"`). Tasks that share the same idempotency key
	 * are treated as the **same work unit**, even if they arrive multiple times.
	 *
	 * @returns An {@link IdempotencyKeys} object containing:
	 * `prefix`, `doneKey`, `lockKey`, `startKey`, and a unique `token`
	 * used by the idempotency workflow.
	 */
	private idempotencyKeys(key: string): IdempotencyKeys {
		const prefix = `q:${this.stream.replace(/[^\w:\-]/g, '_')}:`;
		const keyP = key.replace(/[^\w:\-]/g, '_');
		const doneKey  = `${prefix}done:${keyP}`;
		const lockKey  = `${prefix}lock:${keyP}`;
		const startKey = `${prefix}start:${keyP}`;
		const token = `${this.consumer()}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2)}`;

		return {
			prefix,
			doneKey,
			lockKey,
			startKey,
			token,
		};
	}

	/**
	 * Runs the **first step** of the idempotency workflow and decides what this
	 * worker is allowed to do for a given idempotency key.
	 *
	 * Internally this calls the Lua script {@link IdempotencyAllow} via
	 * {@link runScript}. The script checks three main things:
	 *
	 * 1. **Is the task already done?**
	 *    - If `doneKey` exists, the script returns `1`.
	 *      → This means some worker has **already finished** the work for this key.
	 *      → The caller should treat the task as completed and skip execution.
	 *
	 * 2. **Can this worker acquire the lock?**
	 *    - If `doneKey` does not exist, the script tries to set `lockKey` with a TTL
	 *      using `SET lockKey token NX PX ttlMs`.
	 *    - If the lock is successfully set, the script may also set `startKey` with
	 *      the same TTL (to mark the beginning of execution) and returns `2`.
	 *      → This means **this worker becomes the owner** and is allowed to execute.
	 *
	 * 3. **Is another worker already processing this key?**
	 *    - If `lockKey` already exists (owned by another worker), the script
	 *      returns `0`.
	 *      → This means someone else is either executing or has recently started
	 *        handling the same work unit, so this worker should **back off**.
	 *
	 * ### Return codes
	 * The result is normalized to a small numeric union:
	 *
	 * - `1` — Task is **already completed** (the `doneKey` exists).
	 *   - The caller (see {@link idempotency}) should return `{ id }` immediately
	 *     without executing the user handler again.
	 *
	 * - `2` — This worker **successfully acquired** the idempotency lock.
	 *   - The caller should continue with {@link idempotencyStart}, start heartbeat,
	 *     and execute the actual business logic.
	 *
	 * - `0` — Another worker is currently processing (or recently processed) this key.
	 *   - The caller should typically:
	 *     - Read `PTTL` of `startKey`,
	 *     - Wait a small amount via {@link waitAbortable},
	 *     - Return `{ contended: true }` so the queue can treat this as a soft conflict.
	 *
	 * ### Example usage (simplified)
	 *
	 * ```ts
	 * const keys = this.idempotencyKeys(idemKey);
	 * const allow = await this.idempotencyAllow(keys);
	 *
	 * if (allow === 1) {
	 *   // Work already done → nothing to do
	 *   return { id };
	 * }
	 *
	 * if (allow === 0) {
	 *   // Another worker is processing → back off
	 *   await this.waitAbortable(ttl);
	 *   return { contended: true };
	 * }
	 *
	 * // allow === 2 → current worker can proceed as owner
	 * ```
	 *
	 * @param keys
	 * A precomputed {@link IdempotencyKeys} object created by {@link idempotencyKeys}.
	 * It contains the `doneKey`, `lockKey`, `startKey`, and `token` that the Lua
	 * script needs to make a decision.
	 *
	 * @returns A promise resolving to:
	 * - `1` if the job for this key is **already fully processed**.
	 * - `2` if this worker **acquired the lock** and is allowed to process.
	 * - `0` if **another worker owns the lock** and this worker should back off.
	 */
	private async idempotencyAllow(keys: IdempotencyKeys): Promise<0 | 1 | 2> {
		const res = await this.runScript('IdempotencyAllow', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.workerExecuteLockTimeoutMs), keys.token ], IdempotencyAllow);

		return Number(res || 0) as 0 | 1 | 2;
	}

	/**
	 * Performs the **second step** of the idempotency handshake and confirms
	 * that this worker still owns the lock before running the actual job logic.
	 *
	 * Internally this calls the Lua script {@link IdempotencyStart} via
	 * {@link runScript}. It is used right after {@link idempotencyAllow} returns `2`
	 * (meaning "you are allowed to become the owner").
	 *
	 * ### What the script does
	 * The `IdempotencyStart` Lua script receives:
	 * - `lockKey` and `startKey` as `KEYS`
	 * - `token` and `ttlMs` as `ARGV`
	 *
	 * And then:
	 * 1. Reads `lockKey` value and checks if it is **equal to** `token`.
	 *    - If this check fails, it returns `0`.
	 *      → Some other worker may have taken over or the lock expired.
	 * 2. If the token matches, it:
	 *    - Sets `startKey` to `1` with a TTL (`PX ttlMs`) if `ttlMs > 0`,
	 *      otherwise sets it without expiry.
	 *    - Refreshes the TTL of `lockKey` (using `PEXPIRE`) if `ttlMs > 0`.
	 *    - Returns `1` to indicate that this worker is now the **active owner**.
	 *
	 * ### Why this step exists
	 * This function is a safety check between:
	 *
	 * - Step 1: {@link idempotencyAllow} → tries to create `lockKey` and decides if
	 *   the worker may proceed (`2`), skip (`1`), or back off (`0`).
	 * - Step 2: {@link idempotencyStart} → verifies that the lock is **still valid**
	 *   and owned by this worker's `token` before doing any real work.
	 *
	 * It protects against edge cases where:
	 * - The lock was granted but expired very quickly.
	 * - Another worker managed to overwrite the lock in between operations.
	 *
	 * Only if this function returns `true` will the caller:
	 * - Start the heartbeat via {@link heartbeat},
	 * - Execute the user handler {@link onExecute},
	 * - And later mark completion via {@link idempotencyDone}.
	 *
	 * ### Typical usage (inside {@link idempotency})
	 *
	 * ```ts
	 * const keys = this.idempotencyKeys(idemKey);
	 * const allow = await this.idempotencyAllow(keys);
	 *
	 * if (allow === 2) {
	 *   const started = await this.idempotencyStart(keys);
	 *   if (!started) {
	 *     // Lost the race – another worker owns the lock now
	 *     return { contended: true };
	 *   }
	 *
	 *   // Now safe to run the handler and heartbeat
	 * }
	 * ```
	 *
	 * @param keys
	 * A precomputed {@link IdempotencyKeys} object (from {@link idempotencyKeys})
	 * containing `lockKey`, `startKey`, and `token`. The token must match the value
	 * stored in `lockKey` for this method to succeed.
	 *
	 * @returns
	 * - `true` if this worker successfully confirmed ownership and refreshed TTLs.
	 * - `false` if the lock is no longer owned by this worker (or does not match its token),
	 *   meaning another worker should handle this idempotency key.
	 */
	private async idempotencyStart(keys: IdempotencyKeys): Promise<boolean> {
		const res = await this.runScript('IdempotencyStart', [ keys.lockKey, keys.startKey ], [ keys.token, String(this.workerExecuteLockTimeoutMs) ], IdempotencyStart);

		return Number(res || 0) === 1;
	}

	/**
	 * Marks an idempotent operation as **successfully completed** and clears
	 * any related lock keys in Redis.
	 *
	 * This is called **after** the user handler (`onExecute`) has finished
	 * without throwing, as part of the idempotency workflow in {@link idempotency}.
	 *
	 * Internally it executes the Lua script {@link IdempotencyDone} via
	 * {@link runScript}. The script performs three main actions:
	 *
	 * 1. **Mark as done**
	 *    - Sets `doneKey` to `1` (no special meaning for the value, the presence
	 *      of the key is what matters).
	 *    - Applies a TTL (in **seconds**) based on `workerCacheTaskTimeoutMs`:
	 *      - If `workerCacheTaskTimeoutMs > 0`, it calls `EXPIRE doneKey ttlSec`.
	 *      - This creates a short-lived cache entry used by {@link idempotencyAllow}
	 *        to immediately short-circuit repeated tasks with the same idempotency key.
	 *
	 * 2. **Clear lock (if still owned by this worker)**
	 *    - Reads `lockKey` and compares it with the `token` of the current worker.
	 *    - If the value matches, it deletes `lockKey`.
	 *
	 * 3. **Clear start marker**
	 *    - If the lock is still owned by the current worker and `startKey` is defined,
	 *      it deletes `startKey` as well.
	 *
	 * Together, these steps:
	 * - Tell other workers that the operation is **finished** (`doneKey` exists),
	 * - Release the lock so no one else treats this worker as the active owner,
	 * - Clean up temporary keys used for coordination and waiting.
	 *
	 * > Note:
	 * > The TTL value passed to the script is `workerCacheTaskTimeoutMs`, but the Lua
	 * > script interprets it as **seconds** (via `EXPIRE`). Make sure this property
	 * > is configured accordingly at the class level.
	 *
	 * ### Typical call site (inside {@link idempotency})
	 *
	 * ```ts
	 * const keys = this.idempotencyKeys(idemKey);
	 *
	 * // ... run onExecute without error ...
	 *
	 * await this.idempotencyDone(keys);
	 * await this.success(id, payload, createdAt, job, key);
	 * ```
	 *
	 * @param keys
	 * A set of Redis key names and token generated by {@link idempotencyKeys}.
	 * Includes `doneKey`, `lockKey`, and `startKey` which the Lua script uses
	 * to mark completion and release the idempotency lock.
	 */
	private async idempotencyDone(keys: IdempotencyKeys): Promise<void> {
		await this.runScript('IdempotencyDone', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.workerCacheTaskTimeoutMs), keys.token ], IdempotencyDone);
	}

	/**
	 * Releases the **idempotency lock** for a given key without marking
	 * the operation as completed.
	 *
	 * This is used when the current worker **should stop owning** the lock,
	 * but the task is **not considered done** yet. Typical cases:
	 *
	 * - The handler (`onExecute`) failed but there are retries left.
	 * - An error happened while handling an idempotent task and we want to give
	 *   other workers a chance to pick it up later.
	 *
	 * Internally it runs the Lua script {@link IdempotencyFree} via {@link runScript}.
	 *
	 * ### What the Lua script does
	 * The script receives:
	 * - `lockKey`, `startKey` as `KEYS`
	 * - `token` as `ARGV[1]`
	 *
	 * Steps:
	 * 1. Reads `lockKey` and checks if its value equals the provided `token`.
	 *    - If it does **not** match, it returns `0` and does nothing
	 *      (another worker owns the lock or it expired).
	 * 2. If the token matches:
	 *    - Deletes `lockKey`.
	 *    - Deletes `startKey` (if it is provided / non-empty).
	 *    - Returns `1` to indicate that the lock was successfully released.
	 *
	 * This ensures that **only the worker that owns the lock** (same `token`)
	 * can release it, preventing other workers from accidentally removing locks
	 * they do not control.
	 *
	 * ### Difference from {@link idempotencyDone}
	 * - `idempotencyDone`:
	 *   - Sets `doneKey` to mark the operation as **fully completed**.
	 *   - Clears the lock and start keys.
	 * - `idempotencyFree`:
	 *   - Only removes the lock/start keys if owned by this worker.
	 *   - Does **not** set `doneKey`, so the operation can still be retried later.
	 *
	 * ### Typical usage (inside {@link idempotency})
	 *
	 * ```ts
	 * // inside the catch branch for idempotent tasks
	 * const attempt = await this.incrAttempts(id);
	 *
	 * if (attempt >= this.workerMaxRetries) {
	 *   // move to DLQ, clear attempts and free lock permanently
	 *   await this.idempotencyFree(keys);
	 *   return { id };
	 * }
	 *
	 * // still have retries → free lock so another worker can try later
	 * await this.idempotencyFree(keys);
	 * return { contended: true };
	 * ```
	 *
	 * @param keys
	 * A set of Redis key names and token created by {@link idempotencyKeys}.
	 * Only if `keys.token` matches the current `lockKey` value will the lock
	 * and its `startKey` marker be removed.
	 */
	private async idempotencyFree(keys: IdempotencyKeys): Promise<void> {
		await this.runScript('IdempotencyFree', [ keys.lockKey, keys.startKey ], [ keys.token ], IdempotencyFree);
	}

	/**
	 * Ensures that a Redis **consumer group** exists for the current stream.
	 *
	 * This helper wraps the `XGROUP CREATE` command and is used by the worker
	 * loop before reading tasks with `XREADGROUP` or claiming pending entries.
	 *
	 * If the group already exists, Redis returns a `BUSYGROUP` error.  
	 * In that case the error is **silently ignored**, because it simply means
	 * that another worker has already created the group. Any other error is
	 * rethrown so that configuration / connection issues are not hidden.
	 *
	 * Internally it executes:
	 *
	 * ```redis
	 * XGROUP CREATE {stream} {group} {from} MKSTREAM
	 * ```
	 *
	 * - `MKSTREAM` tells Redis to create the stream automatically if it does not
	 *   exist yet. This makes startup more robust when the stream is empty.
	 * - `{from}` defines **where the group starts reading**:
	 *   - `'0-0'` → the group will see **all past entries** from the beginning.
	 *   - `'$'`   → the group will only see **new entries** added after creation.
	 *
	 * ### Where it is used
	 * - Called by {@link runQueue} / {@link consumerLoop} to make sure the group is ready.
	 * - Also used by {@link selectStuck} and {@link selectFresh} when they detect
	 *   a `NOGROUP` error from Redis.
	 *
	 * This method is **idempotent**: calling it many times is safe, because
	 * the `BUSYGROUP` error is treated as a "group already exists" signal.
	 *
	 * @param from
	 * Starting ID for the consumer group:
	 * - `'0-0'` – consume the entire history of the stream.
	 * - `'$'`   – start from the latest message only (default).
	 *
	 * @returns A promise that resolves when the group is successfully created
	 *          or already exists. Rejects only on unexpected Redis errors.
	 */
	private async createGroup(from: '$' | '0-0' = '$') {
		try {
			await (this.redis as any).xgroup('CREATE', this.stream, this.group, from, 'MKSTREAM');
		}
		catch (err: any) {
			const msg = String(err?.message || '');
			
			if (!msg.includes('BUSYGROUP')) {
				throw err;
			}
		}
	}

	/**
	 * Fetches the **next batch of tasks** for the worker, combining:
	 * 1. **Stuck (pending) tasks** that should be recovered, and
	 * 2. **Fresh (new) tasks** that have just arrived in the stream.
	 *
	 * This method is the main **entry point for task selection** in the worker
	 * loop and is used directly by {@link consumerLoop}. It hides the complexity
	 * of dealing with pending vs new messages and always returns a normalized,
	 * easy-to-use structure.
	 *
	 * ### How it works
	 * 1. It first calls {@link selectStuck}:
	 *    - Tries to claim long-idle pending messages (using `XAUTOCLAIM` via Lua).
	 *    - These are tasks that were delivered to some consumer but not acknowledged
	 *      for too long (e.g. worker crashed, timed out, or got stuck).
	 *    - Recovering them first helps prevent tasks from being lost or blocked forever.
	 *
	 * 2. If no stuck tasks are found (`entries` is empty):
	 *    - It calls {@link selectFresh} to read **new messages** using `XREADGROUP`.
	 *    - This returns tasks that have never been delivered to any consumer before.
	 *
	 * 3. Regardless of where the raw entries came from, it passes them to
	 *    {@link normalizeEntries}, which:
	 *    - Converts raw Redis reply structures into a **typed, consistent format**.
	 *    - Extracts and decodes the payload.
	 *    - Pulls out metadata fields like `job`, `createdAt`, and `idemKey`.
	 *
	 * ### Returned structure
	 * The final result is an array of tuples:
	 *
	 * ```ts
	 * [
	 *   id: string,
	 *   payload: any[],
	 *   createdAt: number,
	 *   job: string,
	 *   idemKey: string
	 * ][]
	 * ```
	 *
	 * - `id`        – Redis stream entry ID (e.g. `"1699780000000-0"`).
	 * - `payload`   – Parsed/decoded payload passed later into {@link onExecute}.
	 * - `createdAt` – Unix timestamp (in ms) when the task was originally created.
	 * - `job`       – Job/batch identifier used for grouping and metrics.
	 * - `idemKey`   – Idempotency key (empty string if not set); used by
	 *                 {@link idempotency} to ensure only one execution per key.
	 *
	 * ### Usage
	 * You normally do **not** call this method directly. It is used internally by
	 * {@link consumerLoop}:
	 *
	 * ```ts
	 * const tasks = await this.select();
	 * if (!isArrFilled(tasks)) {
	 *   await wait(600);
	 *   continue;
	 * }
	 * const ids = await this.execute(tasks);
	 * ```
	 *
	 * @returns
	 * A promise that resolves to a normalized list of tasks ready to be passed into
	 * {@link execute}. If no tasks are available (neither stuck nor fresh), an
	 * empty array is returned.
	 */
	private async select(): Promise<Array<[ string, any[], number, string, string ]>> {
		let entries: Array<[ string, any[], number, string, string ]> = await this.selectStuck();

		if (!isArrFilled(entries)) {
			entries = await this.selectFresh();
		}
		return this.normalizeEntries(entries);
	}

	/**
	 * Attempts to recover **stuck (pending) tasks** from the Redis stream.
	 *
	 * A "stuck" task is a message that:
	 * - was previously delivered to some consumer in the group,
	 * - but was **not acknowledged** (XACK) within a reasonable time,
	 * - typically because a worker crashed, froze, or exceeded its processing time.
	 *
	 * This method uses the Lua script {@link SelectStuck} (via {@link runScript}),
	 * which combines:
	 *
	 * - `XAUTOCLAIM` to fetch pending messages that have been idle for too long  
	 *   (defined by `recoveryStuckTasksTimeoutMs`),
	 * - a **time budget** to avoid blocking the worker for too long,
	 * - a fallback `XREADGROUP` step inside the script to fill any remaining capacity.
	 *
	 * It returns raw Redis entries, which will later be normalized by
	 * {@link normalizeEntries}.
	 *
	 * ---
	 * ### What the Lua script does (high-level)
	 *
	 * 1. Uses `XAUTOCLAIM` in a loop:
	 *    - Claims pending messages whose idle time ≥ `pendingIdleMs`
	 *      (mapped from `this.recoveryStuckTasksTimeoutMs`).
	 *    - Accumulates up to `count` entries (mapped from `workerBatchTasksCount`).
	 *    - Stops when:
	 *      - enough entries are collected, or
	 *      - the internal **time budget** (`workerSelectionTimeoutMs`) is exceeded.
	 *
	 * 2. If the script still has room to fetch more tasks:
	 *    - It calls `XREADGROUP` with `>` to read **new** messages up to the remaining
	 *      slot count.
	 *    - This ensures the worker always gets some work even if there are not
	 *      enough stuck tasks.
	 *
	 * 3. Returns a plain Lua array of stream entries in the format Redis uses for
	 *    `XREADGROUP` / `XAUTOCLAIM`.
	 *
	 * ---
	 * ### Why recovering stuck tasks first?
	 *
	 * Processing stuck tasks before fresh tasks ensures:
	 * - **At-least-once delivery** is maintained,
	 * - No task stays in "pending" state forever,
	 * - The queue self-heals from worker crashes and unpredictable failures,
	 * - Dead letter queue (DLQ) fallback paths can be consistently enforced.
	 *
	 * ---
	 * ### Error handling
	 *
	 * If Redis responds with `NOGROUP` (meaning the consumer group does not exist),
	 * the method:
	 *
	 * 1. Calls {@link createGroup} to create the group safely,
	 * 2. Returns an empty list so the worker can retry on the next iteration.
	 *
	 * Any other error is swallowed only inside the Lua script execution; unexpected
	 * errors from Redis outside of `NOGROUP` should be considered real failures.
	 *
	 * ---
	 * ### Return format
	 *
	 * Returns an array of raw Redis entries:
	 *
	 * ```ts
	 * [ [id: string, rawKeyValues: (string | Buffer)[]], ... ]
	 * ```
	 *
	 * These entries will later be decoded into:
	 *
	 * ```ts
	 * [ id, payload, createdAt, job, idemKey ]
	 * ```
	 *
	 * by {@link normalizeEntries}.
	 *
	 * ---
	 * ### Typical usage (indirect)
	 *
	 * ```ts
	 * const stuck = await this.selectStuck();
	 * if (isArrFilled(stuck)) {
	 *   return this.normalizeEntries(stuck);
	 * }
	 * // otherwise selectFresh() will be used
	 * ```
	 *
	 * @returns
	 * Raw Redis entries representing reclaimed pending tasks.  
	 * If no pending entries can be reclaimed, an empty array is returned.
	 */
	private async selectStuck(): Promise<any[]> {
		try {
			const res = await this.runScript('SelectStuck', [ this.stream ], [ this.group, this.consumer(), String(this.recoveryStuckTasksTimeoutMs), String(this.workerBatchTasksCount), String(this.workerSelectionTimeoutMs) ], SelectStuck);

			return (isArr(res) ? res : []) as any[];
		}
		catch (err: any) {
			if (String(err?.message || '').includes('NOGROUP')) {
				await this.createGroup();
			}
		}
		return [];
	}

	/**
	 * Reads **fresh (new) tasks** from the Redis stream using `XREADGROUP`.
	 *
	 * This method is called after {@link selectStuck} when no stuck/pending
	 * messages are available. It focuses only on messages that have **never**
	 * been delivered to any consumer in the group before.
	 *
	 * Internally it issues a blocking `XREADGROUP` command:
	 *
	 * ```redis
	 * XREADGROUP GROUP {group} {consumer}
	 *   BLOCK {blockMs}
	 *   COUNT {batchSize}
	 *   STREAMS {stream} >
	 * ```
	 *
	 * - `GROUP {group} {consumer}` – reads messages on behalf of the current
	 *   consumer in the configured group.
	 * - `BLOCK {blockMs}` – waits up to `blockMs` milliseconds for new messages.
	 *   - `blockMs` is derived from `workerLoopIntervalMs` (with a minimum of 2ms).
	 * - `COUNT {batchSize}` – at most `workerBatchTasksCount` messages will be
	 *   returned in a single call.
	 * - `STREAMS {stream} >` – the special ID `>` means:
	 *   "Give me only **new** messages that have not been delivered before".
	 *
	 * ### Returned data
	 *
	 * The raw Redis response structure is simplified to:
	 *
	 * ```ts
	 * const res = await xreadgroup(...);
	 * const entries = res?.[0]?.[1] ?? [];
	 * ```
	 *
	 * So this method returns:
	 *
	 * ```ts
	 * [
	 *   [ id: string | Buffer, [ field1, value1, field2, value2, ... ] ],
	 *   ...
	 * ]
	 * ```
	 *
	 * These entries are later transformed into normalized task tuples by
	 * {@link normalizeEntries} inside {@link select}.
	 *
	 * If Redis returns no messages within the blocking timeout, or the array
	 * is empty, this method resolves to an empty array.
	 *
	 * ### Error handling
	 *
	 * - If Redis returns a `NOGROUP` error (consumer group does not exist yet):
	 *   - It calls {@link createGroup} to create the group safely.
	 *   - Returns an empty array so the worker can retry on the next loop.
	 * - Other errors are not specially handled here and will bubble up in
	 *   the calling code if needed.
	 *
	 * ### Typical usage (indirect)
	 *
	 * You usually do not call this directly. Instead, it is used from {@link select}:
	 *
	 * ```ts
	 * let entries = await this.selectStuck();
	 *
	 * if (!isArrFilled(entries)) {
	 *   entries = await this.selectFresh();
	 * }
	 *
	 * return this.normalizeEntries(entries);
	 * ```
	 *
	 * @returns
	 * A promise that resolves to an array of raw Redis entries representing
	 * **newly delivered** tasks for this consumer group.  
	 * If there are no new messages or the group had to be created, an empty
	 * array is returned.
	 */
	private async selectFresh(): Promise<any[]> {
		let entries: Array<[ string, any[], number, string, string ]> = [];

		try {
			const res = await (this.redis as any).xreadgroup(
				'GROUP', this.group, this.consumer(),
				'BLOCK', Math.max(2, this.workerLoopIntervalMs | 0),
				'COUNT', this.workerBatchTasksCount,
				'STREAMS', this.stream, '>',
			);

			entries = res?.[0]?.[1] ?? [];

			if (!isArrFilled(entries)) {
				return [];
			}
		}
		catch (err: any) {
			if (String(err?.message || '').includes('NOGROUP')) {
				await this.createGroup();
			}
		}
		return entries;
	}

	/**
	 * Sleeps for a short, **abortable** delay with a bit of random jitter.
	 *
	 * This is a small helper used mainly when the worker needs to **back off**
	 * for a while, but should also be able to stop quickly when the queue
	 * is shutting down (via {@link abort} / {@link signal}).
	 *
	 * The returned promise resolves in two cases:
	 *
	 * 1. **Timeout finished** – normal delay elapsed.
	 * 2. **Abort signal fired** – if `this.abort.abort()` was called, the wait
	 *    is cancelled early and the promise resolves immediately.
	 *
	 * ### How the delay is calculated
	 *
	 * The `ttl` argument usually comes from Redis `PTTL` (remaining time-to-live
	 * of some key) and is used to choose a reasonable wait duration:
	 *
	 * - If `ttl > 0`:
	 *   - `base` = `clamp(ttl, 25ms, 5000ms)` – so we never sleep less than 25ms
	 *     and never longer than 5s in one call.
	 *   - `jitter` = random number between `0` and `min(base, 200ms)`.
	 *   - `delay` = `base + jitter`.
	 *   - This avoids a "thundering herd" effect where many workers wake up at exactly
	 *     the same time, spreading their retries slightly.
	 *
	 * - If `ttl <= 0`:
	 *   - `delay` is a small random value between **5ms and 20ms**.
	 *   - This is a generic short backoff when we do not have a meaningful TTL.
	 *
	 * The timer handle is `unref()`-ed when available (Node.js), which means:
	 * - The presence of this timeout **will not keep the process alive** if
	 *   there is nothing else preventing it from exiting.
	 *
	 * ### Abort behaviour
	 *
     * - The method subscribes to the current {@link AbortController} signal via
	 *   {@link signal}.
	 * - If the signal is already aborted, it resolves **immediately**.
	 * - If the signal is aborted during the delay:
	 *   - It clears the timeout,
	 *   - Removes the event listener,
	 *   - Resolves the promise.
	 *
	 * This allows the worker loop to stop quickly even if it is currently
	 * in a waiting period.
	 *
	 * ### Typical usage
	 *
	 * - After detecting idempotency contention (`allow === 0` in {@link idempotency}),
	 *   to wait until another worker finishes:
	 *
	 * ```ts
	 * // ttl comes from PTTL(startKey) or a similar source
	 * await this.waitAbortable(ttl);
	 * return { contended: true };
	 * ```
	 *
	 * - After seeing many contended tasks in {@link execute}, to briefly pause
	 *   before polling again:
	 *
	 * ```ts
	 * if (!isArrFilled(result) && contended > (tasks.length >> 1)) {
	 *   await this.waitAbortable(dynamicDelay);
	 * }
	 * ```
	 *
	 * @param ttl
	 * Suggested time-to-wait in milliseconds:
	 * - If `ttl > 0`, it is used to derive the base delay (clamped between 25ms and 5000ms)
	 *   plus some jitter.
	 * - If `ttl <= 0`, a small default delay (5–20ms) is used instead.
	 *
	 * @returns A promise that resolves when the delay has elapsed or when the
	 * current abort signal is triggered.
	 */
	private async waitAbortable(ttl: number) {
		return new Promise<void>((resolve) => {
			const signal = this.signal();

			if (signal?.aborted) {
				return resolve();
			}
			let delay: number;

			if (ttl > 0) {
				const base = Math.max(25, Math.min(ttl, 5000));
				const jitter = Math.floor(Math.min(base, 200) * Math.random());
				
				delay = base + jitter;
			}
			else {
				delay = 5 + Math.floor(Math.random() * 15);
			}
			const t = setTimeout(() => {
				if (signal) {
					signal.removeEventListener('abort', onAbort as any);
				}
				resolve();
			}, delay);
			(t as any).unref?.();

			function onAbort() { 
				clearTimeout(t); 
				resolve(); 
			}
			signal?.addEventListener?.('abort', onAbort, { once: true });
		});
	}

	/**
	 * Sends a single **heartbeat** to Redis for an idempotent task.
	 *
	 * This method **extends the TTL** (time-to-live) of the idempotency keys
	 * so that long-running jobs are not considered "stuck" and do not lose
	 * their ownership while still actively processing.
	 *
	 * Internally it calls `PEXPIRE` on:
	 *
	 * - `keys.lockKey`  – lock owner marker for the current worker.
	 * - `keys.startKey` – marker that indicates processing has started.
	 *
	 * Both keys are extended by `workerExecuteLockTimeoutMs` milliseconds.
	 *
	 * ### Why this matters
	 *
	 * Without heartbeats, a long-running job could:
	 *
	 * - Exceed the original lock TTL,
	 * - Have its lock key expire,
	 * - Allow another worker to acquire the same idempotency key,
	 * - Result in **duplicate execution** of the same logical operation.
	 *
	 * By periodically calling {@link sendHeartbeat} (via {@link heartbeat}):
	 *
	 * - The lock and start markers stay alive as long as the worker is alive,
	 * - Other workers know that this task is still being processed,
	 * - The queue maintains **stronger idempotency guarantees** even for
	 *   slow operations.
	 *
	 * ### Return value
	 *
	 * - Returns `true` if **at least one** of the keys (`lockKey` or `startKey`)
	 *   had its TTL successfully updated (`PEXPIRE` returned `1`).
	 * - Returns `false` if:
	 *   - Both keys failed to update (no key existed or Redis returned `0`), or
	 *   - An error occurred while talking to Redis.
	 *
	 * The {@link heartbeat} loop uses this value to detect when heartbeats are
	 * failing repeatedly and eventually stops sending them (considering the lock
	 * effectively lost).
	 *
	 * ### Typical usage
	 *
	 * You do not call this directly in user code. It is used internally by
	 * {@link heartbeat}, which manages:
	 *
	 * - The repeat interval,
	 * - Failure counting,
	 * - Stopping conditions when too many heartbeats fail.
	 *
	 * @param keys
	 * A set of idempotency keys produced by {@link idempotencyKeys}.  
	 * Only `lockKey` and `startKey` are used here; both are refreshed with
	 * `workerExecuteLockTimeoutMs` as their new TTL.
	 *
	 * @returns
	 * `true` if at least one key was successfully refreshed, otherwise `false`.
	 */
	private async sendHeartbeat(keys: IdempotencyKeys): Promise<boolean> {
		try {
			const r1 = await (this.redis as any).pexpire(keys.lockKey, this.workerExecuteLockTimeoutMs);
			const r2 = await (this.redis as any).pexpire(keys.startKey, this.workerExecuteLockTimeoutMs);
			const ok1 = Number(r1 || 0) === 1;
			const ok2 = Number(r2 || 0) === 1;

			return ok1 || ok2;
		}
		catch {
			return false;
		}
	}

	/**
	 * Starts a **periodic heartbeat loop** for an idempotent task and returns a
	 * function to stop it.
	 *
	 * This method is used inside {@link idempotency} after the worker has
	 * successfully acquired the idempotency lock (via {@link idempotencyStart}).
	 * It repeatedly calls {@link sendHeartbeat} to refresh the TTL of the lock
	 * and start keys while the task is running.
	 *
	 * > If `workerExecuteLockTimeoutMs <= 0`, heartbeats are disabled and this
	 * > method returns `undefined`.
	 *
	 * ### How it works
	 *
	 * 1. It calculates a heartbeat interval:
	 *    - `workerHeartbeatTimeoutMs = max(1000, floor(max(5000, workerExecuteLockTimeoutMs) / 4))`
	 *    - So:
	 *      - The interval is **at least 1 second**.
	 *      - For larger `workerExecuteLockTimeoutMs`, heartbeats are sent roughly
	 *        every 1/4 of that value, but never less frequent than every 5s/4.
	 *
	 * 2. It subscribes to the current abort signal (from {@link signal}):
	 *    - If the signal is aborted, it stops the heartbeat loop.
	 *
	 * 3. It schedules a `tick` function with `setTimeout`:
	 *    - On each tick it calls {@link sendHeartbeat(keys)}.
	 *    - If the heartbeat succeeds:
	 *      - `hbFails` is reset to `0`.
	 *    - If the heartbeat fails or throws:
	 *      - `hbFails` is incremented.
	 *
	 * 4. Failure thresholds:
	 *    - If **3 consecutive logical failures** occur (`hbFails >= 3` after a
	 *      successful call), an error `"Heartbeat lost."` is thrown in order to
	 *      trigger the catch block and increment `hbFails` again.
	 *    - In the `catch` block:
	 *      - `hbFails` is incremented again.
	 *      - If `hbFails >= 6`, the loop is **stopped permanently**:
	 *        - No more heartbeats are sent.
	 *        - The lock may eventually expire and be taken by another worker.
	 *
	 * 5. After each tick (if still alive), the next tick is scheduled with the
	 *    same `workerHeartbeatTimeoutMs`. The timeout handle is `unref()`-ed so
	 *    it does not keep the Node.js process alive by itself.
	 *
	 * ### Returned function
	 *
	 * If heartbeats are enabled, this method returns a **stop function**:
	 *
	 * ```ts
	 * const stopHeartbeat = this.heartbeat(keys);
	 *
	 * // later, when the job is done or the worker is shutting down:
	 * stopHeartbeat?.();
	 * ```
	 *
	 * Calling this function:
	 * - Removes the abort listener from the signal,
	 * - Clears any pending heartbeat timeout,
	 * - Prevents further heartbeat attempts.
	 *
	 * The caller (e.g. {@link idempotency}) usually gets this function and calls
	 * it in a `finally` block to guarantee cleanup:
	 *
	 * ```ts
	 * const heartbeat = this.heartbeat(keys);
	 * try {
	 *   // run handler
	 * } finally {
	 *   heartbeat?.();
	 * }
	 * ```
	 *
	 * @param keys
	 * A set of idempotency keys produced by {@link idempotencyKeys}. These keys
	 * are passed to {@link sendHeartbeat} to refresh their TTL regularly.
	 *
	 * @returns
	 * - A **stop function** that cancels the heartbeat loop and removes internal
	 *   listeners, or
	 * - `undefined` if heartbeats are disabled (`workerExecuteLockTimeoutMs <= 0`).
	 */
	private heartbeat(keys: IdempotencyKeys) {
		if (this.workerExecuteLockTimeoutMs <= 0) {
			return;
		}
		const workerHeartbeatTimeoutMs = Math.max(1000, Math.floor(Math.max(5000, this.workerExecuteLockTimeoutMs | 0) / 4));
		let timer: any;
		let alive = true;
		let hbFails = 0;

		const stop = () => {
			alive = false;

			if (timer) {
				clearTimeout(timer);
			}
		};
		const signal = this.signal();
		const onAbort = () => stop();

		signal?.addEventListener?.('abort', onAbort, { once: true });

		const tick = async () => {
			if (!alive) {
				return;
			}
			try {
				const ok = await this.sendHeartbeat(keys);

				hbFails = ok ? 0 : hbFails + 1;

				if (hbFails >= 3) {
					throw new Error('Heartbeat lost.');
				}
			}
			catch {
				hbFails++;

				if (hbFails >= 6) {
					stop();
					
					return;
				}
			}
			timer = setTimeout(tick, workerHeartbeatTimeoutMs);
			(timer as any).unref?.();
		};

		timer = setTimeout(tick, workerHeartbeatTimeoutMs);
		(timer as any).unref?.();

		return () => {
			signal?.removeEventListener?.('abort', onAbort as any);
			stop();
		};
	}

	/**
	 * Converts raw Redis stream entries into a **normalized, strongly-typed** format
	 * used by the worker pipeline.
	 *
	 * Redis returns stream entries from `XREADGROUP`, `XAUTOCLAIM`, or Lua scripts
	 * in a very loose structure:
	 *
	 * ```ts
	 * [
	 *   [ id: string | Buffer, [ field1, value1, field2, value2, ... ] ],
	 *   [ ... ],
	 * ]
	 * ```
	 *
	 * This method transforms each entry into a clean tuple:
	 *
	 * ```ts
	 * [
	 *   id: string,
	 *   payload: any[],
	 *   createdAt: number,
	 *   job: string,
	 *   idemKey: string
	 * ]
	 * ```
	 *
	 * The resulting structure is much easier to work with inside the worker,
	 * especially for:
	 *
	 * - {@link execute}
	 * - {@link idempotency}
	 * - {@link onExecute}
	 * - {@link onSelected}
	 * - {@link onReady}
	 *
	 * ---
	 * ### Steps performed
	 *
	 * 1. **Validate input**
	 *    - If `raw` is not an array, return `[]`.
	 *
	 * 2. **Convert Buffers → strings**
	 *    - Redis clients may return IDs and field names/values as Node.js Buffers.
	 *    - This converts them to strings to avoid later type issues.
	 *
	 * 3. **Filter invalid entries**
	 *    - Entries without an ID or with a malformed key/value array are skipped.
	 *    - A valid entry must have an even number of KV items.
	 *
	 * 4. **Extract metadata**
	 *    - Reads all KV pairs into a JS object using {@link values}.
	 *    - Extracts:
	 *      - `job`  
	 *      - `createdAt`  
	 *      - `payload`  
	 *      - `idemKey`
	 *
	 *    - `payload` is further decoded using {@link payload}, which:
	 *      - Tries to JSON-decode it,
	 *      - Returns raw data if decoding fails.
	 *
	 * 5. **Produce normalized tuple**
	 *    - Ensures a consistent structure for each entry.
	 *
	 * ---
	 * ### Example transformation
	 *
	 * Raw Redis entry:
	 *
	 * ```ts
	 * [
	 *   "1700000000-0",
	 *   ["payload", "{\"x\":1}", "createdAt", "1700000000000", "job", "abc", "idemKey", "u1"]
	 * ]
	 * ```
	 *
	 * Normalized:
	 *
	 * ```ts
	 * [
	 *   "1700000000-0",
	 *   { x: 1 },
	 *   1700000000000,
	 *   "abc",
	 *   "u1"
	 * ]
	 * ```
	 *
	 * ---
	 * ### Why this method exists
	 *
	 * Redis stream APIs return very low-level arrays. Normalizing them:
	 *
	 * - Removes Buffer/string inconsistencies,
	 * - Extracts metadata reliably,
	 * - Ensures workers always receive well-formed task entries,
	 * - Makes downstream code significantly simpler and safer.
	 *
	 * ---
	 * @param raw
	 * Raw Redis entries returned by `XREADGROUP`, Lua `XAUTOCLAIM`, or the
	 * `SelectStuck` script. Can be `null`, malformed, or empty.
	 *
	 * @returns
	 * Array of normalized task tuples:
	 *
	 * ```ts
	 * [ id, payload, createdAt, job, idemKey ][]
	 * ```
	 *
	 * or an empty array if no usable entries are found.
	 */
	private normalizeEntries(raw: any): Array<[ string, any[], number, string, string ]> {
		if (!Array.isArray(raw)) {
			return [];
		}
		return Array
			.from(raw || [])
			.map((e) => {
				const id = Buffer.isBuffer(e?.[0]) ? e[0].toString() : e?.[0];
				const kvRaw = e?.[1] ?? [];
				const kv = isArr(kvRaw) ? kvRaw.map((x: any) => (Buffer.isBuffer(x) ? x.toString() : x)) : [];
	
				return [ id as string, kv ] as [ string, any[] ];
			})
			.filter(([ id, kv ]) => isStrFilled(id) && isArr(kv) && (kv.length & 1) === 0)
			.map(([ id, kv ]) => {
				const { idemKey = '', job, createdAt, payload } = this.values(kv);

				return [ id, this.payload(payload), createdAt, job, idemKey ];
			});
	}

	/**
	 * Converts a flat Redis key/value array into a plain JavaScript object.
	 *
	 * Redis stream entries store their fields as a flat array:
	 *
	 * ```ts
	 * ["field1", "value1", "field2", "value2", ...]
	 * ```
	 *
	 * This helper turns that into:
	 *
	 * ```ts
	 * {
	 *   field1: "value1",
	 *   field2: "value2",
	 *   ...
	 * }
	 * ```
	 *
	 * It is intentionally minimal — no type conversion or parsing is performed
	 * here. All values remain raw strings unless later processed by higher-level
	 * methods such as:
	 *
	 * - {@link payload} – which tries to JSON-decode the `"payload"` field,
	 * - {@link normalizeEntries} – which extracts `createdAt`, `job`, `idemKey`, etc.
	 *
	 * ---
	 * ### How it works
	 *
	 * - Iterates through the array in steps of **2**.
	 * - Treats each pair `[key, value]` as a property on the resulting object.
	 * - If the array length is odd (should not happen in valid Redis entries),
	 *   the last dangling element is ignored.
	 * - Keys and values are used **as-is** (already converted to strings earlier).
	 *
	 * ---
	 * ### Example
	 *
	 * Input:
	 *
	 * ```ts
	 * ["payload", "{\"x\":123}", "createdAt", "1700000000000", "job", "abc"]
	 * ```
	 *
	 * Output:
	 *
	 * ```ts
	 * {
	 *   payload: "{\"x\":123}",
	 *   createdAt: "1700000000000",
	 *   job: "abc"
	 * }
	 * ```
	 *
	 * The returned object is then passed to {@link normalizeEntries} which:
	 * - Parses `payload`,
	 * - Converts `createdAt` to a number,
	 * - Extracts `job` and `idemKey`,
	 * - Builds a normalized worker task tuple.
	 *
	 * ---
	 * @param value
	 * Flat array of alternating key/value items from Redis.
	 * This is typically the `entry[1]` part of an `XREADGROUP` response.
	 *
	 * @returns A simple object containing the extracted field/value mappings.
	 */
	private values(value: any[]) {
		const result: any = {};

		for (let i = 0; i < value.length; i += 2) {
			result[value[i]] = value[i + 1];
		}
		return result;
	}

	/**
	 * Attempts to decode the `"payload"` field of a Redis stream entry.
	 *
	 * The `payload` stored in Redis is usually a JSON string created by
	 * {@link addTasks}, but depending on user input or malformed data,
	 * it may also be plain text. This helper ensures that downstream code
	 * always receives a **safe and predictable JavaScript value**.
	 *
	 * ### Behaviour
	 *
	 * 1. Tries to `JSON.parse` the provided value using {@link jsonDecode}.
	 *    - If decoding succeeds, the parsed object is returned.
	 *    - If decoding fails (invalid JSON), the raw input value is returned.
	 *
	 * 2. Any unexpected error encountered while decoding is swallowed:
	 *    - The method falls back to returning the raw data unchanged.
	 *
	 * ### Why this method exists
	 *
	 * Redis stream values are always stored as plain strings.  
	 * Without decoding:
	 * - Workers would have to manually `JSON.parse` payloads everywhere,
	 * - Errors could leak or crash the worker loop,
	 * - The queue would be more fragile to malformed data.
	 *
	 * By using this method, the worker pipeline receives:
	 * - Valid JS objects when the payload is proper JSON,
	 * - Safe fallback values when payloads are not JSON,
	 * - A consistent interface for all messages.
	 *
	 * ### Example
	 *
	 * Input (string from Redis):
	 *
	 * ```ts
	 * "{\"x\": 42 }"
	 * ```
	 *
	 * Output:
	 *
	 * ```ts
	 * { x: 42 }
	 * ```
	 *
	 * Invalid input:
	 *
	 * ```ts
	 * "{not json}"
	 * ```
	 *
	 * Output:
	 *
	 * ```ts
	 * "{not json}"
	 * ```
	 *
	 * ### Usage
	 *
	 * Normally used only inside {@link normalizeEntries}:
	 *
	 * ```ts
	 * const { payload } = this.values(kv);
	 * return [ id, this.payload(payload), createdAt, job, idemKey ];
	 * ```
	 *
	 * @param data
	 * The raw `"payload"` value taken from Redis (typically a string).
	 *
	 * @returns
	 * A parsed JS object if `data` contains valid JSON, otherwise the raw value.
	 */
	private payload(data: any): any {
		try {
			return jsonDecode(data);
		}
		catch (err) {
		}
		return data;
	}

	/**
	 * Returns the **AbortSignal** associated with this queue instance.
	 *
	 * The signal comes from the internal {@link AbortController} stored in
	 * `this.abort`, and is used throughout the worker lifecycle to support
	 * **graceful shutdown**, **interruptible waits**, and **cleanup** logic.
	 *
	 * ### Why this exists
	 *
	 * Many internal operations — such as {@link waitAbortable}, {@link heartbeat},
	 * and the main {@link consumerLoop} — need a way to stop immediately when
	 * the queue is shutting down. Using a shared abort signal allows:
	 *
	 * - Breaking out of sleeps or backoff delays,
	 * - Cancelling heartbeat timers,
	 * - Stopping worker loops,
	 * - Cleaning up pending timers or event listeners.
	 *
	 * This avoids scenarios where the worker keeps retrying or waiting even after
	 * shutdown has been requested.
	 *
	 * ### How it is used
	 *
	 * - **waitAbortable** subscribes to `signal.abort` to wake up early:
	 *
	 * ```ts
	 * const signal = this.signal();
	 * if (signal.aborted) return;
	 * signal.addEventListener('abort', () => resolve());
	 * ```
	 *
	 * - **heartbeat** stops sending TTL-refreshing heartbeats when the signal fires.
	 *
	 * - **consumerLoop** checks `signal.aborted` to know when to exit its main loop.
	 *
	 * - External code can trigger shutdown via:
	 *
	 * ```ts
	 * queues.abort.abort();
	 * ```
	 *
	 * which immediately propagates to all internal listeners.
	 *
	 * ### Example
	 *
	 * ```ts
	 * const q = new PowerQueues();
	 * const signal = q.signal();
	 *
	 * signal.addEventListener('abort', () => {
	 *   console.log('Queue shutdown started');
	 * });
	 *
	 * // Later…
	 * q.abort.abort(); // triggers all waiting operations to stop
	 * ```
	 *
	 * ### Returns
	 *
	 * The active `AbortSignal` associated with this queue’s lifecycle.
	 */
	private signal() {
		return this.abort.signal;
	}

	/**
	 * Generates a unique **consumer identifier** for this worker instance.
	 *
	 * Redis consumer groups require each worker to supply a `consumer` name
	 * when reading from a stream via `XREADGROUP`. This method returns a
	 * predictable, unique identifier for the current process.
	 *
	 * ### Format
	 *
	 * The returned string has the form:
	 *
	 * ```
	 * {host}:{pid}
	 * ```
	 *
	 * - `{host}` – taken from `this.consumerHost`  
	 *   (defaults to `"host"` unless overridden in your subclass or instance)
	 * - `{pid}` – the current Node.js process ID (`process.pid`)
	 *
	 * Example:
	 *
	 * ```
	 * "worker-1:48210"
	 * "prod-api:10933"
	 * ```
	 *
	 * ### Why this matters
	 *
	 * - Each consumer in a Redis consumer group must have a **unique name**.
	 * - Names are used for:
	 *   - Pending-entry tracking,
	 *   - Idle time measurement,
	 *   - Worker crash detection,
	 *   - Task claiming (`XAUTOCLAIM`),
	 *   - Monitoring via `XPENDING`.
	 * - Using `{host}:{pid}` makes it very unlikely for two workers on the same
	 *   machine to collide, and completely avoids collisions across machines.
	 *
	 * ### How it is used
	 *
	 * - When reading messages:
	 *
	 * ```ts
	 * xreadgroup('GROUP', this.group, this.consumer(), ...)
	 * ```
	 *
	 * - When recovering stuck messages:
	 *
	 * ```ts
	 * this.runScript('SelectStuck', [this.stream], [this.group, this.consumer(), ...])
	 * ```
	 *
	 * - When generating idempotency tokens (part of the idempotency lock logic):
	 *
	 * ```ts
	 * const token = `${this.consumer()}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2)}`
	 * ```
	 *
	 * ### Customization
	 *
	 * You may override `this.consumerHost` in a subclass or via constructor
	 * configuration to give meaningful names such as:
	 *
	 * ```
	 * "api-queue-worker"
	 * "batch-service"
	 * "worker-eu-central-1"
	 * ```
	 *
	 * @returns A string uniquely identifying this worker instance in Redis.
	 */
	private consumer(): string {
		return `${String(this.consumerHost || 'host')}:${process.pid}`;
	}
}