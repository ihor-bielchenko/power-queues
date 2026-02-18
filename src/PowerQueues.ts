import { setMaxListeners } from 'node:events';
import type { IORedisLike } from 'power-redis';
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
	isExists,
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
	/** @hidden */
	public abort = new AbortController();
	
	/** @hidden */
	public redis!: IORedisLike;
	
	/** @hidden */
	public readonly scripts: Record<string, SavedScript> = {};
	
	/**
	 * Logical host identifier used to build a unique Redis Streams **consumer name**.
	 *
	 * This value is combined with the current process id to form {@link consumer}:
	 * `"<host>:<pid>"`.
	 *
	 * Why it matters:
	 * - In Redis Streams consumer groups, each worker instance must have a distinct consumer name.
	 * - Using `host + pid` makes collisions very unlikely even if you run multiple workers
	 *   on the same machine or inside multiple containers.
	 *
	 * Example:
	 * - `host = "monitoring-worker-1"`
	 * - `process.pid = 12345`
	 * - `consumer()` becomes `"monitoring-worker-1:12345"`
	 *
	 * Practical recommendations:
	 * - Set this to something stable and human-readable in production:
	 *   hostname, container name, service instance id, etc.
	 * - If you run multiple pods/containers, include the pod/container identifier in `host`.
	 *
	 * Default in this class is `"host"` (placeholder).
	 */
	public readonly host: string = 'host';

	/**
	 * Name of the Redis Streams **consumer group** used by this queue worker.
	 *
	 * In Redis Streams, a consumer group allows multiple consumers (workers)
	 * to coordinate message processing without duplicating work.
	 *
	 * All workers that:
	 * - use the same `queueName`
	 * - and the same `group`
	 *
	 * will share the same consumer group state (Pending Entries List, last delivered ID, etc.).
	 *
	 * ---
	 *
	 * ## What is a consumer group?
	 * A consumer group:
	 * - Tracks which messages were delivered
	 * - Tracks which messages are still pending (not acknowledged)
	 * - Distributes new messages across consumers in the group
	 *
	 * Example Redis command used internally:
	 * `XGROUP CREATE <queueName> <group> <from>`
	 *
	 * ---
	 *
	 * ## How this affects scaling
	 *
	 * ### Horizontal scaling (multiple workers)
	 * If you want multiple worker instances to cooperate on the same queue:
	 *
	 * - Use the **same `group` name**
	 * - Ensure each worker has a unique consumer name (handled by `host + pid`)
	 *
	 * This way:
	 * - Redis distributes new messages across workers
	 * - Stuck/pending messages can be reclaimed by other workers
	 *
	 * ### Independent processing
	 * If you want different services to process the same stream independently:
	 *
	 * - Use **different `group` names**
	 *
	 * Each group will:
	 * - Maintain its own delivery position
	 * - Have its own Pending Entries List
	 * - Process the same messages separately
	 *
	 * ---
	 *
	 * ## Example scenarios
	 *
	 * Single logical worker group:
	 * ```ts
	 * group = "alerts-workers"
	 * ```
	 *
	 * Two independent consumers of the same stream:
	 * ```ts
	 * // Service A
	 * group = "alerts-email"
	 *
	 * // Service B
	 * group = "alerts-telegram"
	 * ```
	 *
	 * ---
	 *
	 * ## Best practices
	 * - Choose a stable and descriptive name (e.g. `"monitoring-workers-v1"`).
	 * - Do not randomly change this value in production — doing so creates a new group
	 *   and may cause old messages to be reprocessed depending on `from`.
	 * - If you deploy breaking changes in message format, consider versioning the group name.
	 *
	 * Default in this class: `"gr1"` (simple placeholder).
	 */
	public readonly group: string = 'gr1';

	/**
	 * Maximum number of "stuck" (pending) messages to fetch in a single stuck-selection pass.
	 *
	 * This value is used by {@link selectS}, which calls the Lua script {@link SelectStuck}.
	 * That script is responsible for finding messages that were delivered to some consumer
	 * but were not acknowledged within a reasonable time (for example, because the worker
	 * crashed or lost connectivity).
	 *
	 * In other words, this controls the **batch size** for recovering pending work.
	 *
	 * ---
	 *
	 * ## How it is used
	 * Internally we call:
	 * `runScript('SelectStuck', [queueName], [ group, consumer, selectStuckTimeout, selectStuckCount, selectStuckMaxTimeout ])`
	 *
	 * So `selectStuckCount` is passed to Redis as the "COUNT" / limit parameter for stuck selection.
	 *
	 * ---
	 *
	 * ## Why it matters
	 * - Higher values recover stuck work faster after outages,
	 *   but increase the amount of work processed in one loop iteration
	 *   (more memory + longer batch execution time).
	 * - Lower values reduce per-iteration load, but recovery may take longer
	 *   if there are many pending messages.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - For lightweight tasks (fast processing), values like `200`–`2000` can be reasonable.
	 * - For heavy tasks (slow processing), keep it smaller (e.g. `50`–`200`)
	 *   to avoid long batches and large retry storms.
	 * - Make sure it is balanced with:
	 *   - {@link selectCount} (new-message batch size),
	 *   - {@link approveCount} (ACK batch size),
	 *   - and your worker throughput.
	 *
	 * Default in this class: `200`.
	 */
	public readonly selectStuckCount: number = 200;

	/**
	 * Time threshold (in milliseconds) that defines when a pending message is considered "stuck".
	 *
	 * This value is used by {@link selectS} (stuck selection) via the Lua script {@link SelectStuck}.
	 * Conceptually, a message becomes "stuck" when it was delivered to some consumer,
	 * but was not acknowledged for a long enough time (for example, the worker crashed
	 * after receiving it).
	 *
	 * `selectStuckTimeout` tells the stuck-selection script:
	 * **"Only reclaim messages that have been pending for at least this long."**
	 *
	 * ---
	 *
	 * ## How it is used
	 * {@link selectS} calls:
	 * `runScript('SelectStuck', [queueName], [ group, consumer, selectStuckTimeout, selectStuckCount, selectStuckMaxTimeout ])`
	 *
	 * The script typically compares the idle time of pending entries (PEL) against this threshold.
	 *
	 * ---
	 *
	 * ## Why it matters
	 * - Too small → you may reclaim tasks that are still being processed by a slow worker,
	 *   increasing duplicate work and contention.
	 * - Too large → recovery after a crash is slower (tasks sit pending longer before being reclaimed).
	 *
	 * This setting should reflect the typical maximum execution time of your tasks plus some buffer.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - If most tasks finish within ~2–5 seconds, a value like `30_000`–`60_000` ms is safe.
	 * - If tasks can run for minutes, increase this accordingly (or rely more on heartbeat/idempotency).
	 * - Consider how it interacts with {@link idemLockTimeout} and heartbeat:
	 *   even if a task is reclaimed, idempotency may prevent double execution.
	 *
	 * Default in this class: `60_000` ms (60 seconds).
	 */
	public readonly selectStuckTimeout: number = 60000;

	/**
	 * Upper bound (in seconds) for how long a pending message is allowed to stay "stuck"
	 * before it becomes eligible for more aggressive recovery by the stuck-selection logic.
	 *
	 * This value is passed into {@link SelectStuck} via {@link selectS}.
	 * While {@link selectStuckTimeout} defines the **minimum** idle time (in ms) for a message
	 * to be considered stuck, `selectStuckMaxTimeout` acts as an additional **cap** / safety limit
	 * (in seconds) that the Lua script can use to decide how far back it should look or how
	 * aggressively it should reclaim very old pending entries.
	 *
	 * In simpler words:
	 * - `selectStuckTimeout` = "don’t touch pending entries until they are idle for at least X ms"
	 * - `selectStuckMaxTimeout` = "treat entries older than Y seconds as definitely abandoned"
	 *
	 * ---
	 *
	 * ## How it is used
	 * {@link selectS} calls:
	 * `runScript('SelectStuck', [queueName], [ group, consumer, selectStuckTimeout, selectStuckCount, selectStuckMaxTimeout ])`
	 *
	 * The exact meaning depends on the Lua script implementation, but commonly it is used to:
	 * - avoid reclaiming tasks that are only slightly delayed,
	 * - and focus on tasks that are clearly abandoned (too old).
	 *
	 * ---
	 *
	 * ## Why it matters
	 * - Helps avoid "thrashing" (workers repeatedly stealing tasks from each other too early).
	 * - Makes recovery deterministic for very old pending entries after outages.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - Set it to a bit higher than your expected "long task" duration.
	 * - If tasks should never take more than ~30 seconds, values like `60`–`120` seconds are fine.
	 * - If tasks can take several minutes, increase this value accordingly.
	 *
	 * Default in this class: `80` seconds.
	 */
	public readonly selectStuckMaxTimeout: number = 80;

	/**
	 * Maximum number of **new** messages to read from the stream in one `XREADGROUP` call.
	 *
	 * This value is used by {@link selectF}, which reads fresh (never-delivered) messages using:
	 * `XREADGROUP ... COUNT <selectCount> STREAMS <queueName> >`
	 *
	 * So `selectCount` is the **batch size** for normal consumption of new tasks.
	 *
	 * ---
	 *
	 * ## Why it matters
	 * - Higher values improve throughput (fewer Redis round-trips),
	 *   but can increase memory usage and make each loop iteration heavier.
	 * - Lower values reduce per-iteration work, but may limit throughput when the queue is busy.
	 *
	 * This setting should be tuned together with:
	 * - {@link selectTimeout} (how long we block waiting for new messages)
	 * - {@link approveCount} (how many ACKs we do per request)
	 * - your average task execution time and worker concurrency
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - If tasks are fast and you want high throughput, values like `200`–`2000` can be good.
	 * - If tasks are heavy (slow / expensive), keep it smaller (e.g. `20`–`200`)
	 *   to avoid very long batches and large retry storms.
	 *
	 * Default in this class: `200`.
	 */
	public readonly selectCount: number = 200;

	/**
	 * How long (in milliseconds) the worker should **block-wait** for new messages
	 * when reading the queue via `XREADGROUP`.
	 *
	 * This value is used by {@link selectF} in the Redis command:
	 * `XREADGROUP ... BLOCK <selectTimeout> COUNT <selectCount> STREAMS <queueName> >`
	 *
	 * Meaning:
	 * - If there are new messages, Redis returns immediately.
	 * - If there are no new messages, Redis keeps the connection open and waits up to this
	 *   amount of time before returning `null` / empty response.
	 *
	 * ---
	 *
	 * ## Why it matters
	 * This setting is a trade-off between:
	 * - **low latency** (react quickly when messages appear),
	 * - and **low CPU / low Redis load** (avoid tight polling loops).
	 *
	 * With blocking reads, the worker is mostly idle when the queue is empty.
	 *
	 * ---
	 *
	 * ## Behavior in this implementation
	 * In {@link selectF} we call:
	 * `BLOCK Math.max(2, this.selectTimeout | 0)`
	 *
	 * So:
	 * - the value is forced to an integer (`| 0`)
	 * - and has a minimum of `2` ms (never truly zero)
	 *
	 * Default: `3000` ms (3 seconds).
	 *
	 * After `selectF` returns empty, {@link consumerLoop} additionally waits ~300ms
	 * before the next iteration, which further reduces idle spinning.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - Typical good defaults are `1000`–`5000` ms.
	 * - Lower values (e.g. `50`–`500` ms) can reduce "wake-up" latency slightly,
	 *   but increase Redis calls when the queue is empty.
	 * - Higher values (e.g. `10_000`+ ms) reduce Redis traffic even more, but can make
	 *   shutdown slightly less responsive (because the worker may be blocked in Redis longer).
	 *
	 * Default in this class: `3000` ms.
	 */
	public readonly selectTimeout: number = 3000;

	/**
	 * Maximum number of tasks to include in a single **producer batch** when enqueuing tasks.
	 *
	 * This value is used by {@link buildBatches} to split the input array passed to {@link addTasks}
	 * into smaller chunks.
	 *
	 * Why batching exists:
	 * - {@link addTasks} inserts tasks using the Lua script `XAddBulk`.
	 * - A single Redis call has practical limits (argument count / payload size).
	 * - Splitting into batches protects you from oversized requests and improves stability.
	 *
	 * `buildBatchCount` is the **first** batching limit:
	 * - If the current batch already contains `buildBatchCount` tasks, a new batch is started.
	 *
	 * The final batch size is also limited by {@link buildBatchMaxCount} (argv/token limit),
	 * so the real batch size can be smaller than `buildBatchCount` if tasks are large.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - For small payloads and high throughput, values like `500`–`2000` can be good.
	 * - For large payloads, reduce this number to avoid big Lua argv arrays and timeouts.
	 * - Always consider Redis network latency and your average payload size.
	 *
	 * Default in this class: `800`.
	 */
	public readonly buildBatchCount: number = 800;

	/**
	 * Maximum allowed size of a producer batch expressed as an approximate **Redis argv token limit**.
	 *
	 * This value is used by {@link buildBatches} as a safety cap to prevent creating a batch
	 * that would produce an excessively large argument list for the `XAddBulk` Lua script call.
	 *
	 * In Redis, a Lua script is invoked like:
	 * `EVALSHA <sha> <numKeys> <keys...> <args...>`
	 *
	 * For bulk `XADD`, the number of `<args...>` grows quickly with:
	 * - how many tasks you enqueue in one batch,
	 * - and how many fields each task contains (payload, createdAt, job, idemKey, attempt, etc.).
	 *
	 * This setting helps prevent:
	 * - oversized requests,
	 * - long Lua execution time,
	 * - hitting Redis/proxy limits,
	 * - and increased memory pressure.
	 *
	 * ---
	 *
	 * ## How it is calculated in your code
	 * {@link buildBatches} estimates the "argument size" using `keysLength(task)`:
	 *
	 * - `keysLength(task)` returns: `2 + Object.keys(task).length * 2`
	 *   (an approximation of how many argv tokens will be needed for that task)
	 *
	 * During batch building:
	 * - `realKeysLength` accumulates `reqKeysLength` for each task
	 * - If adding the next task would exceed `buildBatchMaxCount`,
	 *   the current batch is closed and a new batch is started.
	 *
	 * So `buildBatchMaxCount` is the **second** batching limit (in addition to {@link buildBatchCount}).
	 *
	 * ---
	 *
	 * ## Why the name contains "Count"
	 * It does **not** mean "max tasks".
	 * It means "max argv tokens" (approximate count of script arguments).
	 * This is important because:
	 * - 800 small tasks might be fine,
	 * - but 800 tasks with large metadata or many fields might create a huge argv list.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - If you see Redis errors related to argument size, timeouts, or slow scripts,
	 *   reduce this value (e.g. `5000`–`8000`).
	 * - If tasks are small and you want fewer Redis round-trips, you can increase it,
	 *   but test carefully (bigger calls can increase latency spikes).
	 *
	 * Default in this class: `10000`.
	 */
	public readonly buildBatchMaxCount: number = 10000;

	/**
	 * Maximum number of attempts allowed for a task (including the first attempt).
	 *
	 * This value controls the retry behavior when {@link onExecute} throws an error.
	 *
	 * How it is interpreted in code:
	 * - `attempt` starts from `0` for the first run.
	 * - A retry is scheduled while:
	 *   `attempt < (retryCount - 1)`
	 *
	 * So:
	 * - `retryCount = 1`  → no retries (only attempt `0`)
	 * - `retryCount = 2`  → one retry (attempts `0` and `1`)
	 * - `retryCount = 3`  → two retries (attempts `0`, `1`, `2`)
	 *
	 * This logic is implemented in {@link error} and also in {@link batchError}.
	 *
	 * ---
	 *
	 * ## What happens on failure
	 * When a task fails:
	 * - If more attempts remain:
	 *   - {@link onRetry} is called
	 *   - the task is re-enqueued into the same queue with `attempt + 1`
	 * - If attempts are exhausted:
	 *   - if {@link logStatus} is enabled, the task is sent to the DLQ queue (`<queueName>:dlq`)
	 *   - {@link onError} is called in both cases (after retry/DLQ decision)
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - For non-critical tasks where duplicates are acceptable: `2`–`5` retries may be fine.
	 * - For critical tasks: combine retries with idempotency and good error classification.
	 * - Avoid very high values unless you also implement backoff/delay, otherwise you can
	 *   create retry storms under outages.
	 *
	 * Default in this class: `1` (no retries).
	 */
	public readonly retryCount: number = 1;

	/**
	 * Controls whether tasks in a batch are executed **sequentially** or **concurrently**.
	 *
	 * This flag is used inside {@link execute}:
	 * - `executeSync = false` (default): tasks are executed concurrently using `Promise.all`.
	 * - `executeSync = true`: tasks are executed one by one (await each task before starting next).
	 *
	 * ---
	 *
	 * ## Why this setting exists
	 * Concurrent execution can drastically increase throughput, but it can also:
	 * - overload external services (API rate limits)
	 * - overload your database
	 * - increase memory usage
	 * - increase lock contention if many tasks share the same {@link idemKey}
	 *
	 * Sequential execution is slower, but it is simpler and can be safer when:
	 * - your task handler is not concurrency-safe
	 * - you must preserve ordering
	 * - you must avoid bursts to downstream systems
	 *
	 * ---
	 *
	 * ## Behavior in this implementation
	 * In {@link execute}:
	 * - When `executeSync` is false, the code collects async functions in `promises`
	 *   and runs them all with `Promise.all(...)`.
	 * - When `executeSync` is true, it awaits each {@link executeProcess} directly in the loop.
	 *
	 * This setting does **not** limit concurrency to a specific number (like 10 workers).
	 * It is either:
	 * - fully parallel (one promise per task in the selected batch),
	 * - or fully sequential.
	 *
	 * If you need a fixed concurrency limit, you would implement a small pool/queue in {@link execute}.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - Keep `false` for most high-throughput use cases where your downstream systems can handle concurrency.
	 * - Set `true` if you see:
	 *   - frequent rate-limit errors,
	 *   - contention spikes,
	 *   - or you need strict ordering guarantees.
	 *
	 * Default in this class: `false` (concurrent execution).
	 */
	public readonly executeSync: boolean = false;

	/**
	 * Idempotency lock TTL (time-to-live) in **milliseconds**.
	 *
	 * This value controls how long a worker keeps the idempotency "lock" alive in Redis
	 * while it is processing a task with a given `idemKey`.
	 *
	 * Idempotency in this queue is implemented with Redis keys:
	 * - `doneKey`  — marks that this `idemKey` was already completed recently
	 * - `lockKey`  — a mutex-like lock to prevent two workers executing the same `idemKey` at once
	 * - `startKey` — a marker that execution has started (used for waiting/contended behavior)
	 *
	 * `idemLockTimeout` is used as the TTL for `lockKey` and `startKey`.
	 *
	 * ---
	 *
	 * ## Where it is used
	 * - {@link idempotencyAllow} passes it to the Lua gate script (`IdempotencyAllow`)
	 * - {@link idempotencyStart} sets it when acquiring the lock (`IdempotencyStart`)
	 * - {@link heartbeat} periodically refreshes TTL via {@link sendHeartbeat}
	 *
	 * In code:
	 * - `IdempotencyAllow(..., [ String(this.idemLockTimeout), token ])`
	 * - `IdempotencyStart(..., [ token, String(this.idemLockTimeout) ])`
	 * - `sendHeartbeat()` calls `PEXPIRE(lockKey, idemLockTimeout)` and `PEXPIRE(startKey, idemLockTimeout)`
	 *
	 * ---
	 *
	 * ## Why it matters
	 * - Too small:
	 *   - long-running tasks may outlive the lock TTL,
	 *   - another worker can acquire the same `idemKey` and run duplicate work.
	 *
	 * - Too large:
	 *   - if a worker crashes, the lock stays in Redis for longer,
	 *   - other workers will treat the task as "contended" and wait longer before retrying.
	 *
	 * Heartbeat reduces the risk for long tasks by extending the TTL while the worker is alive.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - Choose a value slightly larger than your typical **maximum task execution time**.
	 * - If tasks can take ~30 seconds, use something like 60–120 seconds.
	 * - If tasks can take several minutes, use several minutes (and keep heartbeat enabled).
	 * - For very fast tasks, you can lower it, but keep some buffer for GC pauses / network hiccups.
	 *
	 * Default in this class: `180_000` ms (3 minutes).
	 */
	public readonly idemLockTimeout: number = 180000;

	/**
	 * Idempotency "done" TTL (time-to-live) in **milliseconds**.
	 *
	 * After a task with a given `idemKey` is executed successfully, the queue marks it as completed
	 * by writing a `doneKey` in Redis. This prevents duplicate execution if the same logical task
	 * is delivered again (for example, due to retries, consumer rebalancing, or producer duplication).
	 *
	 * `idemDoneTimeout` controls how long that "already done" marker remains in Redis.
	 *
	 * ---
	 *
	 * ## Where it is used
	 * It is passed to the Lua script {@link IdempotencyDone}:
	 * - {@link idempotencyDone} calls:
	 *   `runScript('IdempotencyDone', [doneKey, lockKey, startKey], [ String(idemDoneTimeout), token ])`
	 *
	 * Typically the script:
	 * - sets `doneKey` with TTL = `idemDoneTimeout`
	 * - deletes or releases `lockKey` and `startKey`
	 *
	 * ---
	 *
	 * ## Why it matters
	 * - Higher values increase protection against duplicates (stronger idempotency window),
	 *   but keep more keys in Redis for longer (more memory usage).
	 * - Lower values reduce memory usage, but allow duplicates to execute again sooner
	 *   if the same `idemKey` is reintroduced after the TTL expires.
	 *
	 * You can think of it as:
	 * **"How long do we remember that this idemKey was already processed?"**
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * Pick this based on how long duplicates might realistically occur in your system:
	 * - If duplicates can happen within seconds/minutes (common), 30–300 seconds is fine.
	 * - If duplicates can happen much later (rare), increase it — but monitor Redis memory.
	 * - If `idemKey` is unique per message (UUID), you can keep it relatively small,
	 *   because duplicates are mostly immediate.
	 *
	 * Default in this class: `60_000` ms (60 seconds).
	 */
	public readonly idemDoneTimeout: number = 60000;

	/**
	 * Enable or disable lightweight queue status tracking in Redis.
	 *
	 * When `logStatus` is `true`, the queue writes simple counters into Redis to track
	 * how many tasks were:
	 * - processed successfully (`ok`)
	 * - failed (`err`)
	 * - and "ready" (a total-like counter updated on both success and error paths)
	 *
	 * Counters are stored per queue and per job:
	 * Key prefix format:
	 * `"<queueName>:<job>:"`
	 *
	 * Keys that may be written:
	 * - `"<queueName>:<job>:ok"`
	 * - `"<queueName>:<job>:err"`
	 * - `"<queueName>:<job>:ready"`
	 *
	 * These counters are incremented in:
	 * - {@link success} (on success → `ok` and `ready`)
	 * - {@link error} and {@link batchError} (on final failure / DLQ path → `err` and `ready`)
	 *
	 * All status keys are set with TTL = {@link logStatusTimeout}, so they expire automatically.
	 *
	 * ---
	 *
	 * ## Why it matters
	 * Turning this on gives you a very cheap way to monitor queue progress without external tooling:
	 * - "How many tasks have completed for job X?"
	 * - "How many errors happened for job X?"
	 *
	 * Turning it off removes this Redis write overhead, which can matter at very high throughput.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - Enable it in production if you want basic visibility and the extra Redis writes are acceptable.
	 * - Disable it if you already have strong observability (Prometheus, logs, tracing)
	 *   or if you need absolute maximum throughput.
	 *
	 * Default in this class: `false`.
	 */
	public readonly logStatus: boolean = false;

	/**
	 * TTL (time-to-live) in **milliseconds** for status/metrics keys written when {@link logStatus} is enabled.
	 *
	 * When `logStatus = true`, the queue increments counters in Redis such as:
	 * - `"<queueName>:<job>:ok"`
	 * - `"<queueName>:<job>:err"`
	 * - `"<queueName>:<job>:ready"`
	 * and also (when {@link addTasks} is called with `opts.status = true`):
	 * - `"<queueName>:<job>:total"`
	 *
	 * This setting controls how long those keys remain in Redis before expiring automatically.
	 *
	 * ---
	 *
	 * ## Why it matters
	 * - Higher TTL keeps history longer (useful for debugging and monitoring),
	 *   but increases Redis memory usage (keys stick around longer).
	 * - Lower TTL reduces memory usage, but you may lose visibility sooner
	 *   (status keys disappear quickly).
	 *
	 * You can think of it as:
	 * **"How long should job counters remain available after the job finishes?"**
	 *
	 * ---
	 *
	 * ## Where it is used
	 * - {@link success}: increments `ok` and `ready` with TTL = `logStatusTimeout`
	 * - {@link error}: increments `err` and `ready` with TTL = `logStatusTimeout`
	 * - {@link batchError}: may set/increment counters with TTL = `logStatusTimeout`
	 * - {@link addTasks}: sets `<queueName>:<job>:total` and applies TTL = `logStatusTimeout`
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - If you want to see recent jobs for a few hours, use something like `1h`–`12h`.
	 * - If you want to keep counters for 1–7 days, increase it, but monitor Redis memory.
	 * - If you have external monitoring, a shorter TTL is usually enough.
	 *
	 * Default in this class: `1_800_000` ms (30 minutes).
	 */
	public readonly logStatusTimeout: number = 1800000;

	/**
	 * Target maximum number of task IDs to acknowledge (approve) in a single ACK batch.
	 *
	 * This value is used by {@link approve} to control how many Redis Stream entries
	 * are acknowledged at once using the Lua script `Approve`.
	 *
	 * Why batching ACKs exists:
	 * - Acknowledging each message one-by-one is slow (many Redis round-trips).
	 * - Acknowledging too many in one call can create huge argv payloads and latency spikes.
	 *
	 * So {@link approve} clamps this value to a safe range:
	 * ```ts
	 * const approveCount = Math.max(500, Math.min(4000, this.approveCount));
	 * ```
	 *
	 * Meaning:
	 * - minimum effective batch size is **500**
	 * - maximum effective batch size is **4000**
	 * - values outside that range will be forced into it
	 *
	 * ---
	 *
	 * ## What "approve" means here
	 * Approving a task means:
	 * - removing it from the consumer group's Pending Entries List (PEL),
	 * - and optionally deleting it from the stream entirely if {@link removeOnExecuted} is true
	 *   (depends on what the `Approve` Lua script does).
	 *
	 * This is typically equivalent to Redis `XACK` (and sometimes `XDEL`),
	 * but implemented as a single Lua script for performance and atomic behavior.
	 *
	 * ---
	 *
	 * ## Why it matters
	 * - Larger values reduce Redis round-trips under high throughput,
	 *   but increase per-call payload size and can increase latency spikes.
	 * - Smaller values create more Redis calls, which can limit throughput.
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - Keep it in the `500`–`4000` range (anything else is clamped anyway).
	 * - If you have very high throughput and stable Redis, `2000`–`4000` can be efficient.
	 * - If you see latency spikes during ACK, reduce it closer to `500`–`1000`.
	 *
	 * Default in this class: `2000`.
	 */
	public readonly approveCount: number = 2000;
	
	/**
	 * Controls whether successfully acknowledged tasks should also be **removed from the Redis Stream**.
	 *
	 * This flag is passed into the {@link Approve} Lua script by {@link approve}:
	 * - `'1'` when `removeOnExecuted` is `true`
	 * - `'0'` when `removeOnExecuted` is `false`
	 *
	 * Conceptually, there are two common ways to use Redis Streams as a queue:
	 *
	 * 1) **Keep stream history** (removeOnExecuted = false)
	 *    - Messages are acknowledged (removed from the consumer group's pending list),
	 *      but remain in the stream.
	 *    - Pros:
	 *      - You keep an audit/history of what was produced.
	 *      - You can create new consumer groups later and replay old messages (if desired).
	 *    - Cons:
	 *      - Stream grows forever unless you trim it (`MAXLEN`, `MINID`, etc.).
	 *
	 * 2) **Delete after processing** (removeOnExecuted = true)
	 *    - Messages are acknowledged and then deleted from the stream (usually via `XDEL`).
	 *    - Pros:
	 *      - Stream does not accumulate processed messages (acts more like a classic queue).
	 *      - Lower Redis memory usage without requiring trimming.
	 *    - Cons:
	 *      - You lose history (cannot replay the exact same message later).
	 *      - Debugging/auditing requires external logs/metrics.
	 *
	 * ---
	 *
	 * ## How it is used
	 * In {@link approve}:
	 * ```ts
	 * this.runScript('Approve', [queueName], [
	 *   this.group,
	 *   this.removeOnExecuted ? '1' : '0',
	 *   ...ids
	 * ], Approve);
	 * ```
	 *
	 * The exact behavior depends on the `Approve` Lua script implementation, but typically:
	 * - always performs `XACK` for the given ids
	 * - optionally performs `XDEL` (or similar cleanup) when the flag is `1`
	 *
	 * ---
	 *
	 * ## Tuning recommendations
	 * - Set to `true` if you want queue-like behavior and do not need message history.
	 * - Set to `false` if you need replay/audit and you also configure trimming
	 *   (`maxlen`, `minid*`, etc.) to avoid unbounded growth.
	 *
	 * Default in this class: `true`.
	 */
	public readonly removeOnExecuted: boolean = true;

	/** @hidden */
	private signal() {
		return this.abort.signal;
	}

	/** @hidden */
	private consumer(): string {
		return this.host +':'+ process.pid;
	}

	/**
	 * Start consuming a Redis Stream queue with a consumer group and keep processing messages
	 * until the queue is stopped via {@link abort}.
	 *
	 * This method is the **main entry point** to run a queue worker:
	 *
	 * 1) It ensures a **consumer group** exists for the given stream (queue).
	 * 2) It starts an **infinite consume loop** that reads tasks from Redis Streams and executes them.
	 * 3) It can be stopped gracefully by calling `this.abort.abort()`.
	 *
	 * ---
	 *
	 * ## What is a "queue" here?
	 * In this library a queue is implemented as a **Redis Stream**.
	 * - `queueName` is the stream key in Redis.
	 * - Tasks are stored as stream entries.
	 *
	 * ## What is a "consumer group"?
	 * Redis Streams consumer groups allow multiple workers to consume the same stream
	 * without processing the same message twice.
	 * - `this.group` is the consumer group name (default: `"gr1"`).
	 * - `this.consumer()` returns a unique consumer id (default: `"host:<pid>"`).
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param queueName - Redis Stream key to consume from (e.g. `"emails"`, `"alerts"`, `"payments"`).
	 * Must be a valid Redis key. If it doesn't exist yet, it will be created automatically
	 * (because `createGroup()` uses `MKSTREAM`).
	 *
	 * @param from - Where to start the consumer group if it is being created right now:
	 * - `"$"` (recommended for production): start from **new messages only** (ignore old history).
	 * - `"0-0"` (useful for dev/backfills): start from the **beginning of the stream**.
	 *
	 * Important:
	 * - `from` is only used when the group is created. If the group already exists,
	 *   Redis will continue from where it left off (pending/last delivered), regardless of `from`.
	 *
	 * Default is `"0-0"` in this code, which means "start from the very beginning"
	 * on first creation of the group.
	 *
	 * ---
	 *
	 * ## How it works internally
	 * ### 1) Remove listener limits for AbortSignal
	 * `setMaxListeners(0, this.abort.signal)` disables the warning about "MaxListenersExceededWarning".
	 * This is useful because the worker loop may attach abort listeners multiple times
	 * (for example inside {@link waitAbortable}), and we don't want Node.js to treat that as a leak.
	 *
	 * ### 2) Create the consumer group (if missing)
	 * {@link createGroup} calls:
	 * `XGROUP CREATE <queueName> <group> <from> MKSTREAM`
	 *
	 * - If the group already exists, Redis replies with `BUSYGROUP` → we ignore it.
	 * - If the stream doesn't exist, `MKSTREAM` creates it automatically.
	 *
	 * ### 3) Start the consumer loop
	 * {@link consumerLoop} is an infinite loop that:
	 * - Reads tasks (first tries "stuck" tasks, then reads new ones)
	 * - Parses entries into your internal tuple format
	 * - Calls {@link beforeExecute} (hook)
	 * - Executes tasks via {@link onExecute} (hook) with idempotency + heartbeat
	 * - Acknowledges (approves) successfully handled entries via Lua script `Approve`
	 * - Handles failures with retry / DLQ logic via {@link batchError}
	 *
	 * The loop runs until `this.abort.abort()` is called (i.e. until `AbortSignal.aborted === true`).
	 *
	 * ---
	 *
	 * ## Typical usage
	 * ```ts
	 * const q = new PowerQueues();
	 * q.redis = redisClient;
	 * await q.loadScripts(true);
	 *
	 * // Start worker for "alerts" queue. New messages only:
	 * await q.runQueue('alerts', '$');
	 *
	 * // ...later, to stop:
	 * q.abort.abort();
	 * ```
	 *
	 * ---
	 *
	 * ## Notes & best practices
	 * - Prefer `from = '$'` for long-running production workers to avoid consuming historical data
	 *   when a group is created for the first time.
	 * - Use `"0-0"` only when you really want to process the entire stream history.
	 * - To run multiple workers for the same queue, start multiple processes/containers.
	 *   Each process will have a different `consumer()` value because it includes `process.pid`.
	 *
	 * @returns A promise that resolves only when the loop stops (i.e., when aborted).
	 * In normal operation it runs forever.
	 *
	 * @throws Any error from {@link createGroup} that is not a "BUSYGROUP" error.
	 * Other errors inside the consume loop are handled internally and do not crash the worker by design.
	 */
	async runQueue(queueName: string, from: '$' | '0-0' = '0-0') {
		setMaxListeners(0, this.abort.signal);

		await this.createGroup(queueName, from);
		await this.consumerLoop(queueName, from);
	}

	/**
	 * Ensure that a Redis Stream **consumer group** exists for the given queue (stream).
	 *
	 * In Redis Streams, a consumer group must be created **once** before any worker can read
	 * messages using `XREADGROUP`.
	 *
	 * This method is **safe to call on every startup**:
	 * - If the group already exists, Redis throws a `BUSYGROUP` error → we ignore it.
	 * - If the stream does not exist yet, `MKSTREAM` will create it automatically.
	 *
	 * ---
	 *
	 * ## What it creates
	 * It executes the Redis command:
	 *
	 * `XGROUP CREATE <queueName> <group> <from> MKSTREAM`
	 *
	 * Where:
	 * - `<queueName>` is the Redis Stream key (your queue name).
	 * - `<group>` is `this.group` (default in your class: `"gr1"`).
	 * - `<from>` controls the **starting point** *only at the moment of group creation*.
	 * - `MKSTREAM` makes Redis create an empty stream if it doesn't exist yet.
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param queueName - Redis Stream key for the queue. Example: `"alerts"`, `"emails"`.
	 *
	 * @param from - Starting ID used **only if the group is created now**:
	 * - `"$"`: start from new messages only (ignore historical entries).
	 * - `"0-0"`: start from the beginning of the stream (consume full history).
	 *
	 * If the group already exists, Redis ignores `from` (because the group’s position is already set).
	 *
	 * ---
	 *
	 * ## Error handling behavior
	 * - If Redis returns an error that contains `"BUSYGROUP"`:
	 *   it means the consumer group already exists → this is not a real error → we do nothing.
	 *
	 * - Any other error is re-thrown because it likely indicates a real problem:
	 *   - permission issues / ACL restrictions
	 *   - Redis connection issues
	 *   - invalid stream name, etc.
	 *
	 * ---
	 *
	 * ## Typical usage
	 * You usually don't call this manually; {@link runQueue} calls it before starting
	 * {@link consumerLoop}.
	 *
	 * ```ts
	 * await this.createGroup('alerts', '$');
	 * // now you can safely use XREADGROUP on "alerts" with group "gr1"
	 * ```
	 *
	 * @returns Resolves when the group exists (either created now or already existed).
	 *
	 * @throws Re-throws any Redis error except `"BUSYGROUP"`.
	 */
	async createGroup(queueName: string, from: '$' | '0-0' = '0-0') {
		try {
			await (this.redis as any).xgroup('CREATE', queueName, this.group, from, 'MKSTREAM');
		}
		catch (err: any) {
			const msg = String(err?.message || '');
			
			if (!msg.includes('BUSYGROUP')) {
				throw err;
			}
		}
	}

	/**
	 * Main worker loop that continuously consumes tasks from a Redis Stream and processes them.
	 *
	 * This method is designed to run **forever** in normal operation and stops only when
	 * the queue is aborted via {@link abort} (i.e. `this.abort.abort()` is called).
	 *
	 * It follows this high-level pipeline:
	 *
	 * 1) **Read** tasks from Redis (first try "stuck" / pending tasks, then new tasks)
	 * 2) If nothing is available → **sleep a bit** and retry
	 * 3) **Preprocess** tasks with {@link beforeExecute} (hook)
	 * 4) **Execute** tasks with {@link execute} which calls {@link onExecute} (hook) per task
	 * 5) **Acknowledge** processed tasks via {@link approve} (removes them from PEL / stream depending on config)
	 * 6) On any batch error → perform retry/DLQ logic via {@link batchError}, then try to acknowledge
	 *    the original messages to avoid infinite re-delivery, then continue looping.
	 *
	 * ---
	 *
	 * ## Why a loop is needed
	 * Redis Streams do not "push" messages to consumers by default. Workers need to poll (or block-read)
	 * for new entries. We use a loop with blocking reads and small waits to balance:
	 * - low latency (handle messages quickly),
	 * - and low CPU usage (avoid tight spin loops).
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param queueName - Redis Stream key (queue) to consume.
	 *
	 * @param from - Group creation start point (passed only for recovery when group is missing).
	 * This value is used only if Redis replies with `NOGROUP` and we need to call {@link createGroup}.
	 * See {@link createGroup} for details about `"$"` vs `"0-0"`.
	 *
	 * Default: `"0-0"`.
	 *
	 * ---
	 *
	 * ## Detailed behavior
	 * ### Stop condition
	 * The loop checks `this.abort.signal.aborted`. Once it becomes `true`,
	 * the loop exits and the promise resolves.
	 *
	 * ### Reading tasks: {@link select}
	 * `select()` tries two strategies:
	 * - {@link selectS}: a Lua-based fetch of **stuck/pending** messages (unacked for too long)
	 * - {@link selectF}: a normal `XREADGROUP ... STREAMS <queueName> >` fetch of **new** messages
	 *
	 * The result is normalized by {@link selectP} into tuples:
	 * `[ id, payload, createdAt, job, idemKey, attempt ]`
	 *
	 * ### When no tasks were read
	 * If the queue is empty (or nothing is currently available), we wait ~300ms and retry.
	 * This prevents CPU spinning when idle.
	 *
	 * ### Execution and acknowledgment
	 * The main processing call:
	 *
	 * `approve(queueName, await execute(queueName, await beforeExecute(queueName, tasks)))`
	 *
	 * - {@link beforeExecute} is a hook to filter, reorder, enrich, or validate the batch.
	 * - {@link execute} runs tasks and returns only those that were actually processed successfully.
	 * - {@link approve} acknowledges those successful tasks (and optionally removes them from the stream).
	 *
	 * ### Error handling (batch-level)
	 * If anything in the processing pipeline throws:
	 * - {@link batchError} is called to:
	 *   - group failed tasks by `(attempt, createdAt, job)` (see implementation)
	 *   - re-enqueue tasks if retries are allowed
	 *   - or move them to DLQ when retries are exhausted (if `logStatus` is enabled)
	 *   - call {@link onBatchError} hook
	 *
	 * After that, the code *tries* to acknowledge the original messages anyway using {@link approve}.
	 * This is important because otherwise the same broken batch could get re-delivered forever
	 * as pending/stuck messages.
	 *
	 * Finally it waits ~300ms and continues the loop.
	 *
	 * ---
	 *
	 * ## Notes & best practices
	 * - This method intentionally **swallows** read errors and keeps running. In a worker system,
	 *   intermittent Redis/network hiccups are expected.
	 * - If you want crash-on-fail behavior, implement it inside {@link onBatchError} or override
	 *   this method and rethrow.
	 * - For graceful shutdown, call `this.abort.abort()` and optionally wait for `consumerLoop`
	 *   to resolve.
	 *
	 * @returns A promise that resolves when the loop stops (when aborted).
	 */
	async consumerLoop(queueName: string, from: '$' | '0-0' = '0-0') {
		const signal = this.signal();

		while (!signal?.aborted) {
			let tasks: any[] = [];

			try {
				tasks = await this.select(queueName, from);
			}
			catch (err) {
			}
			if (!isArrFilled(tasks)) {
				await wait(300);
				continue;
			}
			try {
				await this.approve(queueName, await this.execute(queueName, await this.beforeExecute(queueName, tasks)));
			}
			catch (err) {
				await this.batchError(err, queueName, tasks);

				try {
					await this.approve(queueName, tasks.map((task) => ({
						id: task[0],
						createdAt: Number(task[2]),
						payload: task[1],
						job: task[3],
						idemKey: task[4],
						attempt: Number(task[5] || 0),
					})));
				}
				catch {
				}
				await wait(300);
			}
		}
	}

	/** @hidden */
	private async batchError(err: any, queueName: string, tasks: Array<[ string, any, number, string, string, number ]>) {
		try {
			const filtered: any = {};

			tasks.forEach((task, index) => {
				const key = JSON.stringify([ task[5] || '0', task[2], task[3] ]);

				if (!filtered[key]) {
					filtered[key] = [];
				}
				filtered[key].push({ ...tasks[index][1], idemKey: tasks[index][4] });
			});

			for (let key in filtered) {
				const filteredTasks = filtered[key];
				const keyP = JSON.parse(key);
				const attempt = Number(keyP[0] || 0);
				const job = String(keyP[2]);

				if (!(attempt >= (this.retryCount - 1))) {
					await this.addTasks(queueName, filteredTasks, {
						job,
						attempt: attempt + 1,
					});
				}
				else if (this.logStatus) {
					const statusKey = `${queueName}:${job}:`;

					await this.setOne(statusKey +'err', Number(await this.getOne(statusKey +'err') || 0) + filteredTasks.length, this.logStatusTimeout);
					await this.setOne(statusKey +'ready', Number(await this.getOne(statusKey +'ready') || 0) + filteredTasks.length, this.logStatusTimeout);
					await this.addTasks(queueName +':dlq', filteredTasks, {
						job,
					});
				}
			}
		}
		catch (err: any) {
		}
		try {
			await this.onBatchError(err, queueName, tasks);
		}
		catch (err: any) {
		}
	}

	/** @hidden */
	private async approve(queueName: string, tasks: Task[]) {
		if (!isArrFilled(tasks)) {
			return 0;
		}
		const approveCount = Math.max(500, Math.min(4000, this.approveCount));
		let total = 0,
			i = 0;

		while (i < tasks.length) {
			const room = Math.min(approveCount, tasks.length - i);
			const part = tasks.slice(i, i + room).map((item) => String(item.id || ''));
			const approved = await this.runScript('Approve', [ queueName ], [ this.group, this.removeOnExecuted ? '1' : '0', ...part ], Approve);

			total += Number(approved || 0);
			i += room;
		}
		return total;
	}

	/**
	 * Select (read) a batch of tasks from the queue (Redis Stream).
	 *
	 * This method is a **high-level read operation** used by {@link consumerLoop}.
	 * It combines two different read strategies:
	 *
	 * 1) Try to get **stuck / pending** messages first (messages that were delivered earlier
	 *    but were not acknowledged due to a crash, timeout, or worker failure).
	 * 2) If there are no stuck messages, read **new** messages from the stream using `XREADGROUP`.
	 * 3) Normalize and parse the raw Redis response into an internal tuple format.
	 *
	 * The return value is always an array of tuples:
	 * `[ id, payload, createdAt, job, idemKey, attempt ]`
	 *
	 * ---
	 *
	 * ## Why "stuck first"?
	 * Redis Streams consumer groups keep unacknowledged messages in a Pending Entries List (PEL).
	 * If a worker dies after receiving a message but before acknowledging it,
	 * that message becomes "pending" and will not be delivered again as a "new" message (`>`).
	 *
	 * To avoid losing such messages, we intentionally:
	 * - first attempt to reclaim/collect stuck pending messages via {@link selectS},
	 * - and only then read brand-new messages via {@link selectF}.
	 *
	 * This gives your queue **at-least-once** delivery behavior.
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param queueName - Redis Stream key (queue) to read from.
	 *
	 * @param from - Used only for recovery if the consumer group is missing (`NOGROUP`).
	 * In that case we call {@link createGroup(queueName, from)}.
	 * See {@link createGroup} for details.
	 *
	 * Default: `"0-0"`.
	 *
	 * ---
	 *
	 * ## Return value format
	 * Each returned item is:
	 * - `id` (string): Redis Stream entry id (e.g. `"1700000000000-0"`).
	 * - `payload` (any): Parsed JSON payload (or raw string if JSON parsing fails).
	 * - `createdAt` (number): Timestamp stored in message fields (expected ms).
	 * - `job` (string): Job identifier stored in message fields.
	 * - `idemKey` (string): Idempotency key stored in message fields.
	 * - `attempt` (number): Retry attempt counter stored in message fields.
	 *
	 * If Redis returns nothing, an empty array is returned.
	 *
	 * ---
	 *
	 * ## Typical usage
	 * You normally don't call this directly. It is called inside {@link consumerLoop}:
	 *
	 * ```ts
	 * const tasks = await this.select('alerts', '$');
	 * if (tasks.length) {
	 *   // handle tasks...
	 * }
	 * ```
	 *
	 * @returns A promise that resolves to an array of normalized tasks.
	 * Returns an empty array if no tasks are available.
	 */
	async select(queueName: string, from: '$' | '0-0' = '0-0'): Promise<any[]> {
		let selected = await this.selectS(queueName, from);

		if (!isArrFilled(selected)) {
			selected = await this.selectF(queueName, from);
		}
		return this.selectP(selected);
	}

	/** @hidden */
	private async selectS(queueName: string, from: '$' | '0-0' = '0-0'): Promise<any[]> {
		try {
			const res = await this.runScript('SelectStuck', [ queueName ], [ this.group, this.consumer(), String(this.selectStuckTimeout), String(this.selectStuckCount), String(this.selectStuckMaxTimeout) ], SelectStuck);

			return (isArr(res) ? res : []) as any[];
		}
		catch (err: any) {
			if (String(err?.message || '').includes('NOGROUP')) {
				await this.createGroup(queueName, from);
			}
		}
		return [];
	}

	/** @hidden */
	private async selectF(queueName: string, from: '$' | '0-0' = '0-0'): Promise<any[]> {
		let rows = [];

		try {
			const res = await (this.redis as any).xreadgroup(
				'GROUP', this.group, this.consumer(),
				'BLOCK', Math.max(2, this.selectTimeout | 0),
				'COUNT', this.selectCount,
				'STREAMS', queueName, '>',
			);

			rows = res?.[0]?.[1] ?? [];

			if (!isArrFilled(rows)) {
				return [];
			}
		}
		catch (err: any) {
			if (String(err?.message || '').includes('NOGROUP')) {
				await this.createGroup(queueName, from);
			}
		}
		return rows;
	}

	/** @hidden */
	private selectP(raw: any): Array<[ string, any, number, string, string, number ]> {
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
				const { payload, createdAt, job, idemKey, attempt } = this.values(kv);

				return [ id, this.payload(payload), createdAt, job, idemKey, Number(attempt) ];
			});
	}

	/** @hidden */
	private values(value: any[]) {
		const result: any = {};

		for (let i = 0; i < value.length; i += 2) {
			result[value[i]] = value[i + 1];
		}
		return result;
	}

	/** @hidden */
	private payload(data: any): any {
		try {
			return JSON.parse(data);
		}
		catch (err) {
		}
		return data;
	}

	/** @hidden */
	private async execute(queueName: string, tasks: Array<[ string, any, number, string, string, number ]>): Promise<Task[]> {
		const result: Task[] = [];
		let contended = 0,
			promises = [];

		for (const [ id, payload, createdAt, job, idemKey, attempt ] of tasks) {
			if (!this.executeSync) {
				promises.push(async () => {
					const r = await this.executeProcess(queueName, { id, payload, createdAt, job, idemKey, attempt });

					if (r.id) {
						result.push(r);
					}
					else if (r.contended) {
						contended++;
					}
				});
			}
			else {
				const r = await this.executeProcess(queueName, { id, payload, createdAt, job, idemKey, attempt });

				if (r.id) {
					result.push(r);
				}
				else if (r.contended) {
					contended++;
				}
			}
		}
		let start = Date.now();

		if (!this.executeSync && promises.length > 0) {
			await Promise.all(promises.map((item) => item()));
		}
		await this.onBatchReady(queueName, result);

		if (!isArrFilled(result) && contended > (tasks.length >> 1)) {
			await this.waitAbortable((15 + Math.floor(Math.random() * 35)) + Math.min(250, 15 * contended + Math.floor(Math.random() * 40)));
		}
		return result;
	}

	/** @hidden */
	private async executeProcess(queueName: string, task: Task): Promise<any> {
		const keys = this.idempotencyKeys(queueName, String(task.idemKey));
		const allow = await this.idempotencyAllow(keys);

		if (allow === 1) {
			return { contended: true };
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
			const processed = await this.onExecute(queueName, task);
			
			await this.idempotencyDone(keys);
			return await this.success(queueName, processed);
		}
		catch (err: any) {
			try {
				await this.idempotencyFree(keys);
				return await this.error(err, queueName, task);
			}
			catch (err2: any) {
			}
		}
		finally {
			heartbeat();
		}
	}

	/** @hidden */
	private idempotencyKeys(queueName: string, key: string): IdempotencyKeys {
		const prefix = `q:${queueName.replace(/[^\w:\-]/g, '_')}:`;
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

	/** @hidden */
	private async idempotencyAllow(keys: IdempotencyKeys): Promise<0 | 1 | 2> {
		const res = await this.runScript('IdempotencyAllow', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.idemLockTimeout), keys.token ], IdempotencyAllow);

		return Number(res || 0) as 0 | 1 | 2;
	}

	/** @hidden */
	private async idempotencyStart(keys: IdempotencyKeys): Promise<boolean> {
		const res = await this.runScript('IdempotencyStart', [ keys.lockKey, keys.startKey ], [ keys.token, String(this.idemLockTimeout) ], IdempotencyStart);

		return Number(res || 0) === 1;
	}

	/** @hidden */
	private async idempotencyDone(keys: IdempotencyKeys): Promise<void> {
		await this.runScript('IdempotencyDone', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.idemDoneTimeout), keys.token ], IdempotencyDone);
	}

	/** @hidden */
	private async idempotencyFree(keys: IdempotencyKeys): Promise<void> {
		await this.runScript('IdempotencyFree', [ keys.lockKey, keys.startKey ], [ keys.token ], IdempotencyFree);
	}

	/** @hidden */
	private async success(queueName: string, task: Task) {
		if (this.logStatus) {
			const statusKey = `${queueName}:${task.job}:`;

			await this.incr(statusKey +'ok', this.logStatusTimeout);
			await this.incr(statusKey +'ready', this.logStatusTimeout);
		}
		return await this.onSuccess(queueName, task);
	}

	/** @hidden */
	private async error(err: any, queueName: string, task: Task): Promise<Task> {
		const taskP: any = { ...task };

		if (!(taskP.attempt >= (this.retryCount - 1))) {
			await this.onRetry(err, queueName, taskP);
			await this.addTasks(queueName, [{ ...taskP.payload, idemKey: taskP.idemKey }], {
				createdAt: taskP.createdAt,
				job: taskP.job,
				attempt: (taskP.attempt || 0) + 1,
			});
		}
		else if (this.logStatus) {
			const dlqKey = queueName +':dlq';
			const statusKey = `${queueName}:${taskP.job}:`;

			await this.incr(statusKey +'err', this.logStatusTimeout);
			await this.incr(statusKey +'ready', this.logStatusTimeout);
			await this.addTasks(dlqKey, [{ ...taskP.payload, idemKey: taskP.idemKey }], {
				createdAt: taskP.createdAt,
				job: taskP.job,
				attempt: taskP.attempt,
			});
		}
		return await this.onError(err, queueName, { ...taskP, attempt: (taskP.attempt || 0) + 1 });
	}

	/** @hidden */
	private async waitAbortable(ttl: number) {
		return new Promise<void>((resolve) => {
			const signal = this.signal();

			if (signal?.aborted) {
				return resolve();
			}
			let delay: number;

			if (ttl > 0) {
				const base = Math.max(25, Math.min(ttl, 4000));
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

	/** @hidden */
	private async sendHeartbeat(keys: IdempotencyKeys): Promise<boolean> {
		try {
			const r1 = await (this.redis as any).pexpire(keys.lockKey, this.idemLockTimeout);
			const r2 = await (this.redis as any).pexpire(keys.startKey, this.idemLockTimeout);
			const ok1 = Number(r1 || 0) === 1;
			const ok2 = Number(r2 || 0) === 1;

			return ok1 || ok2;
		}
		catch {
			return false;
		}
	}

	/** @hidden */
	private heartbeat(keys: IdempotencyKeys) {
		if (this.idemLockTimeout <= 0) {
			return;
		}
		const workerHeartbeatTimeoutMs = Math.max(1000, Math.floor(Math.max(5000, this.idemLockTimeout | 0) / 4));
		let timer: any,
			alive = true,
			hbFails = 0;

		const stop = () => {
			alive = false;
			
			if (timer) {
				clearTimeout(timer);
			}
		};
		const tick = async () => {
			if (!alive) {
				return;
			}
			if (this.signal()?.aborted) {
				stop();
				return;
			}
			try {
				const ok = await this.sendHeartbeat(keys);
				
				hbFails = ok 
					? 0 
					: (hbFails + 1);
				
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

		return () => stop();
	}

	/** @hidden */
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
	 * Load Lua scripts into Redis and cache their SHA1 hashes in memory.
	 *
	 * Redis executes Lua scripts efficiently when they are loaded once and then referenced
	 * by their SHA1 hash using `EVALSHA`. This method preloads the scripts used by this queue
	 * implementation so that later calls (via {@link runScript}) are fast and reliable.
	 *
	 * ---
	 *
	 * ## Why do we need this?
	 * When you run a script in Redis, you have two main options:
	 *
	 * - **EVAL**: send the full Lua source code every time (slow + more network traffic).
	 * - **SCRIPT LOAD** + **EVALSHA**: upload the script once, then call it by SHA (fast).
	 *
	 * This class uses the second approach:
	 * - {@link loadScripts} uploads scripts using Redis `SCRIPT LOAD`
	 * - {@link runScript} executes them using `EVALSHA`
	 *
	 * If Redis is restarted, its script cache is cleared. In that case,
	 * `EVALSHA` fails with `NOSCRIPT`, and {@link runScript} will automatically reload the script.
	 * Still, preloading on startup is best practice because it reduces first-call latency.
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param full - Controls which scripts are loaded:
	 *
	 * - `false` (default): load only the minimal scripts needed to **push tasks** into the queue.
	 *   Currently it loads:
	 *   - `XAddBulk` — a bulk `XADD` script used by {@link addTasks}.
	 *
	 * - `true`: load **all scripts** required for full worker operation (consume + ack + idempotency).
	 *   It loads:
	 *   - `XAddBulk` — bulk producer script
	 *   - `Approve` — acknowledge / optionally remove executed messages
	 *   - `IdempotencyAllow` — idempotency gate (checks done/lock/start state)
	 *   - `IdempotencyStart` — acquire idempotency lock and mark start
	 *   - `IdempotencyDone` — mark done + cleanup lock/start
	 *   - `IdempotencyFree` — free lock/start on failure
	 *   - `SelectStuck` — select/reclaim stuck pending messages
	 *
	 * ---
	 *
	 * ## When should you call it?
	 * - If you only use this class as a **producer** (only {@link addTasks}),
	 *   calling `loadScripts(false)` is enough.
	 *
	 * - If you run a **worker** ({@link runQueue}/{@link consumerLoop}),
	 *   call `loadScripts(true)` once at startup.
	 *
	 * Example:
	 * ```ts
	 * const q = new PowerQueues();
	 * q.redis = redisClient;
	 *
	 * // For worker mode:
	 * await q.loadScripts(true);
	 * await q.runQueue('alerts', '$');
	 * ```
	 *
	 * ---
	 *
	 * ## What this method does internally
	 * 1) Builds a list of `[scriptName, luaCode]` pairs based on `full`
	 * 2) For each script:
	 *    - stores the Lua source code in `this.scripts` via {@link saveScript}
	 *    - uploads it to Redis via {@link loadScript} (SCRIPT LOAD)
	 *
	 * Note: `loadScript(this.saveScript(...))` looks a little unusual, but it is intentional:
	 * - `saveScript` registers the script locally (so {@link runScript} knows it exists)
	 * - `loadScript` actually loads it into Redis and returns its SHA1
	 *   (the SHA1 returned by `loadScript` is not stored here directly; {@link runScript}
	 *   will store it lazily in `codeReady` when needed).
	 *
	 * @returns Resolves when all requested scripts have been uploaded to Redis successfully.
	 *
	 * @throws If Redis `SCRIPT LOAD` fails after retries inside {@link loadScript}.
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

	/** @hidden */
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

	/** @hidden */
	private saveScript(name: string, codeBody: string): string {
		if (!isStrFilled(codeBody)) {
			throw new Error('Script body is empty.');
		}
		this.scripts[name] = { codeBody };

		return codeBody;
	}

	/**
	 * Add (enqueue) multiple tasks into a queue (Redis Stream) in an efficient, batched way.
	 *
	 * This is the **producer** method of the queue:
	 * you provide an array of task payload objects, and the method appends them to the Redis Stream.
	 *
	 * Internally it uses the Lua script `XAddBulk` to insert many entries with one round-trip per batch,
	 * which is significantly faster than calling `XADD` for each task individually.
	 *
	 * ---
	 *
	 * ## What is written into Redis?
	 * Each task becomes a Redis Stream entry with fields like:
	 * - `payload`   — JSON string (task payload)
	 * - `createdAt` — timestamp (ms)
	 * - `job`       — job identifier (string)
	 * - `idemKey`   — idempotency key (string)
	 * - `attempt`   — retry attempt number (integer)
	 *
	 * These fields are later read by {@link select} / {@link selectP} and converted back into:
	 * `[ id, payloadObject, createdAt, job, idemKey, attempt ]`.
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param queueName - Redis Stream key (queue name) to append tasks to.
	 * Example: `"alerts"`, `"emails"`.
	 *
	 * @param data - Array of payload objects (tasks) to enqueue.
	 * Each item is expected to be a plain object that will be JSON-stringified and stored in the stream.
	 *
	 * Important:
	 * - The method does **not** validate your payload shape.
	 * - Payload objects should be JSON-serializable.
	 *
	 * @param opts - Additional enqueue options that affect metadata and trimming behavior.
	 *
	 * Common options:
	 * - `job`: Job name for this batch. If not provided, a random UUID is generated.
	 * - `attempt`: Retry attempt number to store (default `0`).
	 * - `createdAt`: Timestamp (ms) to store (default `Date.now()` per task).
	 * - `idemKey`: Default idempotency key if tasks don’t include their own `idemKey`.
	 * - `maxlen` / `approx` / `exact`: Stream trimming options (like `XADD MAXLEN`).
	 * - `trimLimit`, `minidWindowMs`, `minidExact`: additional trimming/window controls used by the script.
	 * - `nomkstream`: if true, do not auto-create stream (depends on script behavior).
	 * - `status`: if true, write `<queueName>:<job>:total` with the number of tasks (useful for monitoring).
	 *
	 * ---
	 *
	 * ## How batching works
	 * Writing thousands of tasks in one Lua call can exceed practical Redis argument limits.
	 * So this method:
	 *
	 * 1) Splits `data` into smaller batches via {@link buildBatches}
	 *    using two limits:
	 *    - `this.buildBatchCount` (max tasks per batch)
	 *    - `this.buildBatchMaxCount` (max argv tokens for Redis call)
	 *
	 * 2) Converts each batch into a flat argv array using {@link payloadBatch}
	 *
	 * 3) Executes the Lua script `XAddBulk` for each batch:
	 *    `runScript('XAddBulk', [queueName], argv)`
	 *
	 * 4) Collects all created stream IDs in the same order as input `data`
	 *
	 * The batches are executed concurrently via a small worker pool (one runner per batch),
	 * which improves throughput when Redis latency is non-trivial.
	 *
	 * ---
	 *
	 * ## Return value
	 * @returns A promise that resolves to an array of Redis Stream entry IDs.
	 * The returned array has the **same length and order** as the input `data`.
	 *
	 * Example returned IDs:
	 * `["1700000000000-0", "1700000000001-0", ...]`
	 *
	 * ---
	 *
	 * ## Errors and validation
	 * This method throws early for obvious misuse:
	 * - if `data` is empty → `"Tasks is not filled."`
	 * - if `queueName` is empty → `"Queue name is required."`
	 *
	 * Errors from Redis / Lua execution will also be thrown (e.g. connectivity, script errors).
	 *
	 * ---
	 *
	 * ## Example
	 * ```ts
	 * await q.loadScripts(false); // enough for producer mode
	 *
	 * const ids = await q.addTasks('alerts', [
	 *   { telegramChatId: 123, text: 'Hello' },
	 *   { telegramChatId: 456, text: 'World' },
	 * ], {
	 *   job: 'notify-users',
	 *   status: true,
	 * });
	 *
	 * console.log(ids); // ["...", "..."]
	 * ```
	 */
	async addTasks(queueName: string, data: any[], opts: AddTasksOptions = {}): Promise<string[]> {
		if (!isArrFilled(data)) {
			throw new Error('Tasks is not filled.');
		}
		if (!isStrFilled(queueName)) {
			throw new Error('Queue name is required.');
		}
		opts.job = opts.job ?? uuid();

		const batches = this.buildBatches(data, opts);
		const result: string[] = new Array(data.length);
		const promises: Array<() => Promise<void>> = [];
		let cursor = 0;
		
		for (const batch of batches) {
			const start = cursor;
			const end = start + batch.length;
				
			cursor = end;
			promises.push(async () => {
				const payload = this.payloadBatch(batch, opts);
				const partIds = await this.runScript('XAddBulk', [ queueName ], payload, XAddBulk);

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
			await (this.redis as any).set(`${queueName}:${opts.job}:total`, data.length);
			await (this.redis as any).pexpire(`${queueName}:${opts.job}:total`, this.logStatusTimeout);
		}
		await Promise.all(runners);
		return result;
	}

	/** @hidden */
	private buildBatches(tasks: Task[], opts: AddTasksOptions = {}): Task[][] {
		const batches: Task[][] = [];
		let batch: Task[] = [],
			realKeysLength = 8;

		for (let task of tasks) {
			const createdAt = opts?.createdAt || Date.now();
			const { idemKey, ...taskP } = task;
			const entry: Task = { 
				payload: JSON.stringify(taskP),
				attempt: Number(opts.attempt || 0),
				job: opts.job ?? uuid(),
				idemKey: String(idemKey || uuid()),
				createdAt,
			};
			const reqKeysLength = this.keysLength(entry);
			
			if (batch.length && (batch.length >= this.buildBatchCount || realKeysLength + reqKeysLength > this.buildBatchMaxCount)) {
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

	/** @hidden */
	private keysLength(task: Task): number {
		return 2 + Object.keys(task as any).length * 2;
	}

	/** @hidden */
	private payloadBatch(data: Task[], opts: AddTasksOptions): string[] {
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
			let flat: any = [];

			if ('payload' in entry) {
				for (const [ k, v ] of Object.entries(entry)) {
					flat.push(k, v as any);
				}
			}
			else {
				throw new Error('Task must have "payload" or "flat".');
			}
			const pairs = flat.length / 2;

			if (pairs <= 0) {
				throw new Error('Task "flat" must contain at least one field/value pair.');
			}
			argv.push(String(id));
			argv.push(String(pairs));

			for (const token of flat) {
				argv.push(!isExists(token)
					? ''
					: isStrFilled(token)
						? token
						: String(token));
			}
		}
		return argv;
	}

	/**
	 * Hook that runs **right before** the batch is executed.
	 *
	 * By default this method does nothing and simply returns the incoming tasks unchanged.
	 * You can override it in a subclass to implement batch-level preprocessing such as:
	 *
	 * - filtering out tasks you don't want to execute
	 * - enriching payloads (adding computed fields, defaults, normalized values)
	 * - reordering tasks (priorities, grouping)
	 * - splitting or merging tasks (advanced use)
	 * - lightweight validation (reject obviously invalid payloads early)
	 *
	 * This hook is called by {@link consumerLoop} as part of the pipeline:
	 *
	 * `select() -> beforeExecute() -> execute() -> approve()`
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param queueName - Redis Stream key (queue name) that the tasks were read from.
	 * Useful if you run multiple queues with one worker implementation.
	 *
	 * @param tasks - Raw parsed tasks selected from Redis. Each task is a tuple:
	 * `[ id, payload, createdAt, job, idemKey, attempt ]`
	 *
	 * Where:
	 * - `id` (string) — Redis stream entry id
	 * - `payload` (any) — parsed payload object (JSON) or raw string
	 * - `createdAt` (number) — timestamp from the message fields
	 * - `job` (string) — job name/id
	 * - `idemKey` (string) — idempotency key
	 * - `attempt` (number) — retry attempt counter
	 *
	 * ---
	 *
	 * ## Return value
	 * @returns A promise that resolves to the batch of tasks that should be executed next.
	 * You may return:
	 * - the same array (no changes),
	 * - a filtered array (skip tasks),
	 * - or a modified array (changed payloads, reordered tasks, etc.).
	 *
	 * Returning an empty array means "execute nothing" for this iteration.
	 *
	 * ---
	 *
	 * ## Important notes for overrides
	 * - Keep this method **fast**: it runs for every batch.
	 * - Avoid heavy I/O here (DB calls, network calls). Prefer doing that in {@link onExecute}
	 *   so failures/retries are tracked per task.
	 * - If you throw here, the whole batch is treated as failed and will go through
	 *   {@link batchError} logic.
	 *
	 * ---
	 *
	 * ## Example override: filter invalid payloads
	 * ```ts
	 * override async beforeExecute(queueName: string, tasks: Array<[string, any, number, string, string, number]>) {
	 *   return tasks.filter(([id, payload]) => payload && typeof payload.telegramChatId === 'number');
	 * }
	 * ```
	 *
	 * ## Example override: add defaults
	 * ```ts
	 * override async beforeExecute(queueName: string, tasks: Array<[string, any, number, string, string, number]>) {
	 *   return tasks.map(([id, payload, createdAt, job, idemKey, attempt]) => {
	 *     const p = typeof payload === 'object' && payload ? payload : { value: payload };
	 *     if (!('priority' in p)) p.priority = 'normal';
	 *     return [id, p, createdAt, job, idemKey, attempt] as const;
	 *   });
	 * }
	 * ```
	 */
	async beforeExecute(queueName: string, tasks: Array<[ string, any, number, string, string, number ]>) {
		return tasks;
	}

	/**
	 * Execute a single task.
	 *
	 * This is the **main business-logic hook** of the queue system.
	 * By default it does nothing and returns the task unchanged, but in real projects
	 * you override this method to perform the actual work (send a message, call an API,
	 * write to a database, etc.).
	 *
	 * This method is called from inside {@link executeProcess}, which wraps it with:
	 * - **idempotency control** (so duplicate deliveries don't cause duplicate side effects)
	 * - **heartbeat** (keeps the idempotency lock alive for long-running tasks)
	 * - error handling and retry / DLQ logic
	 *
	 * ---
	 *
	 * ## Where it sits in the pipeline
	 * A typical worker iteration looks like this:
	 *
	 * `select() -> beforeExecute() -> execute() -> executeProcess() -> onExecute() -> approve()`
	 *
	 * And inside `executeProcess()`:
	 * 1) Check idempotency (allow/start)
	 * 2) Start heartbeat (optional)
	 * 3) Call {@link onExecute}
	 * 4) Mark idempotency as done
	 * 5) Acknowledge the message (via {@link approve})
	 *
	 * If {@link onExecute} throws, the task is treated as failed and will go through retry/DLQ rules.
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param queueName - Redis Stream key (queue name) the task came from.
	 * Useful if the same worker class processes multiple queues.
	 *
	 * @param task - The task to execute.
	 *
	 * Fields you typically use:
	 * - `task.payload` — your business payload (object/string/whatever you put in {@link addTasks})
	 * - `task.job` — job identifier (useful for grouping/metrics)
	 * - `task.attempt` — retry attempt number (0 for first try)
	 * - `task.idemKey` — idempotency key (important for exactly-once side effects)
	 * - `task.id` — Redis stream entry id (useful for logs/debugging)
	 * - `task.createdAt` — timestamp stored when the task was created
	 *
	 * ---
	 *
	 * ## Return value
	 * @returns A promise that resolves to a {@link Task}.
	 *
	 * Usually you should return the same task (possibly enriched) because:
	 * - {@link execute} collects returned tasks as "successfully processed"
	 * - and passes them to {@link approve} so they can be acknowledged in Redis.
	 *
	 * If you return the task unchanged, that's perfectly fine.
	 *
	 * ---
	 *
	 * ## How to signal failure
	 * To mark the task as failed, throw an error:
	 * ```ts
	 * throw new Error('Something went wrong');
	 * ```
	 *
	 * The queue will then:
	 * - call {@link idempotencyFree}
	 * - call {@link onRetry} + re-enqueue (if attempts remain)
	 * - or put into DLQ (if enabled and attempts exhausted)
	 * - call {@link onError}
	 *
	 * ---
	 *
	 * ## Important notes for overrides (practical advice)
	 * - Make this method **idempotent** if possible (safe to run twice).
	 *   Even with locks, real systems can still deliver duplicates due to retries/network issues.
	 * - Use `task.attempt` to implement backoff logic if needed.
	 * - Keep the method focused: do business logic here, and put logging/metrics into
	 *   {@link onSuccess}/{@link onError}/{@link onRetry} if you prefer.
	 *
	 * ---
	 *
	 * ## Example override: send Telegram message
	 * ```ts
	 * override async onExecute(queueName: string, task: Task): Promise<Task> {
	 *   const { telegramChatId, text } = task.payload;
	 *
	 *   if (!telegramChatId || !text) {
	 *     throw new Error('Invalid payload: telegramChatId and text are required');
	 *   }
	 *
	 *   await this.telegramService.sendMessage(telegramChatId, text);
	 *   return task;
	 * }
	 * ```
	 */
	async onExecute(queueName: string, task: Task): Promise<Task> {
		return task;
	}

	/**
	 * Hook that runs after a batch of tasks has been executed and the list of **successful**
	 * tasks is ready.
	 *
	 * This hook is called by {@link execute} right after all per-task executions finish
	 * (either in parallel or sequentially depending on {@link executeSync}).
	 *
	 * Important: at the moment this hook is called, the tasks are:
	 * - already processed successfully by {@link onExecute} (and idempotency marked as done),
	 * - but **not yet acknowledged** in Redis (ack happens later in {@link consumerLoop} via {@link approve}).
	 *
	 * So you can think of this hook as:
	 * **"Batch succeeded logically, but not yet ACKed."**
	 *
	 * ---
	 *
	 * ## What is in the batch?
	 * The `tasks` parameter contains only tasks that were considered successful by {@link executeProcess}.
	 * It does NOT include:
	 * - contended tasks (idempotency lock was held by another worker),
	 * - tasks that threw errors,
	 * - tasks that were skipped due to idempotency states.
	 *
	 * ---
	 *
	 * ## Why this hook exists
	 * It is useful for batch-level side effects that you want to perform once per batch, for example:
	 * - logging summary: how many tasks were processed, which job, etc.
	 * - updating batch metrics / counters in your own monitoring system
	 * - emitting application-level events ("N tasks processed")
	 * - flushing buffered data (if you accumulate results during per-task execution)
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param queueName - Redis Stream key (queue name) being consumed.
	 * Useful if one worker implementation handles multiple queues.
	 *
	 * @param tasks - Array of successfully processed tasks (instances of {@link Task}).
	 * Each task typically contains:
	 * - `id` — Redis stream entry id to be acknowledged later
	 * - `payload` — your business payload
	 * - `job`, `attempt`, `idemKey`, `createdAt` — metadata fields
	 *
	 * ---
	 *
	 * ## Return value
	 * @returns A promise that resolves when your hook logic is done.
	 * The default implementation does nothing.
	 *
	 * ---
	 *
	 * ## Important notes for overrides
	 * - Keep it **fast**: this runs for every batch.
	 * - If you throw here, the whole batch is treated as failed at the batch level
	 *   (because {@link consumerLoop} wraps the pipeline in a `try/catch`).
	 *   That means it may trigger {@link batchError} and retry logic even though tasks were executed.
	 * - Avoid heavy I/O or fragile logic here unless you're okay with batch-level failure behavior.
	 * - If you need "after ACK" behavior, place it after {@link approve} (override {@link consumerLoop})
	 *   or do it inside {@link onSuccess} per task.
	 *
	 * ---
	 *
	 * ## Example override: lightweight metrics
	 * ```ts
	 * override async onBatchReady(queueName: string, tasks: Task[]) {
	 *   if (!tasks.length) return;
	 *   const job = tasks[0]?.job;
	 *   this.logger.log(`[${queueName}] batch ok: ${tasks.length} tasks (job=${job})`);
	 * }
	 * ```
	 */
	async onBatchReady(queueName: string, tasks: Task[]) {
	}

	/**
	 * Hook that runs after a task has been processed successfully.
	 *
	 * This hook is called from {@link success}, which itself is called after:
	 * 1) {@link onExecute} finished without throwing,
	 * 2) idempotency state was marked as "done" via {@link idempotencyDone},
	 * 3) (optionally) internal success counters were updated when {@link logStatus} is enabled.
	 *
	 * Important: when this hook runs, the task is **logically successful**,
	 * but the Redis Stream entry may still not be acknowledged yet.
	 * Acknowledgment happens later in {@link consumerLoop} via {@link approve}.
	 *
	 * So you can think of {@link onSuccess} as:
	 * **"Task succeeded, ACK will happen right after the batch completes."**
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param queueName - Redis Stream key (queue name) the task came from.
	 *
	 * @param task - Successfully processed task.
	 * Typical fields:
	 * - `task.payload` — your business payload
	 * - `task.job` — job identifier
	 * - `task.attempt` — attempt number (0 means first try)
	 * - `task.idemKey` — idempotency key
	 * - `task.id` — Redis stream entry id (useful for debugging)
	 * - `task.createdAt` — creation timestamp (ms)
	 *
	 * ---
	 *
	 * ## Return value
	 * @returns A promise that resolves to a {@link Task}.
	 *
	 * The returned task object is what {@link execute} collects and passes to {@link approve}.
	 * For correct behavior you should usually return the same task (or a slightly enriched copy)
	 * and keep `task.id` intact, because {@link approve} needs it to acknowledge the message.
	 *
	 * ---
	 *
	 * ## Common use cases
	 * Override this method to implement side effects that should happen on successful processing,
	 * for example:
	 * - logging (per task)
	 * - pushing metrics (Prometheus, StatsD, etc.)
	 * - emitting events to your application
	 * - collecting additional data for debugging/auditing
	 *
	 * ---
	 *
	 * ## Important notes for overrides
	 * - Keep it **fast**: this runs for every successful task.
	 * - If you throw here, the whole batch can be treated as failed by {@link consumerLoop},
	 *   which may trigger {@link batchError} even though {@link onExecute} succeeded.
	 *   Prefer not to throw; log errors instead.
	 * - If you need to do something only after Redis ACK has happened,
	 *   do it after {@link approve} (override {@link consumerLoop}) or move logic elsewhere.
	 *
	 * ---
	 *
	 * ## Example override: per-task logging
	 * ```ts
	 * override async onSuccess(queueName: string, task: Task): Promise<Task> {
	 *   this.logger.log(`[${queueName}] ok id=${task.id} job=${task.job} attempt=${task.attempt}`);
	 *   return task;
	 * }
	 * ```
	 */
	async onSuccess(queueName: string, task: Task): Promise<Task> {
		return task;
	}

	/**
	 * Hook that runs after a task processing attempt has failed.
	 *
	 * This is the main failure callback for per-task errors.
	 * It is called from {@link error} after the queue has already applied its internal
	 * retry / DLQ logic:
	 *
	 * - If retries are still allowed (`attempt < retryCount - 1`):
	 *   1) {@link onRetry} is called
	 *   2) the task is re-enqueued into the same queue with `attempt + 1`
	 *
	 * - If retries are exhausted and {@link logStatus} is enabled:
	 *   1) error counters are incremented
	 *   2) the task is sent to the DLQ queue (`<queueName>:dlq`)
	 *
	 * After that, {@link onError} is called to let you log/notify/trace failures.
	 *
	 * ---
	 *
	 * ## Important: this hook does NOT mean "the message was ACKed"
	 * At the time {@link onError} runs:
	 * - the original Redis Stream entry is still pending until {@link consumerLoop} calls {@link approve}
	 *   (in the batch error path it tries to approve the original tasks best-effort),
	 * - but the retry/DLQ decision has already been made by the queue logic.
	 *
	 * So treat this hook as:
	 * **"Task attempt failed; queue logic handled retry/DLQ; you can react to the failure."**
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param err - The error thrown by {@link onExecute} (or by idempotency/heartbeat logic around it).
	 * It can be anything (string, Error, unknown), but usually it is an `Error`.
	 *
	 * @param queueName - Redis Stream key (queue name) the task came from.
	 *
	 * @param task - The task that failed.
	 * Note: in the current implementation, `task.attempt` passed to this hook is usually
	 * incremented (`attempt + 1`) when returning from {@link error}, so you can treat it as:
	 * - "the next attempt number" (what the retry will be),
	 * rather than "the attempt that just failed".
	 *
	 * Fields:
	 * - `task.payload` — your payload that caused the failure
	 * - `task.job` — job identifier
	 * - `task.attempt` — next attempt number (see note above)
	 * - `task.idemKey` — idempotency key
	 * - `task.id` — Redis stream entry id
	 *
	 * ---
	 *
	 * ## Return value
	 * @returns A promise that resolves to a {@link Task}.
	 *
	 * The returned value is used by {@link executeProcess} to decide whether the original
	 * stream entry should be acknowledged:
	 * - {@link execute} collects returned tasks as processed results
	 * - then {@link consumerLoop} passes them to {@link approve}
	 *
	 * In this implementation, {@link error} returns the result of {@link onError}.
	 * That means: if your override returns the task **with a valid `id`**, the system may
	 * treat it as "processable result" and attempt to acknowledge it later.
	 *
	 * Practical rule:
	 * - If you want the failed entry to be acknowledged (common, because retry was re-enqueued),
	 *   return the task unchanged (keep `id`).
	 * - If you want to leave it pending for some reason, you could return an object without `id`,
	 *   but this is advanced and can cause stuck/pending behavior.
	 *
	 * ---
	 *
	 * ## Common use cases
	 * - log the error with context (queueName, job, attempt, idemKey, payload summary)
	 * - send error notifications (Telegram, email, Sentry)
	 * - implement custom metrics for failures
	 *
	 * ---
	 *
	 * ## Important notes for overrides
	 * - Do not throw inside this hook if you can avoid it.
	 *   Throwing here can break batch flow and cause extra retries.
	 * - Avoid logging full payloads if they may contain sensitive data.
	 * - If you need different behavior for "final failure" vs "will retry",
	 *   check `task.attempt` against `this.retryCount`.
	 *
	 * ---
	 *
	 * ## Example override: Sentry + structured logging
	 * ```ts
	 * override async onError(err: any, queueName: string, task: Task): Promise<Task> {
	 *   this.logger.error(`[${queueName}] failed job=${task.job} attempt=${task.attempt} id=${task.id}`, err);
	 *   Sentry.captureException(err, { extra: { queueName, task } });
	 *   return task; // keep id so original entry can be ACKed
	 * }
	 * ```
	 */
	async onError(err: any, queueName: string, task: Task): Promise<Task> {
		return task;
	}

	/**
	 * Hook that runs when **batch-level processing** fails inside {@link consumerLoop}.
	 *
	 * A "batch-level error" means something threw while processing the batch pipeline, for example:
	 * - {@link beforeExecute} threw
	 * - {@link execute} threw (or something inside it bubbled up)
	 * - {@link approve} threw (ACK/removal script failed)
	 *
	 * When such error happens, {@link consumerLoop} calls {@link batchError}, and at the end of that
	 * method it calls this hook:
	 *
	 * `await this.onBatchError(err, queueName, tasks)`
	 *
	 * This gives you one place to react to a failed batch:
	 * - log the incident with context
	 * - export metrics
	 * - notify monitoring systems
	 * - dump debug information (carefully)
	 *
	 * ---
	 *
	 * ## Important: retry/DLQ logic is NOT done here
	 * The core retry logic for batch errors lives in {@link batchError} itself:
	 * - It groups failed tasks by `(attempt, createdAt, job)`
	 * - Re-enqueues tasks if `attempt < retryCount - 1`
	 * - Otherwise (optionally) moves them to DLQ when {@link logStatus} is enabled
	 *
	 * This hook is called **after** that internal logic has been attempted.
	 * So treat it as:
	 * **"Batch failed; queue tried to handle it; you can observe/log/react."**
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param err - The error that caused the batch failure.
	 * Usually an `Error`, but can be anything (`unknown`).
	 *
	 * @param queueName - Redis Stream key (queue name) being consumed.
	 *
	 * @param tasks - The raw selected tasks that formed the batch at the time of failure.
	 * Each item is a tuple:
	 * `[ id, payload, createdAt, job, idemKey, attempt ]`
	 *
	 * Notes:
	 * - This array may include tasks that were partially processed before the failure happened.
	 * - `payload` is already parsed (JSON -> object) by {@link selectP} when possible.
	 *
	 * ---
	 *
	 * ## Return value
	 * @returns A promise that resolves when your hook logic is done.
	 * Default implementation does nothing.
	 *
	 * ---
	 *
	 * ## Common use cases
	 * - Write one log line with batch size + job info
	 * - Emit a single alert for "worker is unstable" (instead of per-task alerts)
	 * - Track "batch failures" metric
	 *
	 * ---
	 *
	 * ## Important notes for overrides
	 * - Be careful not to throw here. A throw inside this hook will be swallowed by
	 *   {@link batchError} (it already wraps it in a try/catch), but it may hide your real issue.
	 * - Avoid heavy I/O; this hook can be triggered during error storms.
	 * - Avoid logging entire payloads if they may contain secrets or personal data.
	 *
	 * ---
	 *
	 * ## Example override: log batch summary
	 * ```ts
	 * override async onBatchError(err: any, queueName: string, tasks: Array<[string, any, number, string, string, number]>) {
	 *   const size = tasks.length;
	 *   const job = size ? String(tasks[0][3]) : 'unknown';
	 *   this.logger.error(`[${queueName}] batch error size=${size} job=${job}`, err);
	 * }
	 * ```
	 */
	async onBatchError(err: any, queueName: string, tasks: Array<[ string, any, number, string, string, number ]>) {
	}

	/**
	 * Hook that runs right before a failed task is re-enqueued for another attempt.
	 *
	 * This hook is called from {@link error} only when the queue decides that the task
	 * **should be retried**:
	 *
	 * Condition in your code:
	 * `if (!(task.attempt >= (this.retryCount - 1))) { ... retry ... }`
	 *
	 * That means:
	 * - the current attempt failed,
	 * - and there are still retry attempts remaining.
	 *
	 * The hook is executed *before* the task is added back into the queue with `attempt + 1`.
	 *
	 * ---
	 *
	 * ## When exactly it happens in the flow
	 * Failure flow inside {@link executeProcess}:
	 *
	 * 1) {@link onExecute} throws
	 * 2) queue frees idempotency lock via {@link idempotencyFree}
	 * 3) {@link error} decides retry vs DLQ
	 * 4) if retry:
	 *    - call {@link onRetry} (this hook)
	 *    - call {@link addTasks} with the same payload and `attempt + 1`
	 * 5) finally call {@link onError} (after retry/DLQ decision and actions)
	 *
	 * So {@link onRetry} is a great place to:
	 * - log "retry scheduled"
	 * - apply external backoff metadata
	 * - send a lightweight notification (or metrics)
	 *
	 * ---
	 *
	 * ## Parameters
	 * @param err - The error that caused the failure (thrown by {@link onExecute} or wrapper logic).
	 *
	 * @param queueName - Redis Stream key (queue name) the task belongs to.
	 *
	 * @param task - The task that failed and is going to be retried.
	 *
	 * Important note about `attempt`:
	 * - The `task.attempt` here is the **attempt that just failed**.
	 * - The queue will re-enqueue the task with `attempt + 1`.
	 *
	 * Fields you may use:
	 * - `task.payload` — original payload
	 * - `task.job` — job identifier
	 * - `task.attempt` — current failed attempt
	 * - `task.idemKey` — idempotency key
	 * - `task.id` — Redis stream entry id (original message id)
	 *
	 * ---
	 *
	 * ## Return value
	 * @returns A promise that resolves when your hook logic is done.
	 * The default implementation does nothing.
	 *
	 * ---
	 *
	 * ## Common use cases
	 * - Structured logging for retries
	 * - Retry metrics: increment counters by job or queue name
	 * - Add custom backoff hints into your payload (advanced; but remember the payload
	 *   that gets re-enqueued is `{ ...task.payload }` from {@link error})
	 *
	 * ---
	 *
	 * ## Important notes for overrides
	 * - Avoid throwing here. If this hook throws, the retry will still likely happen,
	 *   but you may lose observability and complicate error storms.
	 * - Keep it lightweight; it can be called frequently if downstream systems are unstable.
	 * - Do not rely on this hook to *schedule delays*. This implementation re-enqueues immediately.
	 *   If you need delayed retries, you typically implement separate "delay queues" or
	 *   store `runAt` and skip until time inside {@link beforeExecute} / {@link onExecute}.
	 *
	 * ---
	 *
	 * ## Example override: log retry scheduling
	 * ```ts
	 * override async onRetry(err: any, queueName: string, task: Task) {
	 *   this.logger.warn(
	 *     `[${queueName}] retry scheduled job=${task.job} attempt=${task.attempt} -> ${task.attempt + 1} id=${task.id}`,
	 *   );
	 * }
	 * ```
	 */
	async onRetry(err: any, queueName: string, task: Task) {
	}
}