import type { JsonPrimitiveOrUndefined } from 'power-redis';

/**
 * A simple generic type representing a class constructor.
 *
 * `Constructor<T>` describes any class that can be instantiated using `new`,
 * producing a value of type `T`. It is commonly used for mixins, base-class
 * composition patterns, dependency injection helpers, and factory utilities.
 *
 * ### What it represents
 * This type enforces the shape of:
 *
 * ```ts
 * new (...args: any[]) => T
 * ```
 *
 * Meaning:
 * - The class can be created with any number of arguments.
 * - It will always return an instance that satisfies the type `T`.
 *
 * ### When to use it
 * Use `Constructor<T>` when:
 * - You are writing a mixin that accepts a class and returns an extended class.
 * - You need to constrain a function to accept only constructible classes.
 * - You want strong typing for factories that dynamically instantiate classes.
 *
 * ### Example: Mixin usage
 * ```ts
 * function Timestamped<TBase extends Constructor>(Base: TBase) {
 *   return class extends Base {
 *     createdAt = Date.now();
 *   };
 * }
 *
 * class BaseClass {}
 * const Extended = Timestamped(BaseClass);
 * const instance = new Extended(); // instance.createdAt → number
 * ```
 *
 * ### Example: Factory function
 * ```ts
 * function makeInstance<T>(Cls: Constructor<T>): T {
 *   return new Cls();
 * }
 *
 * class User { name = 'John'; }
 * const user = makeInstance(User); // User instance
 * ```
 *
 * @typeParam T - The instance type produced by the constructor.
 */
export type Constructor<T = {}> = new (...args: any[]) => T;

/**
 * Represents a Lua script registered within Redis and managed by the queue system.
 *
 * `SavedScript` is used internally by {@link PowerQueues} to store both:
 * - the **raw script body** (the original source code), and
 * - the **compiled SHA1 hash** returned by Redis via `SCRIPT LOAD`.
 *
 * Redis identifies loaded scripts by SHA1 rather than by source, so keeping both
 * values allows the system to:
 *
 * - reload scripts if Redis reports `NOSCRIPT`,
 * - avoid re-sending large Lua source on every execution,
 * - transparently recover after Redis restart, eviction, or failover,
 * - ensure consistent and efficient `EVALSHA` usage.
 *
 * ### Fields
 * - `codeBody`  
 *   The full Lua script text as a string.  
 *   This is always present and is what gets sent to Redis when loading the script.
 *
 * - `codeReady`  
 *   The SHA1 hash returned by `SCRIPT LOAD`.  
 *   It is filled automatically on first load and reused for all subsequent calls.
 *   If Redis forgets the script, the system loads it again and updates this value.
 *
 * ### Example (internal usage)
 * ```ts
 * // When saving:
 * scripts["XAddBulk"] = {
 *   codeBody: XAddBulk,
 *   codeReady: undefined
 * };
 *
 * // After loading via SCRIPT LOAD:
 * scripts["XAddBulk"].codeReady = "1ab2c3d4e5...";
 *
 * // During eval:
 * redis.evalsha(codeReady, ...)
 * ```
 *
 * @property codeReady - SHA1 digest assigned by Redis after loading the script. Optional.
 * @property codeBody - The original Lua script source code. Required.
 */
export type SavedScript = { 
	codeReady?: string; 
	codeBody: string; 
};

/**
 * Fine-grained options controlling how tasks are added to a Redis Stream
 * using {@link PowerQueues.addTasks | addTasks}.
 *
 * `AddTasksOptions` allows you to tune trimming behavior, idempotency, batching
 * and basic progress-tracking for bulk task insertion.
 *
 * Every option is optional - defaults are designed to be safe and predictable.
 *
 * ---
 * ## Stream trimming & retention
 *
 * @property maxlen  
 * The maximum allowed length of the stream.  
 * When set, Redis may trim older messages using `MAXLEN` rules.
 *
 * @property approx  
 * Whether to use **approximate trimming** (`~`) instead of exact (`=`).  
 * Approximate trimming is faster and recommended for most queues.
 * Defaults to `true` unless `exact` is explicitly set.
 *
 * @property exact  
 * Forces **exact trimming** (`=`), which makes Redis strictly enforce `maxlen`.  
 * Slower and rarely needed; use only for strict historic retention rules.
 *
 * @property nomkstream  
 * Prevents Redis from creating the stream automatically when empty.  
 * Useful when you want predictable errors instead of auto-creation.
 *
 * @property trimLimit  
 * Redis `LIMIT` parameter for trimming - limits how many entries are removed
 * per XADD operation. Helps avoid latency spikes when maxlen is low and stream is large.
 *
 * ---
 * ## Time-based trimming (MINID)
 *
 * @property minidWindowMs  
 * Enables **MINID-based trimming**: automatically removes entries older than  
 * `now - minidWindowMs`.  
 * Only used if `minidWindowMs > 0`.
 *
 * @property minidExact  
 * Use exact (`=`) or approximate (`~`) MINID trimming.  
 * Approximate is faster; exact guarantees strict cutoff.
 *
 * ---
 * ## Idempotency mode
 *
 * @property idem  
 * Whether to attach an idempotency key to each task when using object payloads.
 * When enabled:
 * - the worker ensures each key is processed at most once,
 * - retries or duplicates with the same key are skipped automatically.
 *
 * ---
 * ## Status tracking (lightweight job progress)
 *
 * @property status  
 * Enables per-job total counter:  
 * `queueName:<jobId>:total = <number of tasks>`
 *
 * @property statusTimeoutMs  
 * TTL (in milliseconds) for all `status` keys.  
 * Defaults to `300000` (5 minutes).
 *
 * ---
 * ## Task ID overwrite
 *
 * @property id  
 * Optional entry ID for all tasks in this batch.  
 * When not set, Redis generates IDs automatically (recommended).
 *
 * ---
 * ## Summary
 * Use this type to control:
 * - how tasks are inserted,
 * - how the stream is trimmed,
 * - whether idempotency should be applied,
 * - and whether basic job-level statistics should be tracked.
 *
 * @example
 * ```ts
 * await queue.addTasks('queue:emails', data, {
 *   maxlen: 200000,
 *   approx: true,
 *   minidWindowMs: 300000, // keep only last 5 minutes
 *   idem: true,
 *   status: true,
 * });
 * ```
 */
export type AddTasksOptions = {
	nomkstream?: boolean;
	maxlen?: number;
	minidWindowMs?: number;
	minidExact?: boolean;
	approx?: boolean;
	exact?: boolean;
	trimLimit?: number;
	id?: string;
	status?: boolean;
	statusTimeoutMs?: number;
	idem?: boolean;
};

/**
 * A structured collection of Redis keys used to implement safe idempotent
 * task execution inside {@link PowerQueues}.
 *
 * When a task includes an `idemKey`, the queue ensures that **only one worker**
 * processes that task - even if duplicates, retries, or race conditions occur.
 *
 * `IdempotencyKeys` contains the 3 keys needed for this coordination:
 *
 * ---
 * ## Keys
 *
 * @property prefix  
 * Common prefix (`q:<stream>:`) applied to all idempotency keys for a queue.  
 * Helps keep keys grouped and easily discoverable in Redis.
 *
 * @property doneKey  
 * Indicates that the task has been successfully processed at least once.  
 * - Set when the task finishes.  
 * - Has a short TTL (`workerCacheTaskTimeoutMs`).  
 * - When present, duplicate tasks are **instantly skipped**.
 *
 * @property lockKey  
 * A temporary lock used to ensure **only one worker** is allowed to start
 * executing a task with this idempotency key.  
 * Contains a random token to prevent other workers from stealing the lock.
 *
 * @property startKey  
 * Marks that a worker has begun processing this task.  
 * Used for detecting contention and calculating wait times for competing workers.
 *
 * @property token  
 * A unique value (per worker, per execution) used to:
 * - claim the lock uniquely,  
 * - validate ownership before freeing or completing the task,  
 * - prevent accidental unlocks or interference by other workers.
 *
 * ---
 * ## How these keys work together
 *
 * 1. **IdempotencyAllow** checks:
 *    - if `doneKey` exists → skip (task already processed);
 *    - otherwise tries to set `lockKey` with the worker’s `token`.
 *
 * 2. **IdempotencyStart** re-validates ownership and extends TTLs.
 *
 * 3. **IdempotencyDone** sets `doneKey` and clears locks.
 *
 * 4. **IdempotencyFree** is used to safely release locks when execution fails.
 *
 * Together, they provide strong guarantees that a task:
 * - is never processed twice,
 * - cannot be stolen by another worker mid-execution,
 * - becomes retryable after TTL expires if processing never fully completed.
 *
 * ---
 * @example
 * ```ts
 * const keys = queue.idempotencyKeys('upload:123');
 *
 * // {
 * //   prefix: "q:queue_name:",
 * //   doneKey:  "q:queue_name:done:upload_123",
 * //   lockKey:  "q:queue_name:lock:upload_123",
 * //   startKey: "q:queue_name:start:upload_123",
 * //   token: "host:12345:abcde"
 * // }
 * ```
 */
export type IdempotencyKeys = {
	prefix: string; 
	doneKey: string; 
	lockKey: string; 
	startKey: string; 
	token: string;
};

/**
 * Represents a single task that can be pushed into a queue via
 * {@link PowerQueues.addTasks | addTasks}.  
 * A task defines **what job should be executed** and **what data it carries**.
 *
 * A `Task` can be represented in **two formats** depending on how payload
 * should be encoded:
 *
 * ---
 * ## 1. Object payload form
 *
 * ```ts
 * {
 *   job: string;
 *   id?: string;
 *   createdAt?: number;
 *   payload: any;
 *   idemKey?: string;
 * }
 * ```
 *
 * - `payload` is a normal JavaScript object.
 * - Values are serialized into the Redis Stream entry automatically.
 * - Additional metadata (`createdAt`, `job`, and `idemKey`) is injected during batching.
 *
 * Use this format whenever your task data is naturally an object.
 *
 * ---
 * ## 2. Flat array form
 *
 * ```ts
 * {
 *   job: string;
 *   id?: string;
 *   createdAt?: number;
 *   flat: JsonPrimitiveOrUndefined[];
 *   idemKey?: string;
 * }
 * ```
 *
 * - `flat` represents a pre-flattened list of alternating `field, value`
 *   pairs:  
 *   `["field1", "value1", "field2", "value2", ...]`
 * - Used when you want **full control** over Stream field/value structure
 *   or want to skip object→string serialization overhead.
 * - Must contain an **even number of elements**.
 *
 * ---
 * ## Field Descriptions
 *
 * @property job  
 * A logical identifier representing the type or category of the task.
 * Used to group related operations and track per-job metrics.
 *
 * @property id  
 * Optional explicit Redis Stream ID.  
 * If omitted, Redis auto-generates an ID (recommended in most cases).
 *
 * @property createdAt  
 * Optional timestamp of task creation.  
 * If omitted, the system fills this automatically.
 *
 * @property payload  
 * A structured object containing arbitrary task data.  
 * Mutually exclusive with `flat`.
 *
 * @property flat  
 * A pre-flattened array of field/value pairs.  
 * Mutually exclusive with `payload`.
 *
 * @property idemKey  
 * Optional idempotency key.  
 * If present, the queue guarantees **at most once** execution across workers.
 *
 * ---
 * ## How the system augments tasks internally
 *
 * During batching, the queue automatically injects:
 * - `createdAt`  
 * - `job`  
 * - optional `idemKey`
 *
 * This ensures every task stored in Redis carries enough metadata to be
 * routed, deduplicated, retried, logged, or reconstructed.
 *
 * ---
 * @example
 * **Object payload**
 * ```ts
 * const task: Task = {
 *   job: 'email',
 *   payload: { to: 'user@example.com', subject: 'Hello' }
 * };
 * ```
 *
 * @example
 * **Flat payload**
 * ```ts
 * const task: Task = {
 *   job: 'metrics',
 *   flat: ['cpu', 34.1, 'ram', 4120]
 * };
 * ```
 */
export type Task =
	| { job: string; id?: string; createdAt?: number; payload: any; idemKey?: string; }
	| { job: string; id?: string; createdAt?: number; flat: JsonPrimitiveOrUndefined[]; idemKey?: string; };
