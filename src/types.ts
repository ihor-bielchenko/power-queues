
export type Constructor<T = {}> = new (...args: any[]) => T;

/**
 * A Lua script definition cached by {@link PowerQueues} for fast execution in Redis.
 *
 * ### Mental model (beginner-friendly)
 * Redis can run Lua scripts with `EVAL` (send the whole script every time),
 * or faster with `EVALSHA` (send only the script hash after the script is loaded).
 *
 * This type represents **one script in two forms**:
 * 1) the **source code** (`codeBody`) — the actual Lua script text  
 * 2) the **loaded hash** (`codeReady`) — the SHA1 returned by `SCRIPT LOAD`
 *
 * `PowerQueues.runScript(...)` uses this structure like this:
 * - If the script isn't saved yet → it saves `codeBody`
 * - If it isn't loaded yet → it calls `SCRIPT LOAD` and stores the returned SHA in `codeReady`
 * - Then it calls `EVALSHA` using `codeReady`
 * - If Redis replies with `NOSCRIPT` (Redis restarted / cache flushed) → it loads again and retries
 *
 * ### Why caching matters
 * Loading scripts once and using `EVALSHA`:
 * - reduces network traffic (no need to send the full Lua source each call)
 * - improves performance under high load
 * - avoids re-parsing the script on every call
 *
 * @example
 * ```ts
 * this.scripts['XAddBulk'] = { codeBody: XAddBulk };
 * // later:
 * if (!this.scripts['XAddBulk'].codeReady) {
 *   this.scripts['XAddBulk'].codeReady = await redis.script('LOAD', this.scripts['XAddBulk'].codeBody);
 * }
 * const res = await redis.evalsha(this.scripts['XAddBulk'].codeReady, ...);
 * ```
 */
export type SavedScript = {
	/**
	 * The "ready-to-run" identifier of the script inside Redis: the SHA1 hash.
	 *
	 * ### What it is
	 * When you call `redis.script('LOAD', luaSource)`, Redis returns a SHA1 string.
	 * Example: `"e0e1f9b7d3c1..."`
	 *
	 * ### When it is present
	 * - `undefined` until the script has been loaded at least once
	 * - may become invalid if Redis restarts or script cache is flushed
	 *
	 * ### How the code handles invalidation
	 * If `EVALSHA` throws `NOSCRIPT`, the library reloads the script and updates this field.
	 *
	 * @defaultValue `undefined` (until first `SCRIPT LOAD`)
	 */
	codeReady?: string;

	/**
	 * The Lua source code of the script.
	 *
	 * ### What it is
	 * The full text of the Lua script that Redis will execute.
	 *
	 * ### Requirements
	 * - must be a non-empty string
	 * - should be valid Lua code compatible with Redis scripting environment
	 *
	 * ### Why it is always stored
	 * Even after `codeReady` is known, you still need `codeBody` because:
	 * - Redis may forget the script (restart / flush)
	 * - the library must be able to reload it automatically on `NOSCRIPT`
	 *
	 * @example
	 * ```lua
	 * -- Lua script body
	 * local key = KEYS[1]
	 * return redis.call('GET', key)
	 * ```
	 */
	codeBody: string;
};

/**
 * Options that control how {@link PowerQueues.addTasks} writes tasks into a Redis Stream
 * and how those tasks should be treated by the queue system (job grouping, retries, idempotency, trimming).
 *
 * ### Mental model (for beginners)
 * `addTasks()` ultimately appends entries to a Redis Stream (via XADD, here wrapped by the `XAddBulk` Lua script).
 * These options let you:
 * - decide **how the stream is created / trimmed**
 * - decide **how IDs are generated**
 * - attach **metadata** to tasks (job name, attempt number, timestamps)
 * - optionally enable **status counters** for monitoring
 * - optionally influence **idempotency** (deduplication) key used by workers
 *
 * ### Common usage patterns
 * - **Default behavior**: call `addTasks(queue, tasks)` without options — it will choose sane defaults.
 * - **Group tasks under one job**: pass `{ job: 'import:2026-02-18' }`
 * - **Retry batch with attempt+1**: pass `{ attempt: prevAttempt + 1 }`
 * - **Keep stream small**: pass `{ maxlen: 100_000, approx: true }`
 *
 * @remarks
 * Redis XADD trimming has two modes:
 * - **Approximate trimming** (faster, less exact length guarantees): `approx = true`
 * - **Exact trimming** (slower, stricter length): `exact = true`
 *
 * In this implementation:
 * - If `exact` is `true`, then `approx` is forced to `false`.
 * - If `approx` is not provided, it defaults to `true` (unless `exact` is `true`).
 */
export type AddTasksOptions = {
	/**
	 * Do not create the stream automatically if it doesn't exist.
	 *
	 * ### What it does
	 * Controls whether `XADD ... MKSTREAM` is allowed.
	 *
	 * - `false` (default): if the stream key does not exist, Redis will create it automatically.
	 * - `true`: do **not** auto-create; writing to a missing stream may fail depending on the script/command behavior.
	 *
	 * ### When to use
	 * - You want stricter infrastructure control and prefer to create streams explicitly during provisioning.
	 * - You want to detect configuration errors early (e.g., wrong queue name).
	 *
	 * @defaultValue `false`
	 */
	nomkstream?: boolean;

	/**
	 * Maximum length of the Redis Stream after adding entries (stream trimming).
	 *
	 * ### What it does
	 * Enables trimming so the stream does not grow forever.
	 * Internally passed to the Lua `XAddBulk` implementation and typically maps to Redis `MAXLEN`.
	 *
	 * - `0` or not set: do not trim by length
	 * - `N > 0`: keep stream length around `N` entries (exactness depends on {@link approx}/{@link exact})
	 *
	 * ### Important
	 * Trimming can delete older entries. If you rely on long retention, do not set `maxlen` too small.
	 *
	 * @defaultValue `0` (no MAXLEN trimming)
	 * @example
	 * ```ts
	 * await q.addTasks('alerts', tasks, { maxlen: 100_000, approx: true });
	 * ```
	 */
	maxlen?: number;

	/**
	 * Sliding window (in milliseconds) used to build more deterministic stream IDs (MINID strategy).
	 *
	 * ### What it does
	 * Helps to generate or constrain entry IDs based on time windows, so IDs increase predictably.
	 * Useful when many producers write concurrently and you want IDs to be time-bucketed.
	 *
	 * This is an advanced option and depends on how `XAddBulk` script uses it.
	 *
	 * @defaultValue `0` (disabled)
	 */
	minidWindowMs?: number;

	/**
	 * Whether MINID calculations should be exact (strict) rather than relaxed/heuristic.
	 *
	 * ### What it does
	 * If your `XAddBulk` script supports a "strict" mode for MINID, enabling this makes it
	 * less tolerant to edge cases, but more deterministic.
	 *
	 * @defaultValue `false`
	 */
	minidExact?: boolean;

	/**
	 * Enable approximate trimming mode.
	 *
	 * ### What it does
	 * Equivalent to Redis `MAXLEN ~ N` in spirit:
	 * - faster
	 * - does not guarantee exact stream size after trimming
	 *
	 * ### Rules in this implementation
	 * - If {@link exact} is `true` => `approx` becomes `false` automatically.
	 * - If not provided and {@link exact} is not `true` => treated as `true`.
	 *
	 * @defaultValue `true` (unless {@link exact} is `true`)
	 */
	approx?: boolean;

	/**
	 * Enable exact trimming mode.
	 *
	 * ### What it does
	 * Equivalent to Redis `MAXLEN = N` in spirit:
	 * - stricter stream length guarantee
	 * - usually slower / more work for Redis
	 *
	 * ### Notes
	 * If you set this to `true`, {@link approx} is effectively disabled.
	 *
	 * @defaultValue `false`
	 */
	exact?: boolean;

	/**
	 * A secondary trimming/cleanup limit used by the Lua script (`trimLimit`).
	 *
	 * ### What it does
	 * This is not a standard Redis XADD flag; it is passed into your `XAddBulk` script.
	 * Typically it means: "limit how many entries can be trimmed in one operation" or
	 * "apply additional trimming constraints".
	 *
	 * ### When to use
	 * - You observe spikes/latency due to heavy trimming and want to cap work per call.
	 *
	 * @defaultValue `0` (script-specific; usually means "no extra limit")
	 */
	trimLimit?: number;

	/**
	 * Job identifier assigned to all tasks in this `addTasks()` call.
	 *
	 * ### What it does
	 * Acts as a label/group name for monitoring and status keys.
	 * If not provided, a UUID is generated.
	 *
	 * ### Why it matters
	 * If {@link status} is enabled, counters are stored under:
	 * `\${queueName}:\${job}:total`
	 * (and workers may maintain per-job/per-queue status as well).
	 *
	 * @defaultValue Random UUID
	 */
	job?: string;

	/**
	 * Enable writing a "total tasks" status counter for the current job.
	 *
	 * ### What it does
	 * When `true`, `addTasks()` will store:
	 * - key: `\${queueName}:\${job}:total`
	 * - value: number of tasks added
	 * - TTL: {@link PowerQueues.logStatusTimeout}
	 *
	 * This is useful for dashboards and progress tracking.
	 *
	 * ### Notes
	 * This does **not** automatically track ok/err/ready here — those are managed elsewhere in the worker flow.
	 *
	 * @defaultValue `false`
	 */
	status?: boolean;

	/**
	 * Status TTL in milliseconds (how long status keys should live).
	 *
	 * ### Important
	 * In the current code you pasted, the TTL used for status keys is `this.logStatusTimeout`,
	 * not this field. So this option is either:
	 * - planned for future use, or
	 * - used in other parts of the codebase, or
	 * - currently unused.
	 *
	 * If you want this option to work, you would need to apply it where `pexpire(...)` is called.
	 *
	 * @defaultValue (currently unused in shown code)
	 */
	statusTimeoutMs?: number;

	/**
	 * Attempt number to assign to each enqueued task (retry counter).
	 *
	 * ### What it does
	 * Sets the `attempt` field for all tasks created in this batch.
	 * - First enqueue is usually `0`
	 * - On retry, workers typically enqueue the same payload with `attempt + 1`
	 *
	 * @defaultValue `0`
	 * @example
	 * ```ts
	 * await q.addTasks('sync', payloads, { job: 'sync:customers', attempt: 0 });
	 * // later on retry:
	 * await q.addTasks('sync', payloads, { job: 'sync:customers', attempt: 1 });
	 * ```
	 */
	attempt?: number;

	/**
	 * Timestamp (milliseconds since epoch) to assign as `createdAt` for each task in this batch.
	 *
	 * ### What it does
	 * If not provided, each task gets `Date.now()` at the time of batching.
	 * Useful if you want tasks to preserve an original creation time across retries/requeues.
	 *
	 * @defaultValue `Date.now()`
	 */
	createdAt?: number;
};

export type IdempotencyKeys = {
	prefix: string; 
	doneKey: string; 
	lockKey: string; 
	startKey: string; 
	token: string;
};

/**
 * A single unit of work processed by {@link PowerQueues}.
 *
 * ### Mental model (beginner-friendly)
 * Think of a **Task** as a small “message” that goes into a queue.
 * A worker (consumer) reads the message, runs some code (your handler), and then acknowledges it.
 *
 * In this library, tasks are stored in a **Redis Stream** entry.
 * Each stream entry contains fields like `payload`, `job`, `attempt`, `createdAt`, and `idemKey`.
 *
 * ### Lifecycle overview
 * 1. **Producer** creates a task object and calls `addTasks(queueName, [task])`
 * 2. The task is written into Redis Stream (XADD via `XAddBulk` script)
 * 3. **Consumer group** reads tasks with `XREADGROUP`
 * 4. Worker calls {@link PowerQueues.onExecute} to process it
 * 5. If successful: task is acknowledged and optionally removed from the stream
 * 6. If failed: it may be re-enqueued with a higher {@link attempt} or moved to a DLQ
 *
 * ### Important: shape depends on the context
 * There are **two "views"** of a task:
 * - **Input task** (what you pass to `addTasks`) — typically contains your `payload` + optional `idemKey`
 * - **Runtime task** (what worker receives in `onExecute`) — contains `id`, timestamps, retry info, etc.
 *
 * @example
 * **Producer side (enqueue)**
 * ```ts
 * await q.addTasks('emails', [{
 *   job: 'send-welcome',         // optional, can be overwritten by AddTasksOptions.job
 *   payload: { userId: 42 },     // your data
 *   attempt: 0,                  // optional, can be overwritten by AddTasksOptions.attempt
 *   idemKey: 'email:welcome:42', // optional but recommended for idempotency
 * }]);
 * ```
 *
 * @example
 * **Consumer side (process)**
 * ```ts
 * async onExecute(queueName: string, task: Task) {
 *   // task.id exists here (Redis Stream ID)
 *   // task.payload is the object you originally sent
 *   // task.attempt tells you which retry this is
 *   return task;
 * }
 * ```
 */
export type Task = {
	/**
	 * Job name (group/label) for this task.
	 *
	 * ### What it is
	 * A string that identifies the “category” or “batch” this task belongs to.
	 * It is mainly used for:
	 * - grouping tasks for monitoring/statistics
	 * - naming status keys (`${queueName}:${job}:...`)
	 * - debugging (“which pipeline produced this task?”)
	 *
	 * ### How it is set
	 * - You can set it on the task directly
	 * - or override / assign it for the entire batch via `AddTasksOptions.job`
	 * - if not provided, this library often generates a UUID
	 *
	 * ### Beginner tip
	 * Use human-readable job names when you can, for example:
	 * - `"sync:devices"`
	 * - `"alerts:telegram"`
	 * - `"email:welcome"`
	 */
	job: string;

	/**
	 * Redis Stream entry ID for this task.
	 *
	 * ### What it is
	 * When a task is stored in a Redis Stream, Redis assigns it an ID like:
	 * `"1708250000000-0"` (millisecond timestamp + sequence number).
	 *
	 * ### When it exists
	 * - It is usually **missing** before enqueue (producer-side)
	 * - It is **present** when the worker reads the task from Redis (consumer-side)
	 *
	 * ### Why it matters
	 * This is the unique identifier of the stream entry.
	 * It is used for acknowledging (`XACK`) and deleting (`XDEL`) entries.
	 *
	 * @defaultValue `undefined` (before enqueue)
	 */
	id?: string;

	/**
	 * Task creation timestamp (milliseconds since UNIX epoch).
	 *
	 * ### What it means
	 * The time when the task was created/enqueued.
	 *
	 * ### How it is set
	 * - If you pass `AddTasksOptions.createdAt`, that value is used
	 * - Otherwise the library sets it to `Date.now()` during batching
	 *
	 * ### What it is used for
	 * - debugging and metrics (how long tasks wait in queue)
	 * - retry logic / dead-letter policies (optional, depending on your design)
	 * - “stuck task” detection in combination with pending entries
	 *
	 * @defaultValue `Date.now()` (if not provided)
	 */
	createdAt?: number;

	/**
	 * User data carried by the task (the "work request").
	 *
	 * ### What it is
	 * This is the payload your application cares about.
	 * Examples:
	 * - send an email: `{ userId: 42, template: 'welcome' }`
	 * - process alert: `{ telegramChatId: 123, message: '...' }`
	 * - sync device: `{ deviceId: 'abc', force: true }`
	 *
	 * ### Important behavior in this implementation
	 * When tasks are enqueued, the library **serializes** your data to JSON:
	 * - in `buildBatches()` it does `payload: JSON.stringify(taskP)`
	 * - later on read, it tries to `JSON.parse(...)` it back
	 *
	 * That means:
	 * - payload should be **JSON-serializable**
	 * - avoid functions, class instances, circular references, BigInt without conversion, etc.
	 *
	 * @remarks
	 * In your type it is `any`, because different jobs can carry completely different payload shapes.
	 * In real projects, it’s common to create a generic version like `Task<TPayload = unknown>`.
	 */
	payload: any;

	/**
	 * Idempotency (deduplication) key for this task.
	 *
	 * ### What it is
	 * A string that uniquely identifies the *logical operation* represented by this task.
	 *
	 * ### Why it exists
	 * In distributed systems, a task can be delivered/processed more than once:
	 * - worker crashes after processing but before acknowledge
	 * - network issues / timeouts
	 * - manual re-queue
	 *
	 * Idempotency makes retries safe by ensuring:
	 * - only one worker can “own” the task at a time (lock)
	 * - already completed tasks are not executed again (done marker)
	 *
	 * ### How it is used in this code
	 * Workers compute internal Redis keys based on `queueName` + `idemKey`:
	 * - done key
	 * - lock key
	 * - start key
	 *
	 * The scripts `IdempotencyAllow/Start/Done/Free` use these keys.
	 *
	 * ### How to choose a good key (beginner tip)
	 * It should represent the business operation:
	 * - `"email:welcome:user:42"`
	 * - `"sync:device:AA:BB:CC:DD:EE:FF"`
	 * - `"alert:telegram:chat:123:msg:999"`
	 *
	 * @defaultValue
	 * If not set on the task, `addTasks()` will use `AddTasksOptions.idemKey`,
	 * and if that is also missing, it generates a random UUID.
	 */
	idemKey?: string;

	/**
	 * Attempt number (retry counter).
	 *
	 * ### What it means
	 * How many times this logical task has been attempted.
	 * - `0` usually means "first try"
	 * - `1` means "first retry"
	 * - `2` means "second retry", etc.
	 *
	 * ### How it is used in this code
	 * - On failure, if `attempt < retryCount - 1`, the task is re-enqueued with `attempt + 1`
	 * - If retries are exhausted, it can be moved to a DLQ (dead-letter queue) if enabled
	 *
	 * ### Beginner tip
	 * You can use `attempt` to implement backoff logic or change behavior on retries.
	 *
	 * @defaultValue `0`
	 */
	attempt: number;
};
