import type { 
	Task,
	TaskResult, 
	TaskChain,
	TaskProgress,
} from './types';
import { v4 as uuid } from 'uuid';
import {
	isStrFilled,
	isStr,
	isArrFilled,
	isArr,
	isObjFilled,
	isNumPZ,
	isNumP,
	isFunc,
	wait,
} from 'full-utils';
import { PowerRedis } from 'power-redis';

/**
 * @category Core
 * @summary
 * Abstract **Redis queue runner** with reservation, visibility timeout, retries, delayed scheduling,
 * chain forwarding and lifecycle hooks - extend and implement {@link execute}.
 *
 * @remarks
 * ### Key design points
 * - **Data structures**
 *   - Ready queue: Redis **LIST** (RPUSH producers, LMOVE/RPOPLPUSH consumers).
 *   - Processing queue: Redis **LIST** (RIGHT push of reserved items).
 *   - Visibility timeouts: Redis **ZSET** (member = raw task string; score = deadline in seconds).
 *   - Delayed tasks: Redis **ZSET** (score = availableAt seconds).
 * - **Reservation**
 *   - Prefers `LMOVE LEFT RIGHT` (Redis ≥ 6.2) with a Lua loop for batch pulls.
 *   - Falls back to `RPOPLPUSH` if LMOVE is unavailable.
 * - **Heartbeat**
 *   - Periodically extends ZSET score before the deadline to keep the item invisible.
 * - **Retries**
 *   - Exponential backoff with jitter, capped by {@link retryMaxSec}.
 * - **Chain**
 *   - On success, automatically enqueues to the next queue in {@link TaskChain.queues}.
 *
 * Extend this class and override the virtual methods as needed:
 * {@link execute} (required), and optionally {@link beforeExecution}, {@link afterExecution},
 * {@link onRetry}, {@link onError}, {@link onFail}, {@link onFatal}, {@link onSuccess},
 * {@link onChainSuccess}, {@link beforeIterationExecution}, {@link afterIterationExecution},
 * {@link onIterationError}.
 *
 * @example
 * class ImageResizer extends PowerQueue {
 *   async execute(task: Task): Promise<TaskResult> {
 *     // your business logic
 *     return { resized: true, id: task.id };
 *   }
 * }
 *
 * const worker = new ImageResizer(redisClient);
 * worker.run('img:resize');
 *
 * @since 2.0.0
 */
export abstract class PowerQueue extends PowerRedis {
	private reserveSha?: string;
	private reserveShaRpoplpush?: string;
	private requeueSha?: string;
	private promoteSha?: string;
	public readonly iterationTimeout: number = 1000;
	public readonly portionLength: number = 1000;
	public readonly expireStatusSec: number = 300;
	public readonly maxAttempts: number = 1;
	public readonly concurrency: number = 32;
	public readonly visibilityTimeoutSec: number = 60;
	public readonly retryBaseSec: number = 1;
	public readonly retryMaxSec: number = 3600;
	private runners = new Map<string, { running: boolean }>();
	private processingRaw = new Map<string, string>();
	private heartbeatTimers = new Map<string, ReturnType<typeof setInterval>>();

	/**
	 * @summary
	 * Returns the **current UNIX timestamp** in **seconds** (integer precision).
	 *
	 * @remarks
	 * This utility method provides a standardized way to obtain the current
	 * time in UNIX format - the number of seconds elapsed since
	 * **January 1, 1970 UTC**.
	 *
	 * Many Redis operations and Lua scripts in `PowerQueue` use timestamps in
	 * seconds (not milliseconds), for example:
	 * - Visibility timeouts in {@link processingVtKey} ZSETs.
	 * - Deadlines passed to Lua scripts (e.g. {@link getReserveScriptLMOVE} / {@link getRequeueScript}).
	 * - Delayed job scheduling in {@link enqueue} and {@link promoteDelayed}.
	 *
	 * Using seconds instead of milliseconds ensures compatibility with Redis’
	 * integer-based time arithmetic, simplifies ZSET comparisons (`ZRANGEBYSCORE`),
	 * and keeps all internal time calculations consistent across the queue system.
	 *
	 * ### Why not use milliseconds?
	 * - Redis commands such as `EXPIRE`, `ZADD`, and `ZRANGEBYSCORE` typically
	 *   operate in seconds-based timestamps.  
	 * - Avoids accidental rounding issues and unnecessary precision for queue timing.  
	 * - Keeps Lua scripts simpler and faster by working with integer values.
	 *
	 * @returns
	 * The current UNIX timestamp as an integer (e.g. `1739820842`).
	 *
	 * @example
	 * ```ts
	 * const now = this.nowSec();
	 * console.log('Current timestamp (seconds):', now);
	 * // → 1739820842
	 *
	 * // Use it to set a visibility timeout deadline
	 * const deadline = now + 60; // +60 seconds
	 * ```
	 *
	 * @since 2.0.0
	 * @category Time Utilities
	 * @see {@link enqueue} - Uses this value for delayed job scoring.
	 * @see {@link requeueExpired} - Compares against VT deadlines.
	 * @see {@link promoteDelayed} - Promotes jobs whose ZSET score ≤ `now`.
	 * @see {@link getRequeueScript} - Lua logic that relies on this timestamp format.
	 */
	private nowSec(): number {
		return Math.floor(Date.now() / 1000);
	}

	/**
	 * @summary
	 * Builds the **Redis key** for the main **ready list** of a given queue -  
	 * the list that stores all jobs waiting to be reserved and processed.
	 *
	 * @remarks
	 * The *ready* list is the entry point of the queue:  
	 * it contains serialized job payloads that are ready for immediate execution.
	 *
	 * Each queue in `PowerQueue` has exactly one such list, populated by:
	 * - {@link enqueue} - when a job is added without a delay.  
	 * - {@link promoteDelayed} - when delayed jobs become due and are promoted.  
	 * - {@link requeueExpired} - when expired processing jobs are requeued for retry.
	 *
	 * Workers reserve items from this list atomically into the `processing` list via
	 * {@link reserveMany} or the Lua-based `reserve` scripts loaded by
	 * {@link ensureReserveScript}.
	 *
	 * ### Key structure
	 * The resulting Redis key follows the pattern:
	 * ```
	 * queue:<queueName>
	 * ```
	 * Example:
	 * ```ts
	 * this.readyKey('emails');
	 * // → "queue:emails"
	 * ```
	 *
	 * ### Why this matters
	 * - Represents the **frontline** of the queue system - where new jobs wait.  
	 * - Enables fast `RPUSH` producers and atomic reservation via `LMOVE`/`RPOPLPUSH`.  
	 * - Provides a stable namespace base for all other queue keys
	 *   (`processing`, `processing:vt`, `delayed`, etc.).  
	 * - Keeps data consistently grouped under the `"queue:"` namespace.
	 *
	 * ### Relation to other keys
	 * - `queue:<name>:processing` - holds currently active jobs.  
	 * - `queue:<name>:processing:vt` - tracks their visibility deadlines.  
	 * - `queue:<name>:delayed` - holds jobs scheduled for future execution.
	 *
	 * @param queueName - The logical name of the queue for which the ready list
	 *   key should be built. Must be a valid non-empty string.
	 *
	 * @returns
	 * A Redis key string in the format `"queue:<queueName>"`.
	 *
	 * @throws
	 * If `queueName` is empty or invalid, an error will be thrown by {@link toKeyString}.
	 *
	 * @example
	 * ```ts
	 * // Example: get the ready list key for the "emails" queue
	 * const key = this.readyKey('emails');
	 * // Result: "queue:emails"
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Key Helpers
	 * @see {@link toKeyString} - Performs strict validation of key segments.
	 * @see {@link processingKey} - List for active jobs currently being handled.
	 * @see {@link delayedKey} - ZSET for scheduled (future) jobs.
	 * @see {@link reserveMany} - Consumes this list during job reservation.
	 * @see {@link enqueue} - Adds new tasks directly into this list.
	 */
	private readyKey(queueName: string): string {
		return this.toKeyString(queueName);
	}

	/**
	 * @summary
	 * Builds the **Redis key** for the list that stores jobs currently being **processed**
	 * (i.e. reserved by workers but not yet acknowledged as complete).
	 *
	 * @remarks
	 * Each queue in `PowerQueue` maintains a dedicated **processing list**
	 * that temporarily holds jobs fetched from the `ready` list via
	 * {@link reserveMany} or the internal Lua reserve script.
	 *
	 * This list acts as a transient buffer of "in-flight" jobs - tasks that are:
	 * - Currently being handled by a worker.
	 * - Still within their visibility timeout period.
	 * - Not yet acknowledged (via {@link ackProcessing}) or requeued on timeout.
	 *
	 * The associated {@link processingVtKey} ZSET keeps track of expiration timestamps
	 * (visibility deadlines) for each job stored in this list.
	 *
	 * ### Key structure
	 * The resulting Redis key follows the pattern:
	 * ```
	 * queue:<queueName>:processing
	 * ```
	 * Example:
	 * ```ts
	 * this.processingKey('emails');
	 * // → "queue:emails:processing"
	 * ```
	 *
	 * ### Where it’s used
	 * - {@link reserveMany} - Moves jobs into this list atomically during reservation.  
	 * - {@link ackProcessing} - Removes completed jobs from this list.  
	 * - {@link requeueExpired} - Removes expired jobs from here when requeuing them.  
	 * - {@link getRequeueScript} - Lua implementation of that requeue logic.  
	 * - {@link ensureReserveScript} - Loads the Lua variant that interacts with this list.
	 *
	 * ### Why this matters
	 * - Provides atomic “in-flight” job tracking per queue.  
	 * - Ensures job visibility and prevents message duplication under concurrency.  
	 * - Separates transient (processing) state from durable (ready/delayed) state.  
	 * - Keeps queue-related data consistently namespaced under `"queue:"`.
	 *
	 * @param queueName - The logical name of the queue for which the processing list
	 *   key should be built. Must be a valid non-empty string.
	 *
	 * @returns
	 * A fully qualified Redis key string in the format:
	 * `"queue:<queueName>:processing"`.
	 *
	 * @throws
	 * If `queueName` is empty or contains invalid symbols,
	 * an error will be thrown by {@link toKeyString}.
	 *
	 * @example
	 * ```ts
	 * // Example: get the processing list key for the "invoices" queue
	 * const key = this.processingKey('invoices');
	 * // Result: "queue:invoices:processing"
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Key Helpers
	 * @see {@link toKeyString} - Base key builder with strict validation.
	 * @see {@link processingVtKey} - ZSET companion tracking visibility timeouts.
	 * @see {@link requeueExpired} - Uses this list to restore expired jobs.
	 * @see {@link ackProcessing} - Removes processed jobs from this list.
	 */
	private processingKey(queueName: string): string {
		return this.toKeyString(queueName, 'processing');
	}

	/**
	 * @summary
	 * Builds the **Redis key** for the ZSET that tracks **visibility timeouts**
	 * of currently processing jobs in a given queue.
	 *
	 * @remarks
	 * This key identifies the **processing visibility-timeout ZSET** (VT set),
	 * which stores timestamps indicating when each job’s *visibility window*
	 * expires.  
	 *  
	 * Each member in this ZSET represents a serialized job that has been
	 * reserved for processing but not yet acknowledged (`ACK`ed).
	 *
	 * - The ZSET **member** is the raw task payload string.  
	 * - The ZSET **score** is the UNIX timestamp (in seconds) at which the
	 *   task’s visibility timeout expires.
	 *
	 * This key is essential for implementing **reliable message visibility**:
	 * it enables automatic requeueing of jobs that were not completed before
	 * their timeout, ensuring that no task is permanently lost.
	 *
	 * ### Key structure
	 * The resulting Redis key follows the pattern:
	 * ```
	 * queue:<queueName>:processing:vt
	 * ```
	 * For example:
	 * ```ts
	 * this.processingVtKey('emails');
	 * // → "queue:emails:processing:vt"
	 * ```
	 *
	 * ### Where it’s used
	 * - {@link reserveMany} - Adds entries to the VT ZSET upon reserving jobs.  
	 * - {@link extendVisibility} - Extends an existing job’s visibility timeout.  
	 * - {@link requeueExpired} - Scans and requeues expired jobs using this key.  
	 * - {@link getRequeueScript} / {@link ensureRequeueScript} - Lua-based requeue logic.  
	 * - {@link startHeartbeat} - Periodically extends VT to keep jobs “alive.”
	 *
	 * ### Why this matters
	 * - Ensures fault-tolerance in distributed processing systems.  
	 * - Prevents “lost” jobs when workers crash or fail mid-task.  
	 * - Enables time-based requeueing via Lua (`ZRANGEBYSCORE processing:vt 0 now`).  
	 * - Keeps per-queue data neatly namespaced under `"queue:"`.
	 *
	 * @param queueName - The logical name of the queue for which the VT ZSET key
	 *   should be generated. Must be a valid non-empty string.
	 *
	 * @returns
	 * A fully-qualified Redis key string in the format:
	 * `"queue:<queueName>:processing:vt"`.
	 *
	 * @throws
	 * If `queueName` contains invalid symbols, is empty, or otherwise fails
	 * validation, an error will be thrown by {@link toKeyString}.
	 *
	 * @example
	 * ```ts
	 * // Example: visibility-timeout tracking for the "reports" queue
	 * const vtKey = this.processingVtKey('reports');
	 * // Result: "queue:reports:processing:vt"
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Key Helpers
	 * @see {@link toKeyString} - Base helper for safe key generation.
	 * @see {@link processingKey} - Corresponding list for in-progress jobs.
	 * @see {@link requeueExpired} - Uses this key to requeue expired jobs.
	 * @see {@link extendVisibility} - Renews the score for active tasks in this ZSET.
	 * @see {@link getRequeueScript} - Lua implementation of the requeue logic.
	 */
	private processingVtKey(queueName: string): string {
		return this.toKeyString(queueName, 'processing', 'vt');
	}

	/**
	 * @summary
	 * Builds the **Redis key** for the ZSET that stores **delayed (scheduled)** jobs
	 * belonging to a specific queue.
	 *
	 * @remarks
	 * Each queue in `PowerQueue` maintains a *delayed* key - a sorted set (ZSET)
	 * where each member represents a serialized task scheduled for future execution.
	 *
	 * The ZSET’s **score** holds the UNIX timestamp (in seconds) at which the job
	 * becomes ready to be moved into the main `ready` list.  
	 *  
	 * This key is used by:
	 * - {@link enqueue} - when adding a job with a delay (`delaySec > 0`)
	 * - {@link promoteDelayed} - when moving due jobs from delayed → ready
	 * - {@link ensurePromoteScript} and {@link getPromoteScript} - during atomic Lua promotions
	 *
	 * ### Key structure
	 * The final Redis key follows the pattern:
	 * ```
	 * queue:<queueName>:delayed
	 * ```
	 * For example:
	 * ```ts
	 * this.delayedKey('emails');
	 * // → "queue:emails:delayed"
	 * ```
	 *
	 * ### Why this matters
	 * - Provides **time-based scheduling** for delayed task execution
	 * - Enables atomic promotion via a single `ZRANGEBYSCORE` in Redis
	 * - Prevents naming collisions between multiple queues
	 * - Keeps queue-specific data properly namespaced under `"queue:"`
	 *
	 * @param queueName - The logical name of the queue for which the delayed key should be built.
	 *   Must be a non-empty string; it is validated internally by {@link toKeyString}.
	 *
	 * @returns
	 * A namespaced Redis key string for the delayed ZSET, e.g. `"queue:<queueName>:delayed"`.
	 *
	 * @throws
	 * If `queueName` is invalid (empty, contains wildcards, or disallowed symbols),
	 * an error will be thrown by {@link toKeyString}.
	 *
	 * @example
	 * ```ts
	 * // Example: get the key for delayed jobs in the "notifications" queue
	 * const key = this.delayedKey('notifications');
	 * // Result: "queue:notifications:delayed"
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Key Helpers
	 * @see {@link toKeyString} - Underlying safe key builder.
	 * @see {@link enqueue} - Uses this key when scheduling delayed tasks.
	 * @see {@link promoteDelayed} - Consumes this key to move jobs into the ready list.
	 * @see {@link getPromoteScript} - Lua script implementing the promotion logic.
	 */
	private delayedKey(queueName: string): string {
		return this.toKeyString(queueName, 'delayed');
	}

	/**
	 * @summary
	 * Builds a **strict, namespaced Redis key** for the current queue
	 * by joining validated segments with a colon (`:`).
	 *
	 * @remarks
	 * This helper provides a consistent and safe way to construct Redis keys
	 * used across all queue components - e.g., `ready`, `processing`, `delayed`, etc.
	 *
	 * It delegates the actual validation and concatenation to the parent
	 * {@link PowerRedis.toKeyString} method but automatically prefixes
	 * all queue-related keys with the constant `"queue"`.
	 *
	 * ### Validation rules
	 * - Disallows wildcard characters: `*`, `?`, `[`, `]`
	 * - Disallows spaces and consecutive colons (`::`)
	 * - Ensures that each segment is a non-empty string or number
	 * - Joins valid segments with a single colon (`:`)
	 *
	 * For example, calling:
	 * ```ts
	 * this.toKeyString('emails', 'ready')
	 * ```
	 * will produce the key:
	 * ```
	 * queue:emails:ready
	 * ```
	 * ensuring the `"queue"` namespace is always present, preventing collisions
	 * with unrelated Redis data structures.
	 *
	 * ### Why this matters
	 * - Enforces a predictable key hierarchy for all queue data
	 * - Helps separate PowerQueue data from other Redis applications
	 * - Makes it easier to inspect and debug Redis keys (`SCAN queue:*`)
	 * - Guarantees key safety even when queue names come from dynamic inputs
	 *
	 * @param parts - Array of additional key segments (strings or numbers).
	 *   Each part is validated before being concatenated.
	 *
	 * @returns
	 * A fully qualified Redis key string in the format:
	 * `queue:<segment1>:<segment2>:...`
	 *
	 * @throws
	 * If any segment fails validation (empty, contains forbidden characters,
	 * or is otherwise invalid), an error will be thrown by the parent
	 * {@link PowerRedis.toKeyString} method.
	 *
	 * @example
	 * ```ts
	 * // Build a key for the delayed list of the "images" queue
	 * const key = this.toKeyString('images', 'delayed');
	 * // Result: "queue:images:delayed"
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Key Helpers
	 * @see {@link PowerRedis.toKeyString} - Base implementation handling validation.
	 * @see {@link readyKey} - Uses this method to generate ready list keys.
	 * @see {@link processingKey} - Uses this method for active processing keys.
	 */
	toKeyString(...parts: Array<string | number>): string {
		return super.toKeyString('queue', ...parts);
	}

	/**
	 * @summary
	 * Returns the **Lua script** that atomically reserves jobs from the **ready list**
	 * into the **processing list** using the modern
	 * {@link https://redis.io/commands/lmove | `LMOVE`} command (Redis ≥ 6.2).
	 *
	 * @remarks
	 * This script represents the preferred reservation logic for modern Redis versions.
	 * It uses `LMOVE LEFT→RIGHT` to transfer items in strict **FIFO order** from the ready
	 * list to the processing list, ensuring predictable job sequencing.
	 *
	 * Each reserved job is also registered in the **visibility-timeout ZSET** (`vtkey`),
	 * whose score represents the UNIX timestamp (`deadline`) when the job expires if not acknowledged.
	 *
	 * ### Script logic
	 * - **KEYS**
	 *   1. `source` - Ready list key containing pending jobs.  
	 *   2. `processing` - Processing list key holding currently executing jobs.  
	 *   3. `vtkey` - ZSET key tracking visibility deadlines (`score = deadline`).
	 * - **ARGV**
	 *   1. `limit` - Maximum number of jobs to move in this batch.  
	 *   2. `deadline` - UNIX timestamp (seconds) indicating visibility-expiration time for each moved job.
	 *
	 * ### Pseudocode summary
	 * 1. Initialize `moved = {}`.  
	 * 2. For up to `limit` iterations:
	 *    - Execute `LMOVE source processing LEFT RIGHT`  
	 *      (pops from the **left** of the ready list → pushes to the **right** of the processing list).  
	 *    - Stop early if the ready list is empty.
	 * 3. For each successfully moved job:
	 *    - Record it in the VT ZSET via `ZADD vtkey deadline job`.
	 * 4. Return the table of moved items.
	 *
	 * ### Behavior notes
	 * - Provides **FIFO correctness**: producers add to the right (`RPUSH`), consumers read from the left (`LMOVE LEFT→RIGHT`).
	 * - Executes atomically on the Redis server - no jobs are lost or duplicated.
	 * - The VT ZSET is used later by {@link requeueExpired} to detect unacknowledged (expired) jobs and recycle them.
	 *
	 * @returns
	 * A raw Lua script string suitable for
	 * {@link https://redis.io/commands/script-load | `SCRIPT LOAD`},
	 * {@link https://redis.io/commands/eval | `EVAL`}, or `EVALSHA`.
	 *
	 * @example
	 * ```ts
	 * // Load and execute the LMOVE-based reserve script manually
	 * const lua = this.getReserveScriptLMOVE();
	 * const sha = await redis.script('LOAD', lua);
	 *
	 * const moved = await redis.evalsha(
	 *   sha, 3,
	 *   readyKey, processingKey, vtKey,
	 *   limit, deadline
	 * );
	 *
	 * console.log('Reserved jobs:', moved);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Lua Scripts
	 * @see {@link getReserveScriptRPOPLPUSH} - Legacy fallback for Redis < 6.2 using `RPOPLPUSH`.
	 * @see {@link ensureReserveScript} - Loads this script and caches its SHA for reuse.
	 * @see {@link reserveMany} - High-level API that uses this script internally.
	 * @see {@link https://redis.io/commands/lmove | Redis LMOVE Command}
	 * @see {@link https://redis.io/commands/zadd | Redis ZADD Command}
	 */
	private getReserveScriptLMOVE(): string {
		return `
			local source = KEYS[1]
			local processing = KEYS[2]
			local vtkey = KEYS[3]
			local limit = tonumber(ARGV[1])
			local deadline = tonumber(ARGV[2])
			local moved = {}
			for i = 1, limit do
				local v = redis.call('LMOVE', source, processing, 'LEFT', 'RIGHT')
				if not v then break end
				table.insert(moved, v)
			end
			if #moved > 0 then
				for i = 1, #moved do
					redis.call('ZADD', vtkey, deadline, moved[i])
				end
			end
			return moved
		`;
	}

	/**
	 * @summary
	 * Returns the **Lua script** that atomically reserves jobs from the **ready list**
	 * into the **processing list** using the legacy Redis command
	 * {@link https://redis.io/commands/rpoplpush | `RPOPLPUSH`}.
	 *
	 * @remarks
	 * This script provides a fallback mechanism for Redis versions **prior to 6.2**,
	 * where the modern {@link https://redis.io/commands/lmove | `LMOVE`} command
	 * is not available.  
	 *  
	 * It moves up to a specified number of jobs (`limit`) from the end of the ready
	 * list to the end of the processing list while simultaneously registering each job
	 * in a **visibility-timeout ZSET**, ensuring atomic handoff semantics.
	 *
	 * ### Script logic
	 * - **KEYS**
	 *   1. `source` - Ready list key (queue containing waiting jobs).  
	 *   2. `processing` - Processing list key (queue for in-progress jobs).  
	 *   3. `vtkey` - Visibility-timeout ZSET key, used to track expiration deadlines.
	 * - **ARGV**
	 *   1. `limit` - Maximum number of jobs to move from the ready list.  
	 *   2. `deadline` - UNIX timestamp (in seconds) when each job’s visibility timeout expires.
	 *
	 * ### Pseudocode summary
	 * 1. Initialize an empty table `moved = {}`.
	 * 2. Loop up to `limit` times:
	 *    - Execute `RPOPLPUSH source processing` to atomically move one job.  
	 *      (This pops from the **right** of `source` and pushes to the **left** of `processing`.)
	 *    - Stop if no item was moved (empty list).
	 * 3. For each successfully moved job:
	 *    - Record it in the visibility-timeout ZSET via `ZADD vtkey deadline job`.
	 * 4. Return the list of all moved items (`moved`).
	 *
	 * ### Behavior notes
	 * - Uses **RPOPLPUSH** to emulate FIFO behavior with producers that push via `RPUSH`.
	 * - All operations occur atomically inside Redis, avoiding race conditions between workers.
	 * - The ZSET ensures that tasks are automatically recoverable if not acknowledged before
	 *   `deadline` (handled later by {@link requeueExpired}).
	 *
	 * @returns
	 * A raw Lua source string suitable for {@link https://redis.io/commands/script-load | `SCRIPT LOAD`},
	 * {@link https://redis.io/commands/eval | `EVAL`}, or `EVALSHA`.
	 *
	 * @example
	 * ```ts
	 * // Load the legacy reserve Lua script and execute it manually
	 * const lua = this.getReserveScriptRPOPLPUSH();
	 * const sha = await redis.script('LOAD', lua);
	 *
	 * const moved = await redis.evalsha(
	 *   sha, 3,
	 *   readyKey, processingKey, vtKey,
	 *   limit, deadline
	 * );
	 *
	 * console.log('Moved jobs:', moved);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Lua Scripts
	 * @see {@link getReserveScriptLMOVE} - Modern FIFO variant using `LMOVE` (Redis ≥ 6.2).
	 * @see {@link ensureReserveScript} - Loads either this or the LMOVE version depending on Redis capabilities.
	 * @see {@link reserveMany} - High-level API that uses this script under the hood.
	 * @see {@link https://redis.io/commands/rpoplpush | Redis RPOPLPUSH Command}
	 * @see {@link https://redis.io/commands/zadd | Redis ZADD Command}
	 */
	private getReserveScriptRPOPLPUSH(): string {
		return `
			local source = KEYS[1]
			local processing = KEYS[2]
			local vtkey = KEYS[3]
			local limit = tonumber(ARGV[1])
			local deadline = tonumber(ARGV[2])
			local moved = {}
			for i = 1, limit do
				local v = redis.call('RPOPLPUSH', source, processing)
				if not v then break end
				table.insert(moved, v)
			end
			if #moved > 0 then
				for i = 1, #moved do
					redis.call('ZADD', vtkey, deadline, moved[i])
				end
			end
			return moved
		`;
	}

	/**
	 * @summary
	 * Returns the Lua script that **requeues expired processing tasks** back into
	 * the **ready** queue once their visibility timeout has elapsed.
	 *
	 * @remarks
	 * In a distributed worker system, each job that is taken for processing is moved
	 * from the ready list into a processing list and tracked in a **visibility-timeout ZSET**.
	 * The ZSET’s score represents the timestamp (`deadline`) after which a job is
	 * considered “expired” (i.e. not acknowledged within its visibility window).
	 *
	 * This Lua script performs a fully **atomic requeue** of all expired jobs:
	 *
	 * ### Script logic
	 * - **KEYS**
	 *   1. `processing` - LIST key holding currently processing items.  
	 *   2. `processingVt` - ZSET key that stores visibility deadlines (`score = UNIX seconds`).  
	 *   3. `ready` - LIST key to which expired items should be requeued.
	 * - **ARGV**
	 *   1. `now` - Current UNIX timestamp (seconds).  
	 *   2. `limit` - Maximum number of expired jobs to requeue in this batch.
	 *
	 * ### Pseudocode summary
	 * 1. Fetch expired members using  
	 *    `ZRANGEBYSCORE processingVt 0 now LIMIT 0 limit`.
	 * 2. For each expired job:
	 *    - Remove it from the VT ZSET (`ZREM vt m`).
	 *    - Remove one instance from the processing list (`LREM processing 1 m`).
	 *    - Push it back into the ready list (`RPUSH ready m`).
	 * 3. Return the number of items requeued.
	 *
	 * This ensures that no job is lost between lists - all operations are performed
	 * within one atomic Lua context on the Redis server, preventing race conditions.
	 *
	 * ### Why this matters
	 * This logic forms the **core reliability mechanism** of a visibility-timeout queue:
	 * if a worker crashes or fails to `ACK` a task, Redis will automatically recycle it
	 * for another worker without requiring external coordination.
	 *
	 * The script is loaded and cached via {@link ensureRequeueScript} and executed through
	 * {@link evalshaWithReload} inside {@link requeueExpired}.
	 *
	 * @returns
	 * A Lua source string that can be passed to
	 * {@link https://redis.io/commands/script-load | `SCRIPT LOAD`} or executed via
	 * `EVAL` / `EVALSHA` by Redis.
	 *
	 * @example
	 * ```ts
	 * // Load and run the Lua requeue script manually
	 * const lua = this.getRequeueScript();
	 * const sha = await redis.script('LOAD', lua);
	 * const requeued = await redis.evalsha(sha, 3, processingKey, vtKey, readyKey, now, limit);
	 * console.log(`Requeued ${requeued} expired jobs`);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Lua Scripts
	 * @see {@link requeueExpired} - Executes this script via `EVALSHA`.
	 * @see {@link ensureRequeueScript} - Preloads and caches the SHA of this script.
	 * @see {@link https://redis.io/commands/zrangebyscore | Redis ZRANGEBYSCORE Command}
	 * @see {@link https://redis.io/commands/zrem | Redis ZREM Command}
	 * @see {@link https://redis.io/commands/lrem | Redis LREM Command}
	 * @see {@link https://redis.io/commands/rpush | Redis RPUSH Command}
	 */
	private getRequeueScript(): string {
		return `
			-- KEYS: 1=processing, 2=processingVt, 3=ready
			-- ARGV: 1=now, 2=limit
			local processing = KEYS[1]
			local vt = KEYS[2]
			local ready = KEYS[3]
			local now = tonumber(ARGV[1])
			local limit = tonumber(ARGV[2])

			local members = redis.call('ZRANGEBYSCORE', vt, 0, now, 'LIMIT', 0, limit)
			for i=1,#members do
				local m = members[i]
				redis.call('ZREM', vt, m)
				redis.call('LREM', processing, 1, m)
				redis.call('RPUSH', ready, m)
			end
			return #members
		`;
	}

	/**
	 * @summary
	 * Returns the raw **Lua script** that promotes **delayed** tasks into the **ready** queue.
	 *
	 * @remarks
	 * This Lua script performs an **atomic promotion** of all delayed jobs whose scheduled
	 * timestamps (`score` in the delayed ZSET) are less than or equal to the current time.
	 *  
	 * It is used internally by {@link promoteDelayed} through `EVALSHA` for efficient,
	 * lock-free promotion of scheduled (future) jobs.
	 *
	 * ### Script logic
	 * - **KEYS**
	 *   1. `delayed` - ZSET key where delayed jobs are stored (each job’s score represents its ready timestamp).  
	 *   2. `ready` - LIST key representing the main ready queue for execution.
	 * - **ARGV**
	 *   1. `now` - Current UNIX timestamp in seconds.  
	 *   2. `limit` - Maximum number of items to promote in this batch.
	 *
	 * ### Pseudocode summary
	 * 1. Use `ZRANGEBYSCORE delayed 0 now LIMIT 0 limit`  
	 *    to select all jobs that are *due* for execution.
	 * 2. For each selected member:
	 *    - Remove it from the delayed ZSET (`ZREM delayed m`).
	 *    - Append it to the ready LIST (`RPUSH ready m`).
	 * 3. Return the number of items successfully promoted.
	 *
	 * This Lua script ensures that:
	 * - No job is lost or promoted twice within the same atomic transaction.
	 * - Promotions are efficient even under high concurrency.
	 * - Scheduling precision depends only on Redis timestamp granularity (seconds).
	 *
	 * ### Use case
	 * The script is typically preloaded via {@link ensurePromoteScript} and executed
	 * through {@link evalshaWithReload} within {@link promoteDelayed}.
	 *  
	 * It replaces client-side loops that would otherwise require multiple `ZRANGE` and `RPUSH`
	 * calls per item - cutting latency and eliminating race conditions.
	 *
	 * @returns
	 * A Lua source string that can be passed to
	 * {@link https://redis.io/commands/script-load | `SCRIPT LOAD`} or
	 * {@link https://redis.io/commands/eval | `EVAL`} / `EVALSHA`.
	 *
	 * @example
	 * ```ts
	 * // Load and run the Lua script manually
	 * const lua = this.getPromoteScript();
	 * const sha = await redis.script('LOAD', lua);
	 * const promoted = await redis.evalsha(sha, 2, delayedKey, readyKey, now, limit);
	 * console.log(`Promoted ${promoted} delayed items`);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Lua Scripts
	 * @see {@link promoteDelayed} - Method that executes this script via `EVALSHA`.
	 * @see {@link ensurePromoteScript} - Preloads this script and caches its SHA.
	 * @see {@link https://redis.io/commands/zrangebyscore | Redis ZRANGEBYSCORE Command}
	 * @see {@link https://redis.io/commands/zrem | Redis ZREM Command}
	 * @see {@link https://redis.io/commands/rpush | Redis RPUSH Command}
	 */
	private getPromoteScript(): string {
		return `
			-- KEYS: 1=delayed, 2=ready
			-- ARGV: 1=now, 2=limit
			local delayed = KEYS[1]
			local ready = KEYS[2]
			local now = tonumber(ARGV[1])
			local limit = tonumber(ARGV[2])

			local due = redis.call('ZRANGEBYSCORE', delayed, 0, now, 'LIMIT', 0, limit)
			for i=1,#due do
				local m = due[i]
				redis.call('ZREM', delayed, m)
				redis.call('RPUSH', ready, m)
			end
			return #due
		`;
	}

	/**
	 * @summary
	 * Ensures that one of the Lua **reserve scripts** (`LMOVE` or `RPOPLPUSH` variant)
	 * is loaded into Redis and that its SHA1 hash is cached for use by
	 * {@link reserveMany}.
	 *
	 * @remarks
	 * When reserving jobs from the **ready** list into the **processing** list,
	 * `PowerQueue` uses a Lua script to atomically:
	 *
	 * 1. Move up to N items from the ready list (`LPUSH`/`RPUSH` producer side)
	 *    to the processing list.
	 * 2. Register each moved item in the visibility-timeout ZSET (VT key)  
	 *    with a score equal to its expiration timestamp (`deadline` in Unix seconds).
	 *
	 * This method loads and caches the corresponding Lua script in Redis via  
	 * {@link https://redis.io/commands/script-load | `SCRIPT LOAD`}, storing its
	 * SHA1 hash in either {@link reserveSha} or {@link reserveShaRpoplpush},
	 * depending on which variant the Redis server supports.
	 *
	 * ### Behavior
	 * - Prefers the **LMOVE**-based script (Redis ≥ 6.2) for true FIFO semantics.  
	 * - If loading the LMOVE version fails (likely on older Redis), it falls back
	 *   to the **RPOPLPUSH** version, which emulates similar behavior.
	 * - If a SHA is already cached and `force` is `false`, the function returns immediately.
	 * - If the Redis client does not implement `.script`, the method no-ops safely.
	 *
	 * ### Why this matters
	 * Using preloaded Lua scripts provides **atomicity** and **speed**:
	 * - Multiple jobs are reserved and timestamped in one round-trip.
	 * - The operation is protected from partial failures or race conditions.
	 * - Redis 6.2+ gains native FIFO order preservation with `LMOVE LEFT→RIGHT`.
	 *
	 * @param force - If `true`, forces reloading of the Lua script even if a SHA
	 *   already exists (e.g. after a Redis restart or script cache flush).
	 *
	 * @returns
	 * A `Promise<void>` that resolves once the script is loaded or no action is needed.
	 *
	 * @throws
	 * This method intentionally does **not** throw if scripting is unsupported.
	 * If both load attempts fail, both `reserveSha` and `reserveShaRpoplpush` remain `undefined`
	 * and {@link reserveMany} will transparently fall back to a client-side move loop.
	 *
	 * @example
	 * ```ts
	 * // Load and cache the appropriate reserve Lua script
	 * await this.ensureReserveScript();
	 *
	 * // Then reserve items atomically using the cached SHA
	 * const items = await this.evalshaWithReload<string[]>(
	 *   () => this.reserveSha || this.reserveShaRpoplpush,
	 *   (force) => this.ensureReserveScript(force),
	 *   3,
	 *   [readyKey, processingKey, vtKey, limit, deadline]
	 * );
	 *
	 * console.log(`Reserved ${items.length} items for processing`);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Lua Helpers
	 * @see {@link getReserveScriptLMOVE} - Returns the LMOVE Lua variant.
	 * @see {@link getReserveScriptRPOPLPUSH} - Returns the legacy RPOPLPUSH Lua variant.
	 * @see {@link reserveMany} - Uses this cached SHA to atomically move tasks.
	 * @see {@link https://redis.io/commands/script-load | Redis SCRIPT LOAD Command}
	 * @see {@link https://redis.io/commands/lmove | Redis LMOVE Command}
	 * @see {@link https://redis.io/commands/rpoplpush | Redis RPOPLPUSH Command}
	 */
	private async ensureReserveScript(force = false): Promise<void> {
		if (!force && (this.reserveSha || this.reserveShaRpoplpush || !isFunc((this.redis as any)?.script))) {
			return;
		}
		this.reserveSha = undefined;
		this.reserveShaRpoplpush = undefined;

		try {
			this.reserveSha = await (this.redis as any)?.script('LOAD', this.getReserveScriptLMOVE());
		}
		catch {
			this.reserveShaRpoplpush = await (this.redis as any)?.script('LOAD', this.getReserveScriptRPOPLPUSH());
		}
	}

	/**
	 * @summary
	 * Ensures that the Lua script used for **requeuing expired processing items**
	 * is loaded into Redis and that its SHA1 hash is cached in {@link requeueSha}.
	 *
	 * @remarks
	 * When a worker fails to acknowledge a task before its **visibility timeout** expires,
	 * the task must be requeued from the *processing* list back to the *ready* list.
	 *  
	 * This Lua script performs that requeue operation atomically in Redis by scanning
	 * a ZSET of visibility deadlines and moving all items whose scores (timestamps)
	 * are less than or equal to the current time (`now`).
	 *
	 * The script ensures:
	 * - Each expired item is removed from both the *processing list* and its VT ZSET.
	 * - It is then safely appended to the *ready list* for retry.
	 *
	 * This helper guarantees that the corresponding Lua script is preloaded via
	 * {@link https://redis.io/commands/script-load | `SCRIPT LOAD`} and that its SHA1
	 * is stored in memory, so that subsequent calls to {@link evalshaWithReload} or
	 * {@link requeueExpired} can use `EVALSHA` directly for faster, atomic execution.
	 *
	 * ### Behavior
	 * - If a valid SHA is already cached and `force` is `false`, the method is a no-op.
	 * - Otherwise, it reloads the Lua source obtained from {@link getRequeueScript}.
	 * - If the Redis client does not support scripting (`.script` not implemented),
	 *   the method silently returns - allowing client-side fallbacks.
	 *
	 * ### Why it matters
	 * Preloading Lua scripts avoids costly round-trip `EVAL` calls and improves both
	 * **latency** and **atomicity** under high concurrency. It also ensures that even
	 * expired jobs are handled consistently without data races between workers.
	 *
	 * @param force - If `true`, forces the script to reload from source even if a SHA
	 *   already exists (for example, after a Redis restart that flushed script cache).
	 *
	 * @returns
	 * A `Promise<void>` that resolves once the script is loaded or returns early if
	 * the client does not support Lua scripting.
	 *
	 * @throws
	 * This method deliberately does **not** throw if scripting is unsupported;  
	 * instead, it quietly skips loading, letting {@link requeueExpired} handle fallback logic.
	 *
	 * @example
	 * ```ts
	 * // Load the Lua requeue script (once per worker)
	 * await this.ensureRequeueScript();
	 *
	 * // Later use evalshaWithReload() to execute it safely
	 * const requeued = await this.evalshaWithReload<number>(
	 *   () => this.requeueSha,
	 *   (force) => this.ensureRequeueScript(force),
	 *   3,
	 *   [processingKey, vtKey, readyKey, now, limit]
	 * );
	 *
	 * console.log(`Requeued ${requeued} expired jobs`);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Lua Helpers
	 * @see {@link getRequeueScript} - Returns the Lua source code for this script.
	 * @see {@link requeueExpired} - Executes the logic that uses this cached SHA.
	 * @see {@link https://redis.io/commands/script-load | Redis SCRIPT LOAD Command}
	 */
	private async ensureRequeueScript(force = false): Promise<void> {
		if (!force && this.requeueSha) {
			return;
		}
		const scriptFn = (this.redis as any)?.script as
			| ((cmd: 'LOAD', lua: string) => Promise<string>)
			| undefined;

		if (!scriptFn) {
			return;
		}
		this.requeueSha = await scriptFn('LOAD', this.getRequeueScript());
	}

	/**
	 * @summary
	 * Ensures that the Lua script responsible for **promoting delayed tasks** is loaded
	 * into Redis and that its SHA1 hash is cached in {@link promoteSha}.
	 *
	 * @remarks
	 * Redis executes Lua scripts via their SHA1 hash for efficiency.  
	 * This helper guarantees that the "promote delayed" Lua script - which moves tasks
	 * from the delayed ZSET (scored by availability time) into the ready list - is
	 * loaded and ready for use by {@link promoteDelayed}.
	 *
	 * ### How it works
	 * - If {@link promoteSha} already contains a valid SHA and `force` is `false`,
	 *   the function returns immediately to avoid redundant network calls.
	 * - Otherwise, it checks that the Redis client implements the `script` command.
	 * - It then loads the Lua string returned by {@link getPromoteScript} via  
	 *   `SCRIPT LOAD`, storing the resulting SHA1 hash in {@link promoteSha}.
	 *
	 * ### Why it matters
	 * The Lua-based promotion is much faster and atomic compared to a client-side loop:
	 * it can move and clean up hundreds of delayed items in a single Redis call.
	 *
	 * By preloading the script and caching its SHA, we allow {@link promoteDelayed}
	 * to use `EVALSHA` instead of `EVAL`, which greatly improves performance and reduces
	 * replication overhead.
	 *
	 * @param force - If `true`, forces the script to reload even if a SHA is already cached.  
	 *   This is typically used after a `NOSCRIPT` error, indicating Redis lost the cached script.
	 *
	 * @returns
	 * A `Promise<void>` that resolves once the Lua script is successfully loaded or
	 * if the client does not support scripting (in which case the method silently returns).
	 *
	 * @throws
	 * This method intentionally does **not** throw if Redis scripting is unsupported -  
	 * it safely no-ops, allowing a fallback to client-side logic in {@link promoteDelayed}.
	 *
	 * @example
	 * ```ts
	 * // Force reload the "promote delayed" Lua script (e.g. after a Redis restart)
	 * await this.ensurePromoteScript(true);
	 *
	 * // Then safely use evalshaWithReload() to execute it:
	 * const promoted = await this.evalshaWithReload<number>(
	 *   () => this.promoteSha,
	 *   (force) => this.ensurePromoteScript(force),
	 *   2,
	 *   [delayedKey, readyKey, now, limit]
	 * );
	 * console.log(`Promoted ${promoted} delayed tasks`);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Lua Helpers
	 * @see {@link promoteDelayed} - Executes the promotion logic using this script.
	 * @see {@link getPromoteScript} - Returns the raw Lua code loaded by this method.
	 * @see {@link https://redis.io/commands/script-load/ | Redis SCRIPT LOAD Command}
	 */
	private async ensurePromoteScript(force = false): Promise<void> {
		if (!force && this.promoteSha) {
			return;
		}
		const scriptFn = (this.redis as any)?.script as
			| ((cmd: 'LOAD', lua: string) => Promise<string>)
			| undefined;

		if (!scriptFn) {
			return;
		}
		this.promoteSha = await scriptFn('LOAD', this.getPromoteScript());
	}

	/**
	 * @summary
	 * Moves **one item** atomically from a source Redis list (ready queue)
	 * to a destination list (processing queue), using the best available command.
	 *
	 * @remarks
	 * This is the *single-item, client-side* fallback used by {@link reserveMany} when
	 * Lua scripts (`LMOVE`/`RPOPLPUSH` batch) are unavailable.  
	 * It attempts to perform a reliable FIFO move between Redis lists
	 * while maintaining compatibility across Redis versions:
	 *
	 * 1. First it tries to call {@link https://redis.io/commands/lmove | `LMOVE`}  
	 *    (available in Redis **≥ 6.2**), moving from the **left** of the source to the **right**
	 *    of the destination list - preserving producer FIFO semantics (`RPUSH` + `LMOVE LEFT→RIGHT`).
	 * 2. If `LMOVE` is not supported or fails, it falls back to  
	 *    {@link https://redis.io/commands/rpoplpush | `RPOPLPUSH`}  
	 *    which emulates similar behavior for older Redis versions (pre-6.2).
	 *
	 * Both operations are *atomic* within a single Redis instance,
	 * ensuring the element is never lost or duplicated between the two lists.
	 *
	 * @param source - Redis key of the **ready list** (where new jobs are waiting).  
	 *   Typically built via `this.toKeyString(queueName)` inside the queue.
	 * @param processing - Redis key of the **processing list**,  
	 *   used to track in-progress jobs for visibility timeouts.
	 *
	 * @returns
	 * A `Promise` resolving to the raw item string that was moved,  
	 * or `null` if the source list was empty or both commands failed.
	 *
	 * @throws
	 * This method does not throw on Redis command errors directly -  
	 * instead it catches and silently falls back, returning `null` if all moves fail.
	 *
	 * @example
	 * ```ts
	 * const item = await this.moveOneToProcessing(
	 *   'queue:images:ready',
	 *   'queue:images:processing'
	 * );
	 * if (item) {
	 *   console.log('Reserved one job for processing:', item);
	 * } else {
	 *   console.log('No jobs available right now');
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Redis Queue Movement
	 * @see {@link reserveMany} - batch variant using Lua for higher throughput
	 * @see {@link https://redis.io/commands/lmove | Redis LMOVE Command}
	 * @see {@link https://redis.io/commands/rpoplpush | Redis RPOPLPUSH Command}
	 */
	private async moveOneToProcessing(source: string, processing: string): Promise<string | null> {
		const cli: any = this.redis;

		try {
			if (isFunc(cli.lmove)) {
				const v = await cli.lmove(source, processing, 'LEFT', 'RIGHT');
				return isStr(v) ? v : null;
			}
		}
		catch {
		}
		try {
			if (isFunc(cli.rpoplpush)) {
				const v = await cli.rpoplpush(source, processing);
				return isStr(v) ? v : null;
			}
		}
		catch {
		}
		return null;
	}

	/**
	 * @summary
	 * Executes a cached Redis Lua script safely using **`EVALSHA`**, with automatic
	 * reload and retry when Redis reports `NOSCRIPT`.
	 *
	 * @remarks
	 * This helper method provides a robust way to call pre-loaded Lua scripts in Redis.
	 * Normally, after a Lua script is loaded via `SCRIPT LOAD`, Redis caches it by its SHA1 hash,
	 * and subsequent calls use `EVALSHA` for performance.
	 *  
	 * However, when the Redis instance restarts or the script cache is flushed, `EVALSHA`
	 * may fail with a `NOSCRIPT` error. This helper transparently handles such cases:
	 *
	 * 1. It calls the provided `ensure(false)` loader to guarantee the script is present
	 *    (but without forcing a reload).
	 * 2. It runs the `EVALSHA` command using the given SHA (obtained from {@link shaGetter}).
	 * 3. If Redis responds with `NOSCRIPT`, it calls `ensure(true)` to reload the script,
	 *    retrieves the new SHA via `shaGetter()`, and retries once automatically.
	 *
	 * This eliminates the need for manual `SCRIPT LOAD` handling throughout the codebase.
	 *  
	 * The method is **generic** - you can specify the expected return type `T` corresponding
	 * to your Lua script’s return value.
	 *
	 * @template T
	 * Type of the value returned by the Lua script (for example, `number`, `string[]`, `Record<string, any>`).
	 *
	 * @param shaGetter - A callback returning the cached SHA string for the Lua script,
	 *   or `undefined` if not yet loaded.  
	 *   Typically references a private property like `this.requeueSha`.
	 *
	 * @param ensure - A callback function `(force?: boolean) => Promise<void>` that
	 *   guarantees the Lua script is loaded into Redis.  
	 *   When `force` is `true`, it should forcibly reload the script via `SCRIPT LOAD`.
	 *
	 * @param numKeys - Number of **key arguments** passed to Redis `EVALSHA`.  
	 *   Redis needs this number to distinguish keys from the following argument list.
	 *
	 * @param keysAndArgs - Array containing both keys and additional arguments
	 *   (`ARGV`), in the exact order expected by your Lua script.  
	 *   All entries are converted to strings before being sent to Redis.
	 *
	 * @returns
	 * Resolves with the return value of the executed Lua script, typed as `T`.
	 *
	 * @throws
	 * - If the Redis client does not support `evalsha`.
	 * - If no SHA is available from {@link shaGetter}.
	 * - If reloading after a `NOSCRIPT` error still yields no SHA.
	 * - Any error originating from the Redis client during script execution.
	 *
	 * @example
	 * ```ts
	 * // Example usage inside a class extending PowerRedis:
	 * private requeueSha?: string;
	 *
	 * private async ensureRequeueScript(force = false) {
	 *   if (!force && this.requeueSha) return;
	 *   this.requeueSha = await this.redis.script('LOAD', LUA_REQUEUE_SCRIPT);
	 * }
	 *
	 * // Later, use evalshaWithReload to execute the cached Lua script safely:
	 * const movedCount = await this.evalshaWithReload<number>(
	 *   () => this.requeueSha,
	 *   (force) => this.ensureRequeueScript(force),
	 *   3, // number of keys
	 *   [processingKey, vtKey, readyKey, now, limit]
	 * );
	 * console.log(`Requeued ${movedCount} expired items`);
	 * ```
	 *
	 * @see {@link https://redis.io/commands/evalsha/ | Redis EVALSHA Command}
	 * @see {@link https://redis.io/docs/latest/develop/interact/scripts/ | Redis Lua Scripting Guide}
	 * @since 2.0.0
	 * @category Redis Lua Helpers
	 */
	private async evalshaWithReload<T = any>(
		shaGetter: () => string | undefined,
		ensure: (force?: boolean) => Promise<void>,
		numKeys: number,
		keysAndArgs: Array<string | number>,
	): Promise<T> {
		await ensure(false);

		const sha = shaGetter();
		const evalshaFn = (this.redis as any)?.evalsha as
			| ((sha: string, numKeys: number, ...args: string[]) => Promise<T>)
			| undefined;

		if (!sha || !evalshaFn) {
			throw new Error('EVALSHA not available or SHA missing');
		}
		try {
			return await evalshaFn(sha, numKeys, ...keysAndArgs.map(String));
		}
		catch (err: unknown) {
			const msg = (err as any)?.message;
			
			if (typeof msg === 'string' && msg.includes('NOSCRIPT')) {
				await ensure(true);
				
				const sha2 = shaGetter();
				
				if (!sha2) {
					throw new Error('EVALSHA NOSCRIPT and reload failed (no SHA)');
				}
				return await evalshaFn(sha2, numKeys, ...keysAndArgs.map(String));
			}
			throw err;
		}
	}

	/**
	 * @summary
	 * Safe helper around Redis `EVALSHA` that **auto-reloads** the Lua script on `NOSCRIPT`
	 * and retries transparently.
	 *
	 * @remarks
	 * Redis caches Lua scripts by their **SHA1**. When a node restarts or the script cache is
	 * evicted, `EVALSHA` may fail with `NOSCRIPT`. This helper:
	 *
	 * 1. Calls the provided {@link ensure} loader to make sure the script is `SCRIPT LOAD`-ed.
	 * 2. Executes `EVALSHA` with the given `numKeys` and `keysAndArgs` (all args coerced to strings).
	 * 3. If it catches `NOSCRIPT`, it forces a reload and retries once.
	 *
	 * This preserves performance (using `EVALSHA` when available) while remaining robust in
	 * failover/sharding scenarios. It also centralizes the `NOSCRIPT` dance so call-sites stay clean.
	 *
	 * **Argument layout**
	 * - `numKeys` tells Redis how many of the subsequent variadic arguments are **keys**; the rest are ARGV.
	 * - `keysAndArgs` must therefore be ordered as `[K1, K2, ..., ARGV1, ARGV2, ...]`.
	 * - All elements are mapped to `string` before calling the client to satisfy Redis wire protocol.
	 *
	 * **Concurrency & idempotency**
	 * - `ensure(false)` is called first to avoid redundant loads; on `NOSCRIPT` we call `ensure(true)`.
	 * - Your `shaGetter` must return the current cached SHA **after** `ensure(true)` succeeds.
	 *
	 * **When to use**
	 * - Any time you have a Lua script with a known/cached SHA and want a one-liner that copes
	 *   with cache misses without falling back to `EVAL` of the raw script.
	 *
	 * @template T
	 * The expected return type from the underlying Lua script (e.g., `number`, `string[]`, etc.).
	 *
	 * @typeParam T - Lua result type returned by the Redis client’s `evalsha`.
	 *
	 * @param shaGetter - Function that returns the cached SHA string (or `undefined` if not set).
	 *   Typically closes over a private field (e.g., `this.requeueSha`).
	 * @param ensure - Script loader `(force?: boolean) => Promise<void>`. Should `SCRIPT LOAD` the
	 *   proper Lua and update the SHA storage. Called with `false` first, and `true` on `NOSCRIPT`.
	 * @param numKeys - Number of **key** arguments per Redis `EVALSHA` contract.
	 * @param keysAndArgs - Ordered list of keys followed by args. Each entry is turned into `String(...)`
	 *   before being sent to Redis.
	 *
	 * @returns
	 * Resolves with whatever the Lua script returns, typed as `T`.
	 *
	 * @throws
	 * - If `shaGetter()` yields no SHA or the client does not support `evalsha`.
	 * - If reloading on `NOSCRIPT` fails to produce a SHA.
	 * - Any other error thrown by the underlying Redis client.
	 *
	 * @example
	 * ```ts
	 * // Inside a class:
	 * private requeueSha?: string;
	 * private async ensureRequeueScript(force = false) {
	 *   if (!force && this.requeueSha) return;
	 *   this.requeueSha = await this.redis.script('LOAD', LUA_REQUEUE);
	 * }
	 *
	 * const moved = await this.evalshaWithReload<number>(
	 *   () => this.requeueSha,
	 *   (force) => this.ensureRequeueScript(!!force),
	 *   3,
	 *   [processingKey, vtKey, readyKey, now, limit]
	 * );
	 * ```
	 *
	 * @see {@link https://redis.io/docs/latest/develop/interact/scripts/ | Redis Scripting}
	 * @see {@link https://redis.io/commands/evalsha/ | EVALSHA}
	 * @since 2.0.0
	 * @category Lua/EVALSHA Helpers
	 */
	private async zaddCompatXXCH(key: string, score: number, member: string): Promise<void> {
		const zadd = (this.redis as any)?.zadd as Function | undefined;

		try {
			if (zadd) {
				await zadd.call(this.redis, key, 'XX', 'CH', score, member);
				return;
			}
		} 
		catch {
		}
		try {
			if (zadd) {
				await zadd.call(this.redis, key, 'CH', 'XX', score, member);
				return;
			}
		} 
		catch {
		}
		try {
			await (this.redis as any).zadd(key, score, member);
		} 
		catch {
		}
	}

	/**
	 * @summary
	 * Starts a periodic **heartbeat** that extends a task’s **visibility timeout (VT)**
	 * while it is being processed, preventing premature requeue.
	 *
	 * @remarks
	 * When a task is reserved, its raw payload is moved to the `processing` list and recorded
	 * in the **processing VT ZSET** with a deadline (`score = now + visibilityTimeoutSec`).
	 * If the worker needs more time, the task’s deadline must be renewed periodically to
	 * keep it invisible to other consumers. This method:
	 *
	 * 1) Looks up the raw payload string for the given `task.id` in {@link processingRaw}.  
	 * 2) Computes a heartbeat period of **~40%** of {@link visibilityTimeoutSec} (clamped to ≥ **1000 ms**)
	 *    to renew well before expiry (buffer against GC pauses and transient stalls).  
	 * 3) Schedules a `setInterval` that calls {@link extendVisibility} for the task’s VT ZSET member.  
	 * 4) Stores the interval handle in {@link heartbeatTimers} so it can be stopped later by {@link stopHeartbeat}.
	 *
	 * Notes:
	 * - If the raw string is missing (e.g., task not in processing), the method safely no-ops.  
	 * - The timer is `unref()`’d (when available) so it does not keep the Node.js event loop alive.  
	 * - Failures inside `extendVisibility` are intentionally suppressed (`.catch(()=>{})`) because
	 *   the main execution flow should not crash due to transient Redis errors; requeue logic will
	 *   take over if the VT actually expires.
	 *
	 * @param task - The task currently in-flight. Must have a valid `id` and `queueName`.
	 *
	 * @returns
	 * `void` - the heartbeat is scheduled as a side effect. If the raw payload cannot be found,
	 * the method returns without scheduling a timer.
	 *
	 * @example
	 * ```ts
	 * // Internally invoked when a task is reserved and decoded:
	 * this.processingRaw.set(task.id, rawString);
	 * this.startHeartbeat(task); // periodically extends VT
	 *
	 * // Later, on completion or finalization:
	 * this.stopHeartbeat(task);  // clears the timer
	 * await this.ack(task);      // removes from processing & VT sets
	 * ```
	 *
	 * @since 2.0.0
	 * @category Time & Visibility
	 * @see {@link extendVisibility} - Renews the VT deadline in the processing ZSET.
	 * @see {@link stopHeartbeat} - Stops and cleans up the timer started here.
	 * @see {@link processingVtKey} - Builds the ZSET key used to store VT deadlines.
	 * @see {@link visibilityTimeoutSec} - Base duration used to compute the heartbeat cadence.
	 */
	private startHeartbeat(task: Task) {
		const raw = this.processingRaw.get(task.id);

		if (!raw) {
			return;
		}
		const vtKey = this.processingVtKey(task.queueName);
		const periodMs = Math.max(1000, Math.floor(this.visibilityTimeoutSec * 1000 * 0.4));
		const t = setInterval(() => {
			this.extendVisibility(vtKey, raw, this.visibilityTimeoutSec).catch(()=>{});
		}, periodMs);
		(t as any).unref?.();
		this.heartbeatTimers.set(task.id, t);
	}

	/**
	 * @summary
	 * Stops and cleans up the **heartbeat timer** previously started by
	 * {@link startHeartbeat} for the given task.
	 *
	 * @remarks
	 * Each task being processed may have an active heartbeat interval
	 * (stored in {@link heartbeatTimers}) that periodically extends
	 * its **visibility timeout** in the `processing:vt` ZSET.  
	 *  
	 * When the task finishes (successfully, failed, or retried),
	 * this method ensures that its heartbeat timer is cleared and removed
	 * from the tracking map to prevent redundant Redis operations and memory leaks.
	 *
	 * ### Behavior
	 * 1. Looks up the timer handle for `task.id` in {@link heartbeatTimers}.  
	 * 2. If found, calls `clearInterval()` to stop further VT extensions.  
	 * 3. Removes the entry from the internal map regardless of whether it existed.  
	 * 4. Silently no-ops if the timer was never started.
	 *
	 * This method is typically invoked in the `finally` block of task execution
	 * logic, right before acknowledgment (`ack`) or failure handling.
	 *
	 * ### Why this matters
	 * - Prevents background timers from extending visibility after a task is done.  
	 * - Avoids memory retention and unnecessary Redis writes.  
	 * - Keeps internal state (`heartbeatTimers`) consistent with the processing map.  
	 * - Guarantees clean lifecycle transitions for every task.
	 *
	 * @param task - The task whose heartbeat interval should be stopped.
	 *   Must have a valid `id` corresponding to an entry in {@link heartbeatTimers}.
	 *
	 * @returns
	 * `void` - The heartbeat timer is stopped as a side effect.  
	 * If no timer existed for the task, the method exits without error.
	 *
	 * @example
	 * ```ts
	 * // Example usage inside the task execution flow
	 * try {
	 *   await this.execute(task);
	 * } finally {
	 *   this.stopHeartbeat(task); // prevent further VT extensions
	 *   await this.ack(task);     // remove task from processing lists
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Time & Visibility
	 * @see {@link startHeartbeat} - Starts the periodic VT renewal loop.
	 * @see {@link heartbeatTimers} - Internal map storing all active intervals.
	 * @see {@link extendVisibility} - Called periodically by the heartbeat to renew deadlines.
	 */
	private stopHeartbeat(task: Task) {
		const t = this.heartbeatTimers.get(task.id);

		if (t) {
			clearInterval(t);
		}
		this.heartbeatTimers.delete(task.id);
	}

	/**
	 * @summary
	 * Atomically **reserves up to `limit` jobs** from a ready list into a processing list
	 * and registers their **visibility timeout** in a ZSET, preferring Lua (`EVALSHA`)
	 * and falling back to a client-side loop when needed.
	 *
	 * @remarks
	 * This method is the primary high-throughput reservation path. It:
	 * 1) Validates Redis connectivity and key/argument formats.  
	 * 2) Ensures a compiled **reserve Lua script** is available via {@link ensureReserveScript}.  
	 * 3) Tries `EVALSHA` using the **LMOVE** variant first (Redis ≥ 6.2), then **RPOPLPUSH** variant as a fallback.  
	 * 4) On `NOSCRIPT`, **reloads** and retries once (see {@link ensureReserveScript}).  
	 * 5) If scripting is unavailable, falls back to a **client-side loop** using {@link moveOneToProcessing}, then
	 *    batches `ZADD` operations to set the VT **deadline** for each moved item.
	 *
	 * **Visibility timeout (VT):**  
	 * For each reserved item, a member is inserted into the `processingVt` ZSET with `score = now + visibilitySec`.
	 * This prevents other consumers from seeing the item until either it is acknowledged
	 * (removed by {@link ackProcessing}) or it expires and is requeued by {@link requeueExpired}.
	 *
	 * **Preference order**
	 * 1) Lua + `LMOVE LEFT→RIGHT` (true FIFO; Redis ≥ 6.2)  
	 * 2) Lua + `RPOPLPUSH` (legacy fallback)  
	 * 3) Client-side loop + batched `ZADD`
	 *
	 * @param source - Ready list key (e.g., `queue:<name>`).
	 * @param processing - Processing list key (e.g., `queue:<name>:processing`).
	 * @param processingVt - Visibility-timeout ZSET key (e.g., `queue:<name>:processing:vt`).
	 * @param limit - Maximum number of items to move. **Default:** `100`.
	 * @param visibilitySec - Visibility timeout in seconds for newly reserved items. **Default:** `60`. Must be `> 0`.
	 *
	 * @returns
	 * A list of **raw item strings** actually moved into processing (possibly empty).
	 *
	 * @throws
	 * - `"Redis connection error."` if the client is not connected.  
	 * - `"Key format error."` if any key is empty/invalid.  
	 * - `"Limit/visibility format error."` if `limit` or `visibilitySec` are invalid.
	 * - Any error from the underlying Redis client during `evalsha` or transactions (re-thrown unless handled).
	 *
	 * @example
	 * ```ts
	 * const ready   = this.toKeyString('images');
	 * const proc    = this.toKeyString('images', 'processing');
	 * const procVt  = this.toKeyString('images', 'processing', 'vt');
	 *
	 * // Reserve up to 200 items with a 90-second visibility window
	 * const raws = await this.reserveMany(ready, proc, procVt, 200, 90);
	 * console.log(`Reserved ${raws.length} items`);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Reservation
	 * @see {@link ensureReserveScript} - Preloads LMOVE/RPOPLPUSH Lua variants and caches SHAs.
	 * @see {@link moveOneToProcessing} - Client-side single-item fallback when Lua isn’t available.
	 * @see {@link ackProcessing} - Acknowledges success and removes VT entries.
	 * @see {@link requeueExpired} - Scans `processing:vt` and requeues expired items.
	 * @see {@link getReserveScriptLMOVE} | {@link getReserveScriptRPOPLPUSH} - The Lua scripts used here.
	 */
	async reserveMany(source: string, processing: string, processingVt: string, limit: number = 100, visibilitySec: number = 60): Promise<string[]> {
		if (!this.checkConnection()) {
			throw new Error('Redis connection error.');
		}
		if (!isStrFilled(source) || !isStrFilled(processing) || !isStrFilled(processingVt)) {
			throw new Error('Key format error.');
		}
		if (!isNumP(limit) || !isNumP(visibilitySec)) {
			throw new Error('Limit/visibility format error.');
		}
		await this.ensureReserveScript();

		const deadline = this.nowSec() + visibilitySec;
		const tryEval = async () => {
			if (isFunc((this.redis as any)?.evalsha)) {
				if (this.reserveSha) {
					return await (this.redis as any)?.evalsha(this.reserveSha, 3, source, processing, processingVt, String(limit), String(deadline));
				}
				if (this.reserveShaRpoplpush) {
					return await (this.redis as any)?.evalsha(this.reserveShaRpoplpush, 3, source, processing, processingVt, String(limit), String(deadline));
				}
			}
			return null;
		};

		try {
			const res = await tryEval();

			if (isArr(res)) {
				return Array.from(res as ReadonlyArray<string>).map(String);
			}
		} 
		catch (err) {
			if (isStr((err as any)?.message) && String((err as any)?.message ?? '').includes('NOSCRIPT')) {
				await this.ensureReserveScript(true);
				try {
					const res2 = await tryEval();

					if (isArr(res2)) {
						return Array.from(res2 as ReadonlyArray<string>).map(String);
					}
				} 
				catch {
				}
			}
		}
		const moved: string[] = [];

		for (let i = 0; i < limit; i++) {
			const v = await this.moveOneToProcessing(source, processing);

			if (!v) {
				break;
			}
			moved.push(v);
		}
		if (moved.length) {
			const tx = (this.redis as any)?.multi();

			for (const v of moved) {
				tx.zadd(processingVt, deadline, v);
			}
			await tx.exec();
		}
		return moved;
	}

	/**
	 * @summary
	 * Acknowledges a **successfully processed** task by removing its raw payload from the
	 * queue’s **processing list** and from the **visibility-timeout ZSET** in a single
	 * Redis `MULTI/EXEC` transaction.
	 *
	 * @remarks
	 * When a task is reserved, its serialized payload (the **raw** string) is placed into:
	 *
	 * - the *processing* list (tracks in-flight items), and
	 * - the *processing:vt* ZSET (stores the visibility deadline).
	 *
	 * Calling `ackProcessing()` confirms the task finished and makes it eligible to be
	 * permanently removed from both structures. The method:
	 *
	 * 1) Verifies Redis connectivity,  
	 * 2) Starts a `MULTI` transaction,  
	 * 3) Enqueues `LREM processing 1 <raw>` and `ZREM processingVt <raw>`,  
	 * 4) Executes `EXEC`.
	 *
	 * If the *raw* payload is already absent (e.g., duplicate ACK or a prior cleanup),
	 * the operation is a **no-op**—Redis will simply report zero removals. Using
	 * `LREM ... 1` ensures only a single list occurrence is removed, which is the
	 * expected invariant for correctly managed processing items.
	 *
	 * ---
	 * ### Why a transaction?
	 * Removing from the list and the ZSET in one `MULTI/EXEC` block minimizes race
	 * windows and keeps the two data structures in sync. While Redis transactions are
	 * not rollbacks in the RDBMS sense, grouping these commands ensures they execute
	 * contiguously and reduces the chance of partial cleanup under concurrency.
	 *
	 * Performance:
	 * - `LREM` is `O(n)` in the list length; `ZREM` is `O(log n)` in the ZSET size.  
	 * - Using a single `MULTI/EXEC` round-trip is generally faster than executing
	 *   two independent calls.
	 *
	 * Security:
	 * - Keys and the `raw` member come from internal, validated sources in this class.
	 * - Avoid passing untrusted strings directly into this method.
	 *
	 * Complexity:
	 * - Time: `O(n + log n)` (list removal + ZSET removal)  
	 * - Space: `O(1)` client-side; Redis memory decreases by the removed member(s).
	 * 
	 * Notes:
	 * - This method intentionally does not remove the item from the *ready* list—
	 *   by design, acknowledged items should no longer exist there.
	 * - If duplicates were somehow inserted into the processing list, `LREM ... 1`
	 *   will remove only one instance. Consider auditing upstream logic if you
	 *   observe duplicates.
	 *
	 * @param processing - Redis **LIST** key that holds in-flight (reserved) items for the queue.
	 *   Typically built with {@link processingKey}.
	 * @param processingVt - Redis **ZSET** key that tracks visibility deadlines for the same items.
	 *   Typically built with {@link processingVtKey}.
	 * @param raw - The **exact serialized payload** string that represents the reserved task.
	 *   Must match the member stored in both the list and the ZSET.
	 *
	 * @returns
	 * `Promise<void>` - resolves after Redis confirms the transaction. No value is returned.
	 *
	 * @throws
	 * - `"Redis connection error."` if the client is not connected.
	 * - Any error propagated by the underlying Redis client during `MULTI/EXEC`.
	 *
	 * @example
	 * ```ts
	 * // After finishing a task:
	 * const raw = JSON.stringify(task); // or PowerQueue.toPayload(task)
	 * const processing   = this.toKeyString(queueName, 'processing');
	 * const processingVt = this.toKeyString(queueName, 'processing', 'vt');
	 *
	 * await this.ackProcessing(processing, processingVt, raw);
	 * // The task is now removed from both the processing list and its VT ZSET.
	 * ```
	 *
	 * @since 2.0.0
	 * @category Acknowledgement
	 * @see {@link ack} - High-level wrapper that looks up the raw payload and calls this method.
	 * @see {@link reserveMany} - Puts items into `processing` and `processing:vt`.
	 * @see {@link requeueExpired} - Moves unacknowledged, expired items back to ready.
	 */
	async ackProcessing(processing: string, processingVt: string, raw: string): Promise<void> {
		if (!this.checkConnection()) {
			throw new Error('Redis connection error.');
		}
		const tx = (this.redis as any)?.multi();

		tx.lrem(processing, 1, raw);
		tx.zrem(processingVt, raw);

		await tx.exec();
		return;
	}

	/**
	 * @summary
	 * Requeues **expired in-flight tasks** from the *processing* list back to the
	 * *ready* list when their **visibility timeout** has elapsed.
	 *
	 * @remarks
	 * In PowerQueue, a reserved task is moved from the ready list into the processing list
	 * and recorded in a visibility-timeout ZSET (`processing:vt`) where the **score** is
	 * the UNIX timestamp (seconds) when the task should become visible again if it
	 * wasn’t acknowledged.
	 *
	 * This method performs that **visibility recovery** in two stages:
	 *
	 * 1) **Fast-path (Lua, atomic, single round-trip):**  
	 *    Executes the cached Lua script (see {@link getRequeueScript}) via
	 *    {@link evalshaWithReload}. The script:
	 *    - finds expired members with `ZRANGEBYSCORE processing:vt 0 now LIMIT 0 chunk`,
	 *    - for each member `m`: `ZREM processing:vt m`, `LREM processing 1 m`, `RPUSH ready m`,  
	 *    - returns the number of requeued items.
	 *
	 * 2) **Fallback (client-side):**  
	 *    If scripting is unavailable or returns `NOSCRIPT` and reload fails, it:
	 *    - reads expired members with `ZREANGEBYSCORE`,  
	 *    - removes them from *processing* and *processing:vt*,  
	 *    - appends them to *ready*,  
	 *    - all within a single `MULTI/EXEC` transaction.
	 *
	 * The operation is **idempotent** with respect to each raw payload value: if a member
	 * does not exist in one of the structures, the corresponding Redis command is a no-op.
	 *
	 * ---
	 * ### When to call
	 * The worker loop periodically calls this method to ensure that **stalled** or **crashed**
	 * workers do not permanently “hold” tasks. Any job that outlives its visibility window
	 * becomes eligible for retry and is returned to the *ready* list.
	 *
	 * Performance:
	 * - **Lua path:** one server-side atomic loop → typically the fastest and safest under load.  
	 * - **Fallback path:** one `Z*` read + a batched `MULTI/EXEC` write round-trip.  
	 * - Choose a sensible `chunk` that balances latency and fairness across queues.
	 *
	 * Complexity:
	 * - Lua: `ZRANGEBYSCORE` + `ZREM` per item (`O(log n)`) + `LREM` list scan (`O(n)` worst-case) + `RPUSH` (`O(1)`).  
	 * - Fallback: same primitives executed via client-side transaction.  
	 * - Overall per batch: approximately `O(k * (log n + n_list))`, where `k ≤ chunk`.
	 *
	 * Security:
	 * - Keys are expected to be generated by {@link toKeyString}; do not pass user-controlled
	 *   arbitrary key names. Raw members originate from `enqueue()`/reservation.
	 *
	 * Notes:
	 * - `nowTs` defaults to the worker’s current UNIX seconds; provide it explicitly only
	 *   for testing or custom scheduling.  
	 * - If multiple workers run this concurrently, Redis atomicity ensures each expired
	 *   item is requeued **once**.  
	 * - Items requeued here will be picked up again by `reserveMany()` and retried according
	 *   to your backoff policy.
	 *
	 * @param processing - Redis **LIST** key holding currently reserved (in-flight) items.
	 * @param processingVt - Redis **ZSET** key that stores visibility deadlines (score = UNIX seconds).
	 * @param ready - Redis **LIST** key to which expired items are requeued for retry.
	 * @param nowTs - Optional UNIX timestamp (seconds) used as the “current time” cutoff.
	 *   If omitted, the method uses {@link nowSec}. {Default: current time}
	 * @param chunk - Maximum number of expired items to move in this batch.  
	 *   {Default: 1000}
	 *
	 * @returns
	 * Resolves to the **number of items requeued** (0 if none were due).
	 *
	 * @throws
	 * - `"Redis connection error."` if the client is not connected.
	 * - Any error surfaced by the underlying Redis client (Lua path or fallback transaction).
	 *
	 * @example
	 * ```ts
	 * // Typical periodic call inside a worker loop:
	 * const moved = await this.requeueExpired(
	 *   this.processingKey('emails'),
	 *   this.processingVtKey('emails'),
	 *   this.readyKey('emails'),
	 * );
	 * if (moved > 0) {
	 *   console.log(`Recovered ${moved} expired tasks back to ready.`);
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Testing with a fixed timestamp and smaller chunk:
	 * const now = Math.floor(Date.now() / 1000);
	 * await this.requeueExpired(
	 *   this.processingKey('reports'),
	 *   this.processingVtKey('reports'),
	 *   this.readyKey('reports'),
	 *   now,
	 *   100,
	 * );
	 * ```
	 *
	 * @since 2.0.0
	 * @category Visibility & Recovery
	 * @see {@link getRequeueScript} for the atomic server-side implementation.
	 * @see {@link evalshaWithReload} for robust `EVALSHA` execution with `NOSCRIPT` recovery.
	 * @see {@link reserveMany} for how items enter *processing* / *processing:vt*.
	 */
	async requeueExpired(processing: string, processingVt: string, ready: string, nowTs?: number, chunk: number = 1000): Promise<number> {
		if (!this.checkConnection()) {
			throw new Error('Redis connection error.');
		}
		const now = isNumP(nowTs) ? nowTs : this.nowSec();

		try {
			const moved = await this.evalshaWithReload<number>(
				() => this.requeueSha,
				(force) => this.ensureRequeueScript(!!force),
				3,
				[ processing, processingVt, ready, String(now), String(chunk) ],
			);

			return isNumP(moved) ? moved : 0;
		}
		catch {
			const expired = await (this.redis as any)?.zrangebyscore(processingVt, 0, now, 'LIMIT', 0, chunk);

			if (!isArrFilled(expired)) {
				return 0;
			}
			const tx = (this.redis as any)?.multi();

			for (const raw of expired) {
				tx.lrem(processing, 1, raw);
				tx.zrem(processingVt, raw);
				tx.rpush(ready, raw);
			}
			await tx.exec();
			return expired.length;
		}
	}

	/**
	 * @summary
	 * Promotes **scheduled (delayed)** tasks from a queue’s **delayed ZSET** into the
	 * **ready LIST** once their scheduled time (ZSET score) is **due**.
	 *
	 * @remarks
	 * Delayed jobs are stored in a ZSET where each member is the serialized task (**raw**),
	 * and the **score** is the UNIX timestamp (in seconds) when the task becomes eligible
	 * to run. This method moves up to `chunk` due members (score `≤ now`) into the ready
	 * list so workers can reserve and process them.
	 *
	 * It has two execution paths:
	 *
	 * 1) **Fast-path (Lua; atomic; single round-trip):**  
	 *    Uses {@link evalshaWithReload} to run the cached script from {@link getPromoteScript}.
	 *    The script:
	 *    - `ZRANGEBYSCORE delayed 0 now LIMIT 0 chunk` → due members  
	 *    - For each member `m`: `ZREM delayed m`; `RPUSH ready m`  
	 *    - Returns the count of promoted items.
	 *
	 * 2) **Fallback (client-side; transactional):**  
	 *    If Lua isn’t available or `NOSCRIPT` reload ultimately fails, it:
	 *    - Reads `due` via `ZRANGEBYSCORE`,  
	 *    - Executes a `MULTI/EXEC` removing each from `delayed` and `RPUSH`-ing to `ready`,  
	 *    - Returns the number of promoted items.
	 *
	 * **Ordering notes**
	 * - Within a single call, items are appended to `ready` in the order returned by
	 *   `ZRANGEBYSCORE` (sorted by score, then lexicographically for ties).  
	 * - Producers typically `RPUSH` into `ready`, and consumers `LMOVE LEFT→RIGHT`, preserving FIFO.
	 *
	 * Performance:
	 * - **Lua path:** one atomic server-side operation; best under high concurrency.  
	 * - **Fallback path:** one range read + one `MULTI/EXEC` write; still efficient for moderate loads.  
	 * - Tune `chunk` to balance latency and fairness across queues.
	 *
	 * Complexity:
	 * - Per batch: `ZRANGEBYSCORE` + (`ZREM` `O(log n)` + `RPUSH` `O(1)`) per promoted member.  
	 * - Overall approx: `O(k * log n)` for `k ≤ chunk` (ignoring LIST append cost).
	 *
	 * Security:
	 * - Keys should come from {@link toKeyString} to avoid unsafe key names.  
	 * - `raw` payloads originate from `enqueue()` and internal flows (not user input).
	 *
	 * Notes:
	 * - `nowTs` defaults to the worker’s current UNIX seconds via {@link nowSec}.  
	 * - Safe under concurrent callers: Redis atomicity ensures a due member is promoted once.  
	 * - After promotion, tasks will be discoverable by {@link reserveMany} on the next iteration.
	 *
	 * @param delayed - Redis **ZSET** key storing delayed members (`score = readyAt UNIX seconds`).
	 * @param ready - Redis **LIST** key where due items are appended for immediate processing.
	 * @param nowTs - Optional UNIX timestamp (seconds) used as the cutoff for “due”.  
	 *   {Default: current time (via nowSec)}
	 * @param chunk - Maximum number of items to promote in this batch.  
	 *   {Default: 1000}
	 *
	 * @returns
	 * Resolves to the **number of delayed items promoted** into the ready list (0 if none).
	 *
	 * @throws
	 * - `"Redis connection error."` if the client is not connected.  
	 * - Any error surfaced by the underlying Redis client (Lua path or fallback transaction).
	 *
	 * @example
	 * ```ts
	 * // Periodic promotion in the worker loop:
	 * const promoted = await this.promoteDelayed(
	 *   this.delayedKey('emails'),
	 *   this.readyKey('emails'),
	 * );
	 * if (promoted) {
	 *   console.log(`Promoted ${promoted} delayed email tasks`);
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Testing with a fixed timestamp and a smaller batch:
	 * const now = Math.floor(Date.now() / 1000);
	 * await this.promoteDelayed(
	 *   this.delayedKey('reports'),
	 *   this.readyKey('reports'),
	 *   now,
	 *   200,
	 * );
	 * ```
	 *
	 * @since 2.0.0
	 * @category Scheduling & Delays
	 * @see {@link getPromoteScript} for the atomic Lua implementation.
	 * @see {@link evalshaWithReload} for robust `EVALSHA` execution with auto-reload on `NOSCRIPT`.
	 * @see {@link enqueue} to schedule delayed tasks (by passing `delaySec > 0`).
	 * @see {@link reserveMany} to reserve items from `ready` into `processing`.
	 */
	async promoteDelayed(delayed: string, ready: string, nowTs?: number, chunk: number = 1000): Promise<number> {
		if (!this.checkConnection()) {
			throw new Error('Redis connection error.');
		}
		const now = isNumP(nowTs) ? nowTs : this.nowSec();

		try {
			const promoted = await this.evalshaWithReload<number>(
				() => this.promoteSha,
				(force) => this.ensurePromoteScript(!!force),
				2,
				[ delayed, ready, String(now), String(chunk) ],
			);

			return isNumP(promoted) ? promoted : 0;
		}
		catch {
			const due = await (this.redis as any)?.zrangebyscore(delayed, 0, now, 'LIMIT', 0, chunk);

			if (!isArrFilled(due)) {
				return 0;
			}
			const tx = (this.redis as any)?.multi();

			for (const raw of due) {
				tx.zrem(delayed, raw);
				tx.rpush(ready, raw);
			}
			await tx.exec();
			return due.length;
		}
	}

	/**
	 * @summary
	 * Enqueues a new task into the queue - either **immediately** (ready list)
	 * or **scheduled for future execution** (delayed ZSET) depending on `delaySec`.
	 *
	 * @remarks
	 * The enqueue process serializes the provided `payload` into a raw string
	 * (via {@link toPayload}) and pushes it to the appropriate Redis data structure:
	 *
	 * - **Immediate tasks:**  
	 *   - Stored in the `ready` **LIST** using `RPUSH`.  
	 *   - These tasks become instantly available to workers via {@link reserveMany}.
	 *
	 * - **Delayed tasks:**  
	 *   - Stored in the `delayed` **ZSET** using `ZADD delayed <score> <raw>`.  
	 *   - The **score** is the UNIX timestamp (seconds) representing the moment when
	 *     the task becomes eligible for execution.  
	 *   - The task will later be automatically moved to the `ready` list by
	 *     {@link promoteDelayed} once its score ≤ current time.
	 *
	 * ---
	 * ### Behavior summary
	 * | Mode | Condition | Redis Command | Destination | Description |
	 * |------|------------|----------------|--------------|-------------|
	 * | Immediate | `delaySec` ≤ 0 or undefined | `RPUSH` | `ready` list | Job ready for immediate processing. |
	 * | Delayed | `delaySec` > 0 | `ZADD` (score = now + delaySec) | `delayed` ZSET | Job scheduled for later execution. |
	 *
	 * ---
	 * ### Typical lifecycle
	 * 1. The producer calls `enqueue()` with a payload.  
	 * 2. If delayed, the item sits in the ZSET until `now ≥ score`.  
	 * 3. The worker’s {@link promoteDelayed} promotes it into the ready list.  
	 * 4. The worker {@link reserveMany} then picks it up for processing.
	 *
	 * ---
	 * Performance:
	 * - `RPUSH` and `ZADD` are both O(1) amortized operations.  
	 * - Enqueueing tens of thousands of tasks per second is typically safe.
	 * - Avoid excessive serialization overhead in `toPayload(payload)`.
	 *
	 * Complexity:
	 * Time: `O(1)` per item (both branches)  
	 * Space: proportional to payload size stored in Redis.
	 *
	 * Security:
	 * - This method does not validate payload content, only structure.  
	 * - Avoid untrusted raw strings that might interfere with deserialization in consumers.  
	 * - Keys (`ready`, `delayed`) should always be built using {@link toKeyString} to
	 *   maintain proper namespacing and safety.
	 *
	 * @param ready - Redis **LIST** key that represents the queue’s “ready-to-run” list.  
	 *   Typically built via {@link readyKey}.
	 * @param delayed - Redis **ZSET** key used for **delayed** (scheduled) jobs.  
	 *   Typically built via {@link delayedKey}.
	 * @param payload - The data or task object to be serialized and queued.  
	 *   It can be any JSON-compatible object; internally converted to string.
	 * @param delaySec - Optional delay (in **seconds**) before the job becomes active.  
	 *   If > 0, the job goes to `delayed`; otherwise to `ready`.  
	 *   {Default: 0 (immediate enqueue)}
	 *
	 * @returns
	 * A promise resolving to the result of the Redis write operation:
	 * - For `RPUSH`, the **new length** of the ready list.  
	 * - For `ZADD`, the **number of new elements added** (0 or 1).
	 *
	 * @throws
	 * - `"Redis connection error."` if the client is disconnected.  
	 * - Any error thrown by the underlying Redis client (`ZADD` or `RPUSH`).
	 *
	 * @example
	 * ```ts
	 * // Example 1: Immediate enqueue
	 * const ready = this.readyKey('images');
	 * const delayed = this.delayedKey('images');
	 * await this.enqueue(ready, delayed, { id: 'img123', resize: true });
	 * // The job is now instantly available to workers.
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example 2: Delayed enqueue (run after 10 seconds)
	 * const ready = this.readyKey('emails');
	 * const delayed = this.delayedKey('emails');
	 * await this.enqueue(ready, delayed, { userId: 42, type: 'welcome' }, 10);
	 * // The job will appear in the ready list once its scheduled time arrives.
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example 3: Scheduling a chain of tasks dynamically
	 * const payload = { step: 'compress', source: '/tmp/a.jpg' };
	 * await this.enqueue(
	 *   this.readyKey('media'),
	 *   this.delayedKey('media'),
	 *   payload,
	 *   Math.random() * 60, // random delay up to 60s
	 * );
	 * ```
	 *
	 * @since 2.0.0
	 * @category Queue Production
	 * @see {@link promoteDelayed} - Moves due delayed jobs into the ready list.
	 * @see {@link toPayload} - Serializes the provided payload into a Redis-storable string.
	 * @see {@link reserveMany} - Consumes ready jobs for processing.
	 * @see {@link readyKey}, {@link delayedKey} - Helpers for constructing valid key names.
	 */
	async enqueue(ready: string, delayed: string, payload: any, delaySec?: number): Promise<number> {
		if (!this.checkConnection()) {
			throw new Error('Redis connection error.');
		}
		const raw = this.toPayload(payload);

		if (isNumP(delaySec) && delaySec > 0) {
			const score = this.nowSec() + delaySec;

			return await (this.redis as any)?.zadd(delayed, score, raw);
		}
		return await (this.redis as any)?.rpush(ready, raw);
	}

	/**
	 * @summary
	 * Extends a task’s **visibility timeout (VT)** by updating its deadline
	 * in the queue’s **processing VT ZSET** (score = `now + visibilitySec`).
	 *
	 * @remarks
	 * When a task is reserved, its serialized payload (**raw**) is inserted into
	 * the `processing:vt` **ZSET** with a score equal to the timestamp at which its
	 * visibility expires. If the worker needs more time to finish the task,
	 * the VT must be **renewed** to prevent the job from being considered expired
	 * and **requeued** by {@link requeueExpired}.
	 *
	 * This method computes a new deadline as:
	 * ```
	 * deadline = nowSec() + max(1, visibilitySec)
	 * ```
	 * and writes it using {@link zaddCompatXXCH}, a compatibility helper that tries:
	 *
	 * - `ZADD key XX CH score member` (preferred modern form),
	 * - then `ZADD key CH XX score member` (some clients swap argument order),
	 * - finally falls back to legacy `ZADD key score member` (no `XX`/`CH`).
	 *
	 * **Semantics**
	 * - The ZSET **member** is the exact `raw` payload string reserved earlier.  
	 * - The ZSET **score** is the UNIX timestamp (seconds) when the item expires.  
	 * - Renewing the score postpones expiration, keeping the item **invisible** to other workers.
	 *
	 * **Typical usage**
	 * - Called periodically by {@link startHeartbeat} (every ~40% of VT) so the deadline
	 *   is extended well before expiry.  
	 * - Can also be invoked manually if a long-running step is anticipated.
	 *
	 * Performance:
	 * - `ZADD ... XX` is `O(log n)`; a single write per heartbeat interval is inexpensive.  
	 * - Using a heartbeat period shorter than the VT (e.g., 30–50%) provides a safety buffer
	 *   against transient stalls and GC pauses.
	 *
	 * Complexity:
	 * Time: `O(log n)` per update (ZSET reindex)  
	 * Space: `O(1)` (overwrites the member’s score)
	 *
	 * Security:
	 * - `processingVt` should be produced by {@link processingVtKey} to ensure safe namespacing.  
	 * - `raw` must be the exact string stored during reservation; do not pass untrusted strings.
	 *
	 * Notes:
	 * - A minimum of **1 second** is enforced for `visibilitySec` to avoid immediate expiry.  
	 * - The compatibility helper swallows transient client differences; if all attempts fail,
	 *   the VT may not be extended, and the item could expire and be requeued by
	 *   {@link requeueExpired}. The method itself is resilient and typically does not throw.
	 *
	 * @param processingVt - Redis **ZSET** key that tracks visibility deadlines for in-flight items.
	 * @param raw - The exact serialized task payload (ZSET member) whose VT should be extended.
	 * @param visibilitySec - Desired VT extension **in seconds**; clamped to a minimum of `1`.
	 *
	 * @returns
	 * `Promise<void>` - resolves after attempting to update the deadline.
	 *
	 * @example
	 * ```ts
	 * // Manual VT extension (e.g., before a long step):
	 * await this.extendVisibility(
	 *   this.processingVtKey(task.queueName),
	 *   this.processingRaw.get(task.id)!,
	 *   120, // extend by 2 minutes
	 * );
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Automatic usage inside the heartbeat:
	 * // (startHeartbeat schedules calls to extendVisibility at safe intervals)
	 * this.startHeartbeat(task);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Time & Visibility
	 * @see {@link startHeartbeat} - Periodically calls this to keep tasks invisible.
	 * @see {@link requeueExpired} - Requeues tasks whose VT has elapsed.
	 * @see {@link zaddCompatXXCH} - Compatibility wrapper for robust `ZADD` usage.
	 */
	async extendVisibility(processingVt: string, raw: string, visibilitySec: number): Promise<void> {
		const deadline = this.nowSec() + Math.max(1, visibilitySec);

		await this.zaddCompatXXCH(processingVt, deadline, raw);
	}

	/**
	 * @summary
	 * Starts a **worker loop** for the given queue if it isn’t already running.
	 *
	 * @remarks
	 * This method is the public entry point to begin processing a queue. It:
	 *
	 * 1) Validates `queueName` (must be a non-empty string).  
	 * 2) Looks up (or initializes) an internal runner flag for that queue.  
	 * 3) Prevents concurrent starts (throws if already running).  
	 * 4) Marks the queue as running and **fire-and-forgets** the main {@link loop}
	 *    (any top-level error from `loop()` is caught and flips the `running` flag to `false`).
	 *
	 * The loop itself repeatedly:
	 * - Promotes due delayed jobs ({@link promoteDelayed}),  
	 * - Requeues expired processing jobs ({@link requeueExpired}),  
	 * - Reserves a batch from the ready list and executes them with concurrency controls
	 *   (see {@link iteration} and {@link reserveMany}).
	 *
	 * Use {@link stop} to request a graceful stop. Stopping sets the `running` flag to `false`,
	 * allowing the current iteration to finish naturally.
	 *
	 * Performance:
	 * - `run()` is O(1); the heavy work is in the background loop.  
	 * - The design supports multiple queues; each queue has its own runner flag.
	 *
	 * Complexity:
	 * - Time/space: O(1) for the `run()` call. Ongoing complexity is handled by {@link loop}.
	 *
	 * Security:
	 * - `queueName` should be a safe identifier (validated internally).  
	 * - Keys derived from `queueName` are built via {@link toKeyString} to enforce namespacing.
	 *
	 * Notes:
	 * - The method intentionally does **not** `await` the loop; it starts asynchronously.  
	 * - If `loop()` throws at the top level, the `.catch()` handler resets `running=false`
	 *   so a subsequent `run()` call can restart the queue.
	 *
	 * @param queueName - Logical name of the queue to start (e.g., `"emails"`).
	 *
	 * @returns
	 * `void` - the processing loop is started asynchronously as a side effect.
	 *
	 * @throws
	 * - `"Queue name is not valid."` if `queueName` is empty/invalid.  
	 * - `"Queue "<name>" already started."` if a runner for this queue is already active.
	 *
	 * @example
	 * ```ts
	 * class EmailWorker extends PowerQueue {
	 *   async execute(task: Task) {
	 *     // ... send email ...
	 *     return { sent: true };
	 *   }
	 * }
	 *
	 * const worker = new EmailWorker(redis);
	 * worker.run('emails'); // starts the processing loop
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Later, to stop the queue gracefully:
	 * worker.stop('emails'); // loop will exit after the current iteration
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle
	 * @see {@link stop} - Signals the loop to stop.
	 * @see {@link loop} - The main iteration loop invoked here.
	 * @see {@link iteration} - Executes tasks with bounded concurrency.
	 * @see {@link reserveMany} - Reserves items from the ready list each iteration.
	 */
	run(queueName: string) {
		if (!isStrFilled(queueName)) {
			throw new Error('Queue name is not valid.');
		}
		const r = this.runners.get(queueName) ?? { running: false };

		if (r.running) {
			throw new Error(`Queue "${queueName}" already started.`);
		}
		r.running = true;

		this.runners.set(queueName, r);
		this.loop(queueName, r).catch(() => { r.running = false; });
	}

	/**
	 * @summary
	 * Gracefully **stops a running worker loop** for the specified queue.
	 *
	 * @remarks
	 * This method signals an active queue loop (started via {@link run})
	 * to **terminate gracefully**. It:
	 *
	 * 1. Retrieves the runner record from the internal {@link runners} map.  
	 * 2. If found, sets `running = false` to instruct the background {@link loop}
	 *    to stop after completing its current iteration.  
	 * 3. Removes the queue’s runner entry from the map to free memory and
	 *    allow a subsequent {@link run} call to restart it cleanly.
	 *
	 * The stop is cooperative - the active loop will exit on its next cycle
	 * when it checks the `running` flag. No immediate termination or forced
	 * cancellation occurs.
	 *
	 * ---
	 * ### Behavior summary
	 * | Step | Action |
	 * |------|--------|
	 * | 1 | Set `r.running = false` to signal shutdown. |
	 * | 2 | Remove queue from `runners` map. |
	 * | 3 | Current iteration completes; future iterations stop. |
	 *
	 * ---
	 * ### Why it matters
	 * - Allows **safe shutdown** of background workers without corrupting Redis state.  
	 * - Prevents concurrent queue restarts (a stopped queue can be restarted with {@link run}).  
	 * - Avoids dangling timers or intervals created by {@link startHeartbeat}.
	 *
	 * Performance:
	 * - O(1) operation; no Redis calls involved.  
	 * - The loop may take up to one full iteration cycle (up to `iterationTimeout`) to exit.
	 *
	 * Complexity:
	 * - Time: `O(1)`  
	 * - Space: `O(1)` (removes one entry from `runners`)
	 *
	 * Security:
	 * - This function only affects local worker state, not Redis data.  
	 * - It’s safe to call repeatedly; if the queue isn’t running, it silently no-ops.
	 *
	 * Notes:
	 * - Calling `stop()` multiple times on the same queue is harmless.  
	 * - To confirm the stop, you can check whether {@link runners}.has(queueName)
	 *   returns `false` after invocation.
	 * - Typically used during application shutdown or maintenance.
	 *
	 * @param queueName - The logical name of the queue to stop (e.g., `"emails"`).
	 *
	 * @returns
	 * `void` - the method completes immediately after signaling stop.
	 *
	 * @example
	 * ```ts
	 * // Start the queue
	 * worker.run('notifications');
	 *
	 * // Later, during shutdown:
	 * worker.stop('notifications');
	 * console.log('Queue stopped gracefully.');
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Safe to call even if not running - no error thrown:
	 * worker.stop('nonexistent-queue');
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle
	 * @see {@link run} - Starts the background processing loop.
	 * @see {@link loop} - Reads the `running` flag each iteration and exits when false.
	 * @see {@link runners} - Internal map tracking currently active queue loops.
	 */
	stop(queueName: string) {
		const r = this.runners.get(queueName);

		if (r) {
			r.running = false;
			this.runners.delete(queueName);
		}
	}

	/**
	 * @summary
	 * Constructs a **normalized, fully valid `Task` object** from partial input,
	 * filling in all required fields with safe defaults.
	 *
	 * @remarks
	 * This method takes a possibly incomplete `Partial<Task>` structure and produces
	 * a ready-to-use `Task` instance with guaranteed type safety and well-defined
	 * metadata. It is typically used by {@link addTask} before enqueuing jobs into Redis.
	 *
	 * The function:
	 * 1. Validates the input structure and `queueName`.  
	 * 2. Generates unique `uuid()` values for missing `id` and `iterationId`.  
	 * 3. Ensures numerical and object fields have correct default values.  
	 * 4. Initializes `progress` and `chain` substructures to track execution lifecycle.  
	 *
	 * ### Field initialization logic
	 * | Field | Source | Default / Behavior |
	 * |--------|---------|--------------------|
	 * | `queueName` | Required | Throws if missing or invalid. |
	 * | `iterationId` | `data.iterationId` or new `uuid()` | Groups all related subtasks in a batch run. |
	 * | `iterationLength` | `Number(data.iterationLength)` | Defaults to 0 if absent. |
	 * | `id` | `data.id` or new `uuid()` | Unique job identifier. |
	 * | `maxAttempts` | `data.maxAttempts` or `this.maxAttempts` | Max retry count before permanent fail. |
	 * | `currentAttempt` | `data.currentAttempt` or 0 | Starts from 0 for new jobs. |
	 * | `chain` | `data.chain` if valid (`queues[]`, `index`) | Defaults to `{ queues: [], index: 0 }`. |
	 * | `payload` | `data.payload` or `{}` | User-provided job content. |
	 * | `progress` | merged from `data.progress` | Tracks timing and retry metadata. |
	 * | `result` | `data.result` or `{}` | Result container for execution outputs. |
	 *
	 * ### Example of resulting structure
	 * ```ts
	 * {
	 *   queueName: 'emails',
	 *   iterationId: 'b23f1a...uuid',
	 *   iterationLength: 100,
	 *   id: 'c93e4a...uuid',
	 *   maxAttempts: 3,
	 *   currentAttempt: 0,
	 *   chain: { queues: ['emails:send', 'emails:log'], index: 0 },
	 *   payload: { userId: 42, subject: 'Welcome!' },
	 *   progress: {
	 *     createdAt: 1730000000000,
	 *     successAt: 0,
	 *     errorAt: 0,
	 *     failAt: 0,
	 *     fatalAt: 0,
	 *     retries: [],
	 *     chain: []
	 *   },
	 *   result: {}
	 * }
	 * ```
	 *
	 * Notes:
	 * - The function uses runtime guards from `full-utils` (e.g., `isStrFilled`, `isNumPZ`)  
	 *   to ensure data consistency and prevent malformed tasks from entering Redis.  
	 * - Timestamps in `progress` use milliseconds (`Date.now()`), unlike Redis timestamps,
	 *   which use seconds (see {@link nowSec}).
	 * - The `chain` mechanism supports queue chaining; see {@link onChainSuccess}.
	 *
	 * Performance:
	 * - Purely in-memory and synchronous; negligible cost.  
	 * - Safe to call thousands of times per second when enqueueing tasks.
	 *
	 * Complexity:
	 * - Time: `O(1)` (simple property normalization)  
	 * - Space: proportional to payload + metadata (~0.5 KB typical)
	 *
	 * Security:
	 * - No direct Redis interaction; safe for untrusted payloads (they are serialized later).  
	 * - Still, avoid passing non-serializable objects in `payload`.
	 *
	 * @param data - Partial `Task` input containing any subset of fields.  
	 *   Must include a valid `queueName`. Other fields are optional.
	 *
	 * @returns
	 * A complete, normalized {@link Task} object ready to enqueue or process.
	 *
	 * @throws
	 * - `"Data property is not valid."` if input is not a non-empty object.  
	 * - `"Queue name is not valid."` if `queueName` is missing or empty.
	 *
	 * @example
	 * ```ts
	 * // Minimal example: queue name only
	 * const t = this.buildTask({ queueName: 'emails' });
	 * console.log(t.id, t.queueName);
	 * // → automatically assigned UUIDs, safe defaults
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example with custom retry and payload
	 * const task = this.buildTask({
	 *   queueName: 'resize',
	 *   maxAttempts: 5,
	 *   payload: { src: '/tmp/img.jpg', width: 400 },
	 * });
	 * await this.addTask(task); // enqueue to Redis
	 * ```
	 *
	 * @since 2.0.0
	 * @category Task Construction
	 * @see {@link addTask} - Wraps this function to enqueue tasks.
	 * @see {@link Task} - Full type definition.
	 * @see {@link TaskChain}, {@link TaskProgress} - Substructures initialized here.
	 */
	buildTask(data: Partial<Task>): Task {
		if (!isObjFilled(data)) {
			throw new Error('Data property is not valid.');
		}
		if (!isStrFilled(data.queueName)) {
			throw new Error('Queue name is not valid.');
		}
		return {
			queueName: data.queueName,
			iterationId: isStrFilled(data.iterationId)
				? data.iterationId
				: uuid(),
			iterationLength: Number(data.iterationLength || 0),
			id: isStrFilled(data.id)
				? data.id
				: uuid(),
			maxAttempts: isNumPZ(data.maxAttempts)
				? data.maxAttempts
				: this.maxAttempts,
			currentAttempt: isNumPZ(data.currentAttempt)
				? data.currentAttempt
				: 0,
			chain: (((isObjFilled(data.chain)
				&& isArrFilled(data.chain.queues)
				&& isNumPZ(data.chain.index))
				? data.chain
				: { 
					queues: [], 
					index: 0 
				}) as TaskChain),
			payload: isObjFilled(data.payload)
				? data.payload
				: {},
			progress: ({
				createdAt: Date.now(),
				successAt: 0,
				errorAt: 0,
				failAt: 0,
				fatalAt: 0,
				retries: [] as number[],
				chain: [] as number[],
				...(isObjFilled(data.progress)
					? data.progress
					: {}),
			} as TaskProgress),
			result: isObjFilled(data.result)
				? data.result
				: {},
		};
	}

	/**
	 * @summary
	 * High-level helper to **create** a normalized {@link Task} from partial input
	 * and **enqueue** it to the appropriate Redis structure (ready list or delayed ZSET).
	 *
	 * @remarks
	 * This convenience method wraps two core responsibilities:
	 *
	 * 1) **Task construction** - calls {@link buildTask} to validate and complete
	 *    the provided `Partial<Task>` (generates UUIDs, fills defaults, normalizes sub-objects).  
	 * 2) **Enqueueing** - computes queue keys via {@link readyKey}/{@link delayedKey} and
	 *    delegates the actual write to {@link enqueue}, which:
	 *    - `RPUSH`es to the **ready** LIST for immediate processing, or
	 *    - `ZADD`s to the **delayed** ZSET if `delaySec > 0` (score = `now + delaySec`).
	 *
	 * This is the typical entry point used by producers to submit work to a queue.
	 *
	 * ---
	 * ### Behavior summary
	 * | Input | Destination | Redis command | Notes |
	 * |------|--------------|---------------|-------|
	 * | `delaySec` ≤ 0 or not a positive number | `ready` (LIST) | `RPUSH` | Job is available immediately. |
	 * | `delaySec` > 0 | `delayed` (ZSET) | `ZADD` | Scheduled; later promoted by {@link promoteDelayed}. |
	 *
	 * Keys are derived from `data.queueName` using {@link readyKey} / {@link delayedKey},
	 * which in turn rely on {@link toKeyString} for strict, safe namespacing.
	 *
	 * Performance:
	 * - Minimal overhead: `buildTask` is in-memory; `enqueue` is a single Redis call.  
	 * - Suitable for high-throughput producers; consider batching at the application level if needed.
	 *
	 * Complexity:
	 * - Time: `O(1)` (serialize + one Redis write).  
	 * - Space: proportional to payload size stored in Redis.
	 *
	 * Security:
	 * - Validates structure via {@link buildTask}; payload content is not sanitized here.  
	 * - Always use safe queue names; `readyKey`/`delayedKey` ensure namespacing.
	 *
	 * @param data - Partial task description. Must contain a valid `queueName`; the rest is defaulted by {@link buildTask}.
	 * @param delaySec - Optional delay in **seconds** before the task is eligible to run. If not a positive number, the task is enqueued immediately.  
	 *   {Default: 0 (immediate)}
	 *
	 * @returns
	 * A promise resolving to the numeric result of the underlying Redis write:  
	 * - `RPUSH` → new length of the ready LIST,  
	 * - `ZADD`  → number of elements added to the delayed ZSET (0 or 1).
	 *
	 * @throws
	 * - Any error thrown by {@link buildTask} (e.g., `"Queue name is not valid."`).  
	 * - Any error thrown by {@link enqueue} / Redis client calls.  
	 * - `"Redis connection error."` if the client is disconnected (propagated from {@link enqueue}).
	 *
	 * @example
	 * ```ts
	 * // Immediate enqueue (no delay)
	 * await this.addTask({
	 *   queueName: 'emails',
	 *   payload: { to: 'user@example.com', subject: 'Hello' },
	 *   maxAttempts: 3,
	 * });
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Schedule a task to run 30 seconds later
	 * await this.addTask(
	 *   { queueName: 'reports', payload: { reportId: 'r-123' } },
	 *   30
	 * );
	 * ```
	 *
	 * @since 2.0.0
	 * @category Queue Production
	 * @see {@link buildTask} - Normalizes and validates the Task.
	 * @see {@link enqueue} - Performs the actual Redis write (RPUSH/ZADD).
	 * @see {@link promoteDelayed} - Later moves due delayed jobs to the ready list.
	 * @see {@link readyKey} | {@link delayedKey} - Construct the Redis keys from the queue name.
	 */
	async addTask(data: Partial<Task>, delaySec?: number): Promise<number> {
		const ready = this.readyKey(String(data.queueName));
		const delayed = this.delayedKey(String(data.queueName));

		return await this.enqueue(ready, delayed, this.buildTask(data), isNumP(delaySec) ? delaySec : 0);
	}

	/**
	 * @summary
	 * Executes a **single processing iteration** over a batch of tasks with
	 * **bounded concurrency**, applying pre/post iteration hooks.
	 *
	 * @remarks
	 * The iteration pipeline is:
	 *
	 * 1) **Pre-hook** - {@link beforeIterationExecution} can filter, reorder, enrich,
	 *    or otherwise transform the incoming `tasks`. The returned array becomes
	 *    `tasksProcessed`.
	 * 2) **Concurrent execution** - Tasks are split into slices of size
	 *    {@link concurrency}. Each slice is executed in parallel via `Promise.all`,
	 *    where each task runs through the internal {@link logic} method
	 *    (which calls {@link beforeExecution} → {@link execute} → {@link afterExecution}
	 *    and handles success/retry/failure, heartbeat, and ACK).
	 * 3) **Post-hook** - {@link afterIterationExecution} receives the final
	 *    `tasksProcessed` and a parallel array of their `result` objects for
	 *    aggregate reporting, metrics, or cleanup.
	 *
	 * The method returns only when all slices in the current iteration complete.
	 * It does **not** loop forever; the outer scheduling is handled by {@link loop}.
	 *
	 * Performance:
	 * - Concurrency is capped by {@link concurrency}; default is `32`.  
	 * - Using slices avoids spawning unbounded promises and keeps memory predictable.  
	 * - Heavy work should live inside {@link execute}; this wrapper adds minimal overhead.
	 *
	 * Complexity:
	 * - Time: roughly `O(n)` for `n = tasksProcessed.length`, with per-task cost
	 *   dominated by your business logic in {@link execute}.  
	 * - Space: `O(n)` for the tasks array and their results. Concurrency limits
	 *   peak in-flight work to `concurrency`.
	 *
	 * Security:
	 * - Task payloads are user/domain data; validate inside {@link execute}.  
	 * - Hooks can sanitize/normalize input or enforce policies before running tasks.
	 *
	 * @param tasks - The list of tasks reserved for this iteration (typically from {@link data}).
	 *   They are first passed to {@link beforeIterationExecution} which may change the list.
	 *
	 * @returns
	 * `Promise<void>` - resolves after all tasks in this iteration have been processed
	 * (including hooks), regardless of individual task outcomes (success/retry/fail).
	 *
	 * @throws
	 * This method itself does not throw for individual task errors; those are handled
	 * inside {@link logic}. It can throw if hooks reject (uncaught in this method),
	 * but the surrounding {@link loop} places calls in try/catch and invokes
	 * {@link onIterationError} on failures.
	 *
	 * @example
	 * ```ts
	 * // Inside the worker loop, after reserving tasks:
	 * const tasks = await this.data('emails');
	 * if (tasks.length > 0) {
	 *   await this.iteration(tasks); // runs tasks with concurrency limits
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Customize hooks to sort tasks by priority before execution
	 * async beforeIterationExecution(tasks: Task[]): Promise<Task[]> {
	 *   return tasks.sort((a, b) => (b.payload.priority ?? 0) - (a.payload.priority ?? 0));
	 * }
	 *
	 * async afterIterationExecution(tasks: Task[], results: TaskResult[]): Promise<void> {
	 *   // Aggregate metrics, log summary, emit events, etc.
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Execution Loop
	 * @see {@link concurrency} - Maximum tasks processed in parallel.
	 * @see {@link logic} - Per-task execution flow (success, retry, ack, heartbeat).
	 * @see {@link beforeIterationExecution} | {@link afterIterationExecution} - Iteration hooks.
	 * @see {@link loop} - Calls this method repeatedly as part of the worker lifecycle.
	 */
	async iteration(tasks: Array<Task>): Promise<void> {
		const tasksProcessed = await this.beforeIterationExecution(tasks);
		const limit = Math.max(1, Number(this.concurrency) || 1);
		let i = 0;

		while (i < tasksProcessed.length) {
			const slice = tasksProcessed.slice(i, i + limit);

			await Promise.all(slice.map((task: Task) => this.logic(task)));
			i += limit;
		}
		await this.afterIterationExecution(tasksProcessed, tasksProcessed.map((t) => t.result ?? {}));
	}

	/**
	 * @summary
	 * Lifecycle **hook** executed **before** an iteration of tasks begins.
	 *
	 * @remarks
	 * This method allows subclasses to **inspect, transform, filter, or reorder**
	 * the list of tasks right before they are processed by {@link iteration}.
	 *
	 * By default, it simply returns the same array (`data`) unchanged, but you can
	 * override it in your subclass to implement pre-processing logic such as:
	 * - Filtering out tasks that do not meet certain conditions.
	 * - Sorting tasks by priority or timestamp.
	 * - Enriching tasks with additional metadata (e.g., configuration or context).
	 * - Grouping or deduplicating jobs within the batch.
	 *
	 * ### Typical use cases
	 * - **Prioritization**: sort by a priority field before execution.  
	 * - **Deduplication**: remove tasks with identical payloads.  
	 * - **Conditional skipping**: drop tasks that are outdated or malformed.  
	 * - **Instrumentation**: record metrics about the batch before running.
	 *
	 * Performance:
	 * - O(n) in the number of tasks (linear pass if you modify/filter them).  
	 * - Usually negligible overhead; keep transformations synchronous or fast.
	 *
	 * Complexity:
	 * - Time: depends on your transformation (default O(1)).  
	 * - Space: O(n) if returning a new array; O(1) if modifying in place.
	 *
	 * Security:
	 * - You can enforce access control or payload validation here before tasks reach {@link execute}.  
	 * - Never mutate critical IDs (`id`, `iterationId`, etc.) unless you understand the implications.
	 *
	 * @param data - The list of {@link Task} objects reserved for this iteration.
	 *
	 * @returns
	 * A promise resolving to the array of tasks to actually execute during this iteration.
	 * The returned array replaces the input for the subsequent processing stage.
	 *
	 * @example
	 * ```ts
	 * // Example: filter out tasks missing a required field
	 * async beforeIterationExecution(tasks: Task[]): Promise<Task[]> {
	 *   return tasks.filter(t => typeof t.payload?.userId === 'number');
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: sort by custom priority value before execution
	 * async beforeIterationExecution(tasks: Task[]): Promise<Task[]> {
	 *   return tasks.sort((a, b) => (b.payload.priority ?? 0) - (a.payload.priority ?? 0));
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default implementation (no modification)
	 * async beforeIterationExecution(tasks: Task[]): Promise<Task[]> {
	 *   return tasks;
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link iteration} - Uses this method at the beginning of each processing cycle.
	 * @see {@link afterIterationExecution} - Complementary hook called after all tasks complete.
	 */
	async beforeIterationExecution(data: Array<Task>): Promise<Array<Task>> {
		return data;
	}

	/**
	 * @summary
	 * Lifecycle **hook** executed **after** an iteration of tasks completes —
	 * regardless of success, retry, or failure outcomes.
	 *
	 * @remarks
	 * This hook runs after {@link iteration} finishes executing all tasks in the current
	 * batch. It receives both:
	 * - `data`: the array of all {@link Task} objects that were processed, and  
	 * - `results`: a parallel array of their corresponding {@link TaskResult} outputs.
	 *
	 * The arrays are **positionally aligned**, meaning that `results[i]` corresponds
	 * to `data[i]`. Both arrays are guaranteed to have the same length.
	 *
	 * You can override this method to implement post-processing logic such as:
	 * - Aggregating task results or metrics (e.g., success/failure counts).
	 * - Writing audit logs or analytics to external systems.
	 * - Emitting application-level events or WebSocket updates.
	 * - Cleaning up temporary resources, cache entries, or temporary files.
	 *
	 * The default implementation does nothing.
	 *
	 * ---
	 * ### Example workflow
	 * ```text
	 * beforeIterationExecution() → logic() (per-task)
	 *                           ↳ afterIterationExecution()
	 * ```
	 *
	 * ---
	 * ### Typical use cases
	 * | Use case | Example |
	 * |-----------|----------|
	 * | **Metrics aggregation** | Count how many tasks succeeded or failed. |
	 * | **Batch logging** | Store all results into a single log record or DB entry. |
	 * | **Progress tracking** | Update global progress counters in Redis or ClickHouse. |
	 * | **Notification** | Send webhook or message once a batch completes. |
	 *
	 * Performance:
	 * - Called once per iteration, not per task - safe for expensive operations.  
	 * - Runs *after* all task promises in {@link iteration} have resolved.
	 *
	 * Complexity:
	 * - Time: O(n) in number of tasks if you scan through results.  
	 * - Space: O(n) due to result arrays.
	 *
	 * Security:
	 * - Avoid logging full payloads or results if they may contain sensitive data.  
	 * - Use aggregation or masking when exporting data externally.
	 *
	 * @param data - Array of {@link Task} objects that were processed in this iteration.
	 * @param results - Array of corresponding {@link TaskResult} objects returned by each task.
	 *
	 * @returns
	 * A `Promise<void>` resolved when post-processing is complete.
	 *
	 * @example
	 * ```ts
	 * // Example: Log iteration summary
	 * async afterIterationExecution(tasks: Task[], results: TaskResult[]): Promise<void> {
	 *   const total = tasks.length;
	 *   const success = results.filter(r => r.status === 'ok').length;
	 *   console.log(`[Iteration] ${success}/${total} tasks succeeded`);
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: Send aggregated metrics to Redis
	 * async afterIterationExecution(tasks: Task[], results: TaskResult[]): Promise<void> {
	 *   const successCount = results.filter(r => r.success === true).length;
	 *   await this.redis.incrby('queue:iteration:success', successCount);
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link iteration} - Calls this method after concurrent batch execution.
	 * @see {@link beforeIterationExecution} - Prepares the batch before this hook.
	 */
	async afterIterationExecution(data: Array<Task>, results: Array<TaskResult>): Promise<void> {
	}

	/**
	 * @summary
	 * Lifecycle **hook** executed **before** an individual task begins processing.
	 *
	 * @remarks
	 * This hook allows you to **inspect, enrich, or modify** a task right before it is
	 * executed by the main business logic in {@link execute}.  
	 * It runs once per task, immediately before its {@link execute} method is called.
	 *
	 * The default implementation simply returns the same `task` unchanged, but subclasses
	 * can override this method to:
	 * - Add contextual metadata (e.g., request IDs, trace tokens, timestamps).
	 * - Validate or sanitize the task’s payload.
	 * - Transform the task into a richer form required by the business logic.
	 * - Short-circuit certain tasks by marking them as already handled.
	 *
	 * This hook runs **inside the execution pipeline** managed by {@link logic}, so any
	 * exceptions thrown here will be caught and handled through the retry/error chain.
	 *
	 * ---
	 * ### Execution flow
	 * ```text
	 * beforeExecution(task)
	 *    ↓
	 * execute(task)
	 *    ↓
	 * afterExecution(task, result)
	 * ```
	 *
	 * ---
	 * ### Typical use cases
	 * | Purpose | Example |
	 * |----------|----------|
	 * | **Enrichment** | Add a timestamp or context key before processing. |
	 * | **Validation** | Throw if payload lacks required fields. |
	 * | **Normalization** | Adjust data types or default values. |
	 * | **Debugging** | Log the task ID or payload before execution. |
	 *
	 * Performance:
	 * - Runs once per task; keep logic lightweight to avoid per-task overhead.
	 * - Avoid network or heavy I/O here; prefer inside {@link execute}.
	 *
	 * Complexity:
	 * - Time: O(1) per task (default).  
	 * - Space: O(1) if not cloning; O(n) only if duplicating payloads.
	 *
	 * Security:
	 * - A good place to enforce type validation, input filtering, or security checks
	 *   before untrusted data is processed in {@link execute}.
	 *
	 * @param task - The task object about to be executed.
	 *   Contains metadata, payload, retry counters, and progress info.
	 *
	 * @returns
	 * A `Promise` resolving to either the original or a modified {@link Task}
	 * that will be passed into {@link execute}.
	 *
	 * @example
	 * ```ts
	 * // Example: Enrich the task with a correlation ID
	 * async beforeExecution(task: Task): Promise<Task> {
	 *   return {
	 *     ...task,
	 *     payload: { ...task.payload, correlationId: uuid() },
	 *   };
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: Validate payload structure before execution
	 * async beforeExecution(task: Task): Promise<Task> {
	 *   if (typeof task.payload?.userId !== 'number') {
	 *     throw new Error('Invalid task payload: missing userId');
	 *   }
	 *   return task;
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default implementation (no changes)
	 * async beforeExecution(task: Task): Promise<Task> {
	 *   return task;
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link execute} - Called immediately after this hook.
	 * @see {@link afterExecution} - Runs after the main execution completes.
	 * @see {@link logic} - Internal pipeline controlling per-task execution.
	 */
	async beforeExecution(task: Task): Promise<Task> {
		return task;
	}

	/**
	 * @summary
	 * Lifecycle **hook** executed **after** a task’s main business logic
	 * (the {@link execute} method) has successfully finished.
	 *
	 * @remarks
	 * This hook provides a final opportunity to **post-process or transform**
	 * the result of a completed task before it is recorded and passed to the next
	 * lifecycle phase (such as {@link success} or {@link onSuccess}).
	 *
	 * The default implementation simply returns the same `result` unchanged,
	 * but you can override it to perform operations like:
	 * - Enriching or sanitizing the result before storage or logging.
	 * - Measuring execution duration or recording custom telemetry.
	 * - Adding metadata (e.g., timestamps, node IDs, version info).
	 * - Transforming internal result formats into a canonical structure.
	 *
	 * This method is called **only if** {@link execute} resolved successfully;
	 * exceptions raised inside {@link execute} trigger retry or error handling
	 * and skip this hook.
	 *
	 * ---
	 * ### Execution flow
	 * ```text
	 * beforeExecution(task)
	 *    ↓
	 * execute(task)
	 *    ↓
	 * afterExecution(task, result)
	 * ```
	 *
	 * ---
	 * ### Typical use cases
	 * | Purpose | Example |
	 * |----------|----------|
	 * | **Enrichment** | Add a `completedAt` timestamp to the result. |
	 * | **Normalization** | Convert internal result shape into standard fields. |
	 * | **Telemetry** | Log metrics about task latency or size. |
	 * | **Auditing** | Persist result summaries in external storage. |
	 *
	 * Performance:
	 * - Runs once per successful task, after execution is complete.  
	 * - Keep logic lightweight to avoid blocking subsequent task acknowledgment.
	 *
	 * Complexity:
	 * - Time: O(1) (default).  
	 * - Space: O(1), unless cloning or deeply modifying large result objects.
	 *
	 * Security:
	 * - Use this hook to strip or mask sensitive data before persisting or logging results.  
	 * - Avoid throwing errors; errors here will be caught by the internal logic but may
	 *   prevent result propagation.
	 *
	 * @param task - The {@link Task} that was just executed.
	 * @param result - The {@link TaskResult} produced by {@link execute}.
	 *
	 * @returns
	 * A `Promise` resolving to the final, possibly transformed, {@link TaskResult}
	 * that will be stored or passed onward in the queue lifecycle.
	 *
	 * @example
	 * ```ts
	 * // Example: Add completion timestamp to each result
	 * async afterExecution(task: Task, result: TaskResult): Promise<TaskResult> {
	 *   return { ...result, completedAt: Date.now() };
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: Normalize output field names
	 * async afterExecution(task: Task, result: TaskResult): Promise<TaskResult> {
	 *   return {
	 *     success: Boolean(result.ok),
	 *     details: result.message ?? '',
	 *   };
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default implementation (no transformation)
	 * async afterExecution(task: Task, result: TaskResult): Promise<TaskResult> {
	 *   return result;
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link execute} - Main business logic producing the result.
	 * @see {@link beforeExecution} - Prepares the task before execution.
	 * @see {@link logic} - Internal handler invoking this hook.
	 */
	async afterExecution(task: Task, result: TaskResult): Promise<TaskResult> {
		return result;
	}

	/**
	 * @summary
	 * Core **business logic handler** - executes a single {@link Task}
	 * and returns its computed {@link TaskResult}.
	 *
	 * @remarks
	 * This is the **primary method to override** when extending {@link PowerQueue}.
	 * Each subclass must implement its own version of `execute()` to perform
	 * the actual work associated with a task (e.g., API call, image processing,
	 * database operation, file transformation, etc.).
	 *
	 * The default implementation here simply returns an **empty object** `{}`,
	 * which serves as a no-op placeholder.
	 *
	 * ---
	 * ### Responsibilities
	 * - Receive a deserialized {@link Task} object.  
	 * - Perform synchronous or asynchronous operations based on its payload.  
	 * - Return a serializable {@link TaskResult} - typically an object containing
	 *   summary fields or output data.
	 *
	 * The return value is automatically passed to {@link afterExecution},
	 * {@link onSuccess}, and potentially {@link onChainSuccess} if the task is
	 * part of a chained workflow.
	 *
	 * ---
	 * ### Error handling
	 * - If `execute()` throws or rejects, the error is caught internally by {@link logic},
	 *   and retry/failure hooks (`onRetry`, `onError`, `onFail`, `onFatal`) are triggered.  
	 * - If it resolves successfully, {@link afterExecution} and {@link onSuccess} run.
	 *
	 * ---
	 * ### Best practices
	 * - Always return lightweight, serializable data structures (plain objects).  
	 * - Avoid returning class instances or non-JSON-compatible types.  
	 * - Keep long-running operations cancellable when possible.  
	 * - Handle transient failures gracefully and let the retry system decide
	 *   on rescheduling.
	 *
	 * ---
	 * ### Typical use cases
	 * | Example | Description |
	 * |----------|-------------|
	 * | HTTP worker | Fetch a remote API and return the parsed JSON. |
	 * | Image worker | Resize or compress an image file. |
	 * | Database job | Insert a record and return its ID. |
	 * | Notification sender | Send an email/SMS and return delivery status. |
	 *
	 * Performance:
	 * - Runs concurrently up to {@link concurrency} per iteration.  
	 * - Total throughput depends on operation latency and external resources.
	 *
	 * Complexity:
	 * - Time: depends on custom implementation.  
	 * - Space: proportional to task payload and result data.
	 *
	 * Security:
	 * - Sanitize and validate untrusted task data before executing.  
	 * - Avoid side effects unless the queue guarantees idempotency.  
	 * - Ensure sensitive data in results is masked if persisted.
	 *
	 * @param task - A full {@link Task} object containing all necessary metadata
	 * and the `payload` to be processed.
	 *
	 * @returns
	 * A `Promise` resolving to a {@link TaskResult} - a plain object representing
	 * the result of this operation.  
	 * Returning `{}` is considered “success with no payload”.
	 *
	 * @throws
	 * Any thrown error (synchronous or rejected promise) will trigger retry/error
	 * handling through {@link retry}, {@link onError}, and related hooks.
	 *
	 * @example
	 * ```ts
	 * // Example: simple addition worker
	 * async execute(task: Task): Promise<TaskResult> {
	 *   const { a, b } = task.payload;
	 *   if (typeof a !== 'number' || typeof b !== 'number') {
	 *     throw new Error('Invalid payload');
	 *   }
	 *   return { sum: a + b };
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: HTTP fetch worker
	 * async execute(task: Task): Promise<TaskResult> {
	 *   const res = await fetch(task.payload.url);
	 *   const json = await res.json();
	 *   return { status: res.status, data: json };
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default implementation (no operation)
	 * async execute(task: Task): Promise<TaskResult> {
	 *   return {};
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Core Business Logic
	 * @see {@link beforeExecution} - Called immediately before this hook.
	 * @see {@link afterExecution} - Runs right after this method resolves successfully.
	 * @see {@link logic} - Internal pipeline that calls this function.
	 * @see {@link Task} | {@link TaskResult} - Data structures representing a queue item and its output.
	 */
	async execute(task: Task): Promise<TaskResult> {
		return {};
	}

	/**
	 * @summary
	 * Lifecycle **hook** triggered whenever a task is **scheduled for retry**
	 * after a recoverable failure during execution.
	 *
	 * @remarks
	 * This method is automatically called by {@link retry} whenever a task fails
	 * but has **remaining attempts** left (i.e., `currentAttempt < maxAttempts`).
	 *
	 * You can override this hook to perform custom side effects, such as:
	 * - Logging or monitoring retry attempts.
	 * - Updating external dashboards or alerting systems.
	 * - Recording retry delays or failure reasons for analytics.
	 * - Emitting application events (“task requeued”, “attempt #2 started”, etc.).
	 *
	 * The default implementation does nothing.
	 *
	 * ---
	 * ### Retry workflow
	 * ```text
	 * execute(task) throws Error
	 *     ↓
	 * retry(task) schedules new delayed attempt
	 *     ↓
	 * onRetry(task) (this hook)
	 * ```
	 *
	 * ---
	 * ### Timing & semantics
	 * - Runs **after** the task has been re-enqueued for a future retry.  
	 * - The delay for the next attempt is computed via exponential backoff with jitter
	 *   inside {@link jitteredBackoffSec}.  
	 * - Does **not** block the main worker loop - called asynchronously.
	 *
	 * ---
	 * ### Typical use cases
	 * | Use case | Example |
	 * |-----------|----------|
	 * | **Logging** | Write “Task X retried (#2)” to console or file. |
	 * | **Metrics** | Increment retry counter in Prometheus or Redis. |
	 * | **Debugging** | Store failure context for later inspection. |
	 * | **Notifications** | Send alert when retries exceed a threshold. |
	 *
	 * Performance:
	 * - Runs once per retry attempt, after requeue scheduling completes.  
	 * - Keep logic lightweight and non-blocking (avoid I/O where possible).
	 *
	 * Complexity:
	 * - O(1) (default, no-op).  
	 * - O(n) only if you perform custom batch logging or persistence.
	 *
	 * Security:
	 * - Avoid logging full payloads if they contain sensitive user data.  
	 * - Mask or truncate large payloads in monitoring systems.
	 *
	 * @param task - The {@link Task} that has just been scheduled for retry.
	 *   It includes updated retry counters (`currentAttempt`), payload, and metadata.
	 *
	 * @returns
	 * A `Promise<void>` that resolves after your custom side effect completes.
	 *
	 * @example
	 * ```ts
	 * // Example: log retry attempt with exponential delay
	 * async onRetry(task: Task): Promise<void> {
	 *   console.warn(
	 *     `Task ${task.id} (${task.queueName}) retried - attempt #${task.currentAttempt}`
	 *   );
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: increment Redis counter for retry stats
	 * async onRetry(task: Task): Promise<void> {
	 *   await this.redis.incr(`stats:${task.queueName}:retries`);
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default (no operation)
	 * async onRetry(task: Task): Promise<void> {
	 *   // no-op
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link retry} - Invokes this method when re-enqueuing a failed task.
	 * @see {@link onError} - Called when a task fails immediately without retry.
	 * @see {@link jitteredBackoffSec} - Computes the retry delay duration.
	 */
	async onRetry(task: Task): Promise<void> {
	}

	/**
	 * @summary
	 * Lifecycle **hook** triggered when a task encounters a **recoverable error**
	 * during execution but cannot be immediately retried (e.g., validation or runtime error).
	 *
	 * @remarks
	 * This method is called whenever {@link execute} or its preceding hooks
	 * throw an exception that is **not successfully handled** by {@link retry}.
	 *  
	 * It represents a **non-fatal failure** - meaning the task failed this attempt,
	 * but the worker process itself remains healthy and continues running.
	 *
	 * You can override this hook to implement:
	 * - Error logging (console, file, or external services like Sentry).
	 * - Application-specific error categorization or metrics collection.
	 * - Conditional alerts or diagnostics (e.g., network errors vs logic bugs).
	 * - Integration with distributed tracing or observability tools.
	 *
	 * The default implementation does nothing.
	 *
	 * ---
	 * ### Error propagation flow
	 * ```text
	 * execute(task) → throws Error
	 *     ↓
	 * retry(task) → fails or maxAttempts reached
	 *     ↓
	 * onError(err, task) → this hook
	 * ```
	 *
	 * ---
	 * ### Behavior
	 * - Triggered after retry attempts are exhausted *for this iteration*,
	 *   or when retry scheduling itself fails.
	 * - Does **not** automatically stop the worker or requeue the task —
	 *   that is handled by {@link error} and {@link fail}.
	 * - Executed asynchronously; exceptions thrown here are caught internally.
	 *
	 * ---
	 * ### Typical use cases
	 * | Use case | Example |
	 * |-----------|----------|
	 * | **Logging** | Write error details to console or structured logger. |
	 * | **Monitoring** | Send metric counters for failed attempts. |
	 * | **Alerting** | Notify developers on critical queue errors. |
	 * | **Diagnostics** | Record stack traces for later debugging. |
	 *
	 * Performance:
	 * - Runs once per failed task (after retry/failure resolution).  
	 * - Safe for I/O operations, but prefer batching for high-volume queues.
	 *
	 * Complexity:
	 * - O(1) default; depends on the complexity of your logging/alerting logic.
	 *
	 * Security:
	 * - Avoid logging sensitive or personally identifiable information (PII).  
	 * - Sanitize stack traces or payloads before exporting externally.
	 *
	 * @param err - The {@link Error} object thrown during task execution.  
	 *   Contains message, stack, and possibly additional properties from the failure.
	 * @param task - The {@link Task} instance that caused the error.
	 *   Includes identifying fields (`id`, `queueName`) and payload details.
	 *
	 * @returns
	 * A `Promise<void>` that resolves once your error-handling side effect completes.
	 *
	 * @example
	 * ```ts
	 * // Example: log the error details to console
	 * async onError(err: Error, task: Task): Promise<void> {
	 *   console.error(
	 *     `Task ${task.id} (${task.queueName}) failed: ${err.message}`
	 *   );
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: send error to monitoring system
	 * async onError(err: Error, task: Task): Promise<void> {
	 *   await this.redis.hset(
	 *     `errors:${task.queueName}`,
	 *     task.id,
	 *     JSON.stringify({ message: err.message, time: Date.now() })
	 *   );
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default (no operation)
	 * async onError(err: Error, task: Task): Promise<void> {
	 *   // no-op
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link onFail} - Called when a task reaches max retry limit (permanent failure).
	 * @see {@link retry} - Attempts to reschedule failed tasks before invoking this hook.
	 * @see {@link onFatal} - Handles unrecoverable or internal queue-level errors.
	 */
	async onError(err: Error, task: Task): Promise<void> {
	}

	/**
	 * @summary
	 * Lifecycle **hook** triggered when a task **permanently fails** -  
	 * meaning all retry attempts have been exhausted and it can no longer be requeued.
	 *
	 * @remarks
	 * This method is invoked by {@link fail} once the system determines that
	 * a task has reached its **maximum attempt limit** (`maxAttempts`) and
	 * no further retries are allowed.
	 *
	 * The task is then moved into a dedicated **failure queue**
	 * (`<queueName>:<iterationId>:fail:list`) for inspection or debugging.
	 *
	 * Override this hook to perform custom actions such as:
	 * - Logging or persisting failure details.  
	 * - Alerting operations teams about recurring critical errors.  
	 * - Marking database records as "failed" or "aborted."  
	 * - Sending diagnostic information to external systems (e.g., Sentry, ELK, Prometheus).  
	 *
	 * The default implementation is a no-op.
	 *
	 * ---
	 * ### Failure flow
	 * ```text
	 * execute(task) → throws Error
	 *     ↓
	 * retry(task) → maxAttempts reached
	 *     ↓
	 * fail(err, task)
	 *     ↓
	 * onFail(err, task) → this hook
	 * ```
	 *
	 * ---
	 * ### Behavior
	 * - Runs **after** the task has been permanently classified as failed.  
	 * - The task has already been re-enqueued into a `fail:list` queue for inspection.  
	 * - Execution continues for other tasks; this hook does **not** stop the worker.  
	 * - Exceptions thrown here are caught internally to avoid disrupting the main loop.
	 *
	 * ---
	 * ### Typical use cases
	 * | Purpose | Example |
	 * |----------|----------|
	 * | **Logging** | Record failure message and stack trace. |
	 * | **Monitoring** | Increment “failed job” counters. |
	 * | **Notification** | Alert developers about repeated failures. |
	 * | **Recovery** | Mark corresponding DB entity as failed or expired. |
	 *
	 * Performance:
	 * - Runs once per permanently failed task.  
	 * - Safe for moderate I/O (database writes, logging, etc.).
	 *
	 * Complexity:
	 * - O(1) (default) or O(n) if performing bulk writes.
	 *
	 * Security:
	 * - Be cautious when logging payloads or stack traces; sanitize sensitive data.  
	 * - Use structured logging (JSON) for machine-readable error tracking.
	 *
	 * @param err - The {@link Error} that caused the final failure.  
	 *   It typically includes the message and stack trace of the last thrown exception.
	 * @param task - The {@link Task} object that has permanently failed.
	 *   Includes all metadata such as `queueName`, `id`, `currentAttempt`, and `payload`.
	 *
	 * @returns
	 * A `Promise<void>` resolving once the custom failure handling has completed.
	 *
	 * @example
	 * ```ts
	 * // Example: log failure to console
	 * async onFail(err: Error, task: Task): Promise<void> {
	 *   console.error(
	 *     `Task ${task.id} in queue "${task.queueName}" permanently failed: ${err.message}`
	 *   );
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: save failure details into Redis
	 * async onFail(err: Error, task: Task): Promise<void> {
	 *   await this.redis.hset(
	 *     `failures:${task.queueName}`,
	 *     task.id,
	 *     JSON.stringify({
	 *       message: err.message,
	 *       stack: err.stack,
	 *       attempts: task.currentAttempt,
	 *       time: Date.now(),
	 *     })
	 *   );
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default (no operation)
	 * async onFail(err: Error, task: Task): Promise<void> {
	 *   // no-op
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link retry} - Handles exponential backoff and determines when to fail.
	 * @see {@link onError} - Called for recoverable single-attempt failures.
	 * @see {@link onFatal} - Triggered when a deeper system error occurs.
	 */
	async onFail(err: Error, task: Task): Promise<void> {
	}

	/**
	 * @summary
	 * Lifecycle **hook** triggered when a **fatal or unrecoverable error**
	 * occurs during queue processing - typically an internal logic or infrastructure failure.
	 *
	 * @remarks
	 * This method is invoked by the internal {@link fail} or {@link error} handlers
	 * when an error occurs that **cannot be retried** and indicates a **system-level problem**, not
	 * just a single task failure.
	 *
	 * Examples of fatal errors include:
	 * - Corrupted task payloads that cannot be parsed or validated.
	 * - Redis connection failures or transaction rollbacks.
	 * - Unhandled exceptions inside retry/failure hooks themselves.
	 * - Logic errors or programming bugs in custom worker code.
	 *
	 * You should override this hook to perform **emergency logging, diagnostics, or shutdown actions**, such as:
	 * - Writing a critical log entry to an external monitoring service.  
	 * - Sending an alert (email, Telegram, Slack, etc.) to maintainers.  
	 * - Capturing diagnostic data or triggering an application health alarm.  
	 * - Pausing or restarting the worker service if necessary.
	 *
	 * The default implementation is a no-op.
	 *
	 * ---
	 * ### Fatal error flow
	 * ```text
	 * execute(task) → throws Error
	 *     ↓
	 * retry/task handling fails unexpectedly
	 *     ↓
	 * onFatal(err, task) → this hook (final fallback)
	 * ```
	 *
	 * ---
	 * ### Behavior
	 * - Called only when the system detects that recovery or retry logic
	 *   could not handle the error gracefully.  
	 * - Executed asynchronously, and any exceptions thrown here are silently caught
	 *   to avoid recursive failure loops.  
	 * - Does not stop the worker loop by itself - you can explicitly call {@link stop}
	 *   if needed to halt processing of the affected queue.
	 *
	 * ---
	 * ### Typical use cases
	 * | Use case | Example |
	 * |-----------|----------|
	 * | **Alerting** | Send notification to DevOps via email/Slack/Telegram. |
	 * | **Diagnostics** | Write stack trace and task payload to error logs. |
	 * | **Auto-healing** | Stop or restart the queue runner if Redis is unstable. |
	 * | **Telemetry** | Increment “fatal errors” counter for monitoring dashboards. |
	 *
	 * Performance:
	 * - Runs only in exceptional conditions, so performance impact is minimal.  
	 * - Should focus on quick telemetry or alerting, not heavy recovery logic.
	 *
	 * Complexity:
	 * - O(1) per fatal event; depends on your implementation of error reporting.
	 *
	 * Security:
	 * - Never include raw user or payload data in external alert messages.  
	 * - Sanitize stack traces to avoid leaking internal paths or code details.
	 *
	 * @param err - The {@link Error} that caused the unrecoverable failure.
	 *   Typically includes stack trace, message, and contextual metadata.
	 * @param task - The {@link Task} being processed when the fatal error occurred.
	 *   May contain the payload and identifiers of the failing job.
	 *
	 * @returns
	 * A `Promise<void>` that resolves after your emergency handling logic completes.
	 *
	 * @example
	 * ```ts
	 * // Example: Send alert to monitoring channel
	 * async onFatal(err: Error, task: Task): Promise<void> {
	 *   console.error('[FATAL]', {
	 *     queue: task.queueName,
	 *     id: task.id,
	 *     message: err.message,
	 *     stack: err.stack,
	 *   });
	 *   await this.redis.incr('stats:fatal_errors');
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: Stop worker on repeated fatal errors
	 * async onFatal(err: Error, task: Task): Promise<void> {
	 *   console.error(`Fatal failure in queue "${task.queueName}":`, err);
	 *   this.stop(task.queueName);
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default (no operation)
	 * async onFatal(err: Error, task: Task): Promise<void> {
	 *   // no-op
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link onFail} - Called for permanent (non-fatal) task failures.
	 * @see {@link onError} - Handles recoverable per-task execution errors.
	 * @see {@link stop} - Can be called here to pause the affected queue.
	 */
	async onFatal(err: Error, task: Task): Promise<void> {
	}

	/**
	 * @summary
	 * Lifecycle **hook** triggered after a task has been **successfully executed**
	 * and acknowledged without errors.
	 *
	 * @remarks
	 * This method is automatically called by {@link success} once the task’s
	 * {@link execute} method finishes successfully and the result has been merged
	 * into its {@link Task.result} object.
	 *
	 * You can override this hook to perform post-processing or bookkeeping actions,
	 * such as:
	 * - Logging or reporting successful task completions.  
	 * - Updating external state (database, cache, analytics, etc.).  
	 * - Emitting application events (“job finished”, “file processed”, etc.).  
	 * - Cleaning up temporary files or releasing external locks.
	 *
	 * The default implementation is a no-op.
	 *
	 * ---
	 * ### Success flow
	 * ```text
	 * execute(task) → returns TaskResult
	 *     ↓
	 * afterExecution(task, result)
	 *     ↓
	 * success(task, result)
	 *     ↓
	 * onSuccess(task, result) → this hook
	 * ```
	 *
	 * ---
	 * ### Behavior
	 * - Runs only after the task has been successfully processed and acknowledged.  
	 * - Executed once per successful task, asynchronously.  
	 * - Exceptions thrown here are caught internally and will not affect other tasks.  
	 * - If the task belongs to a {@link TaskChain}, the next queue is automatically
	 *   scheduled after this hook (handled inside {@link success}).
	 *
	 * ---
	 * ### Typical use cases
	 * | Use case | Example |
	 * |-----------|----------|
	 * | **Logging** | Write “Task completed successfully” to logs. |
	 * | **Metrics** | Increment success counters or track duration. |
	 * | **Notifications** | Notify an API or message bus about completion. |
	 * | **Cleanup** | Remove temporary files or transient Redis keys. |
	 *
	 * Performance:
	 * - Invoked once per successful task; avoid heavy blocking I/O here.  
	 * - Ideal for lightweight asynchronous operations.
	 *
	 * Complexity:
	 * - O(1) default, but depends on your side-effect logic.
	 *
	 * Security:
	 * - Avoid exposing sensitive data from `result` in logs or telemetry.  
	 * - Sanitize outputs if `result` contains user-provided content.
	 *
	 * @param task - The successfully completed {@link Task} instance,
	 *   containing metadata, payload, and progress tracking information.
	 * @param result - The {@link TaskResult} returned from {@link execute},
	 *   typically a JSON-serializable object summarizing the operation outcome.
	 *
	 * @returns
	 * A `Promise<void>` resolving once the custom post-success logic has completed.
	 *
	 * @example
	 * ```ts
	 * // Example: Log the result to console
	 * async onSuccess(task: Task, result: TaskResult): Promise<void> {
	 *   console.info(
	 *     `OK: Task ${task.id} (${task.queueName}) completed successfully:`,
	 *     result
	 *   );
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: Track successful jobs in Redis
	 * async onSuccess(task: Task, result: TaskResult): Promise<void> {
	 *   await this.redis.incr(`stats:${task.queueName}:success`);
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default (no operation)
	 * async onSuccess(task: Task, result: TaskResult): Promise<void> {
	 *   // no-op
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link afterExecution} - Runs immediately before this hook.
	 * @see {@link onChainSuccess} - Triggered when the entire {@link TaskChain} finishes.
	 * @see {@link success} - Internal orchestrator that invokes this hook.
	 */
	async onSuccess(task: Task, result: TaskResult): Promise<void> {
	}

	/**
	 * @summary
	 * Lifecycle **hook** triggered when a **task chain** has completed successfully —
	 * i.e., the **last task** in a {@link TaskChain} has been executed and acknowledged.
	 *
	 * @remarks
	 * This hook represents the **final success stage** of a multi-queue workflow.
	 *  
	 * A {@link TaskChain} is a sequence of related queues (`chain.queues`) that a task
	 * automatically flows through - each queue representing a processing stage.
	 * When the chain reaches its last queue and the final task succeeds,
	 * this hook is invoked to signal **end-of-chain completion**.
	 *
	 * Override this method to implement post-chain behavior such as:
	 * - Marking a complex multi-step process as complete.  
	 * - Merging results from multiple intermediate tasks.  
	 * - Notifying external systems (webhooks, message buses, APIs).  
	 * - Recording global completion metrics or audit logs.
	 *
	 * The default implementation does nothing.
	 *
	 * ---
	 * ### Chain success flow
	 * ```text
	 * execute(task) → returns TaskResult
	 *     ↓
	 * success(task, result)
	 *     ↓
	 * (detects chain completion)
	 *     ↓
	 * onChainSuccess(task, result) → this hook
	 * ```
	 *
	 * ---
	 * ### Behavior
	 * - Triggered **only once per chain**, at its final stage (last queue).  
	 * - Automatically called after {@link onSuccess} for the last task in the sequence.  
	 * - Does not interfere with other queues; purely informational and user-defined.  
	 * - Exceptions thrown here are caught internally to avoid affecting the worker loop.
	 *
	 * ---
	 * ### Typical use cases
	 * | Use case | Example |
	 * |-----------|----------|
	 * | **Finalization** | Mark the overall job as complete in a database. |
	 * | **Aggregation** | Store or summarize results from all chain stages. |
	 * | **Notification** | Send webhook or message to indicate chain success. |
	 * | **Metrics** | Increment counters for successfully completed chains. |
	 *
	 * Performance:
	 * - Runs once per fully completed chain.  
	 * - Suitable for lightweight completion logic or async reporting.
	 *
	 * Complexity:
	 * - O(1) per chain. May vary depending on custom I/O operations.
	 *
	 * Security:
	 * - Avoid exposing sensitive task results in external notifications.  
	 * - Redact private data before logging or sending externally.
	 *
	 * @param task - The final {@link Task} instance in the chain.  
	 *   Contains accumulated metadata (progress timestamps, retries, chain index, etc.).
	 * @param result - The combined {@link TaskResult} returned from the last task’s
	 *   {@link execute} call. Typically includes merged data from previous stages.
	 *
	 * @returns
	 * A `Promise<void>` that resolves once your post-chain logic completes.
	 *
	 * @example
	 * ```ts
	 * // Example: log final completion
	 * async onChainSuccess(task: Task, result: TaskResult): Promise<void> {
	 *   console.info(
	 *     `Chain completed for ${task.queueName}:`,
	 *     `Final result:`,
	 *     result
	 *   );
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: record chain completion in Redis
	 * async onChainSuccess(task: Task, result: TaskResult): Promise<void> {
	 *   await this.redis.incr(`stats:${task.queueName}:chains:success`);
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: trigger external webhook
	 * async onChainSuccess(task: Task, result: TaskResult): Promise<void> {
	 *   await fetch('https://api.example.com/notify', {
	 *     method: 'POST',
	 *     headers: { 'Content-Type': 'application/json' },
	 *     body: JSON.stringify({
	 *       jobId: task.iterationId,
	 *       chain: task.chain.queues,
	 *       status: 'success',
	 *       result,
	 *     }),
	 *   });
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default (no operation)
	 * async onChainSuccess(task: Task, result: TaskResult): Promise<void> {
	 *   // no-op
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link TaskChain} - Describes queue sequences executed in order.
	 * @see {@link onSuccess} - Runs after individual task success.
	 * @see {@link success} - Internal logic that triggers this hook at the final chain stage.
	 */
	async onChainSuccess(task: Task, result: TaskResult): Promise<void> {
	}

	/**
	 * @summary
	 * Lifecycle **hook** triggered when an **uncaught error occurs during an iteration**
	 * of the main queue processing loop ({@link loop}).
	 *
	 * @remarks
	 * This method handles **iteration-level failures**, which differ from individual task errors:
	 * it’s invoked when an unexpected error disrupts the entire queue iteration cycle —
	 * for example, a Redis command failure, data deserialization issue, or logic bug inside
	 * {@link iteration}, {@link reserveMany}, or {@link data}.
	 *
	 * The queue automatically catches such errors and invokes this hook, allowing you
	 * to respond gracefully - e.g. log diagnostic information, alert developers, or temporarily pause the queue.
	 *
	 * The default implementation does nothing.
	 *
	 * ---
	 * ### Iteration error flow
	 * ```text
	 * loop(queueName)
	 *     ↓
	 *   iteration()  ← (multiple tasks per batch)
	 *     ↓
	 *   throws Error
	 *     ↓
	 * onIterationError(err, queueName) → this hook
	 * ```
	 *
	 * ---
	 * ### Behavior
	 * - Executed **once per failed iteration** (not per task).  
	 * - Does **not** stop the queue automatically - the loop continues after a short delay.  
	 * - Intended for global error logging, not individual job failure handling.  
	 * - Exceptions thrown inside this hook are caught internally and ignored.
	 *
	 * ---
	 * ### Typical use cases
	 * | Use case | Example |
	 * |-----------|----------|
	 * | **Logging** | Record unexpected iteration exceptions for debugging. |
	 * | **Monitoring** | Increment Redis or Prometheus “iteration errors” counters. |
	 * | **Alerting** | Notify administrators about repeated iteration-level faults. |
	 * | **Diagnostics** | Save stack traces to disk for forensic analysis. |
	 *
	 * Performance:
	 * - Triggered rarely (only when an entire iteration fails).  
	 * - Safe for moderate I/O operations such as writing to logs or Redis.
	 *
	 * Complexity:
	 * - O(1) default; depends on the complexity of your custom diagnostics logic.
	 *
	 * Security:
	 * - Sanitize stack traces before logging externally.  
	 * - Avoid exposing internal implementation details in public channels.
	 *
	 * @param err - The {@link Error} object describing the iteration-level failure.  
	 *   Includes stack trace and message from the point of failure.
	 * @param queueName - The name of the queue whose iteration failed.
	 *   Corresponds to the `queueName` argument passed to {@link run}.
	 *
	 * @returns
	 * A `Promise<void>` resolving once your error-handling logic completes.
	 *
	 * @example
	 * ```ts
	 * // Example: simple console logging
	 * async onIterationError(err: Error, queueName: string): Promise<void> {
	 *   console.error(
	 *     `Iteration error in queue "${queueName}": ${err.message}`
	 *   );
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: record iteration errors in Redis for monitoring
	 * async onIterationError(err: Error, queueName: string): Promise<void> {
	 *   await this.redis.hset(
	 *     `stats:${queueName}:iteration_errors`,
	 *     Date.now().toString(),
	 *     JSON.stringify({ message: err.message, stack: err.stack })
	 *   );
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Example: notify developers through a webhook
	 * async onIterationError(err: Error, queueName: string): Promise<void> {
	 *   await fetch('https://monitor.example.com/alert', {
	 *     method: 'POST',
	 *     headers: { 'Content-Type': 'application/json' },
	 *     body: JSON.stringify({
	 *       level: 'error',
	 *       source: 'PowerQueue',
	 *       queue: queueName,
	 *       message: err.message,
	 *       stack: err.stack,
	 *       time: new Date().toISOString(),
	 *     }),
	 *   });
	 * }
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Default (no operation)
	 * async onIterationError(err: Error, queueName: string): Promise<void> {
	 *   // no-op
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Lifecycle Hooks
	 * @see {@link iteration} - The batch processor that may trigger this hook.
	 * @see {@link loop} - The continuous worker cycle that catches iteration errors.
	 * @see {@link onError} - Handles per-task failures within iterations.
	 */
	async onIterationError(err: Error, queueName: string): Promise<void> {
	}

		/**
	 * @summary
	 * Internal **per-task execution pipeline** that orchestrates hooks, business logic,
	 * success handling, retries, and cleanup for a single {@link Task}.
	 *
	 * @remarks
	 * This method is the **glue** that runs a task through the full lifecycle:
	 *
	 * 1) **Pre-hook** - {@link beforeExecution}: gives you a chance to validate/enrich the task.  
	 * 2) **Business logic** - {@link execute}: your subclass’s core work.  
	 * 3) **Result merge** - merges any prior `task.result` (if present) with the newly returned result.  
	 * 4) **Success path** - {@link success} updates status/chain; then {@link afterExecution} can post-process the result.  
	 * 5) **Failure path** - on error, attempts {@link retry}; if retry scheduling itself fails, calls {@link error}.  
	 * 6) **Finally/cleanup** - always stops heartbeat and attempts to {@link ack} the item from processing.
	 *
	 * The method returns the (possibly post-processed) {@link TaskResult}. If an error occurs and is handled
	 * via retry/error, it ultimately returns an empty `{}` object for this invocation.
	 *
	 * ---
	 * ### Detailed flow
	 * ```text
	 * data = task
	 * try {
	 *   data = await beforeExecution(task)
	 *   const before = data.result ?? {}
	 *   const after  = await execute(data)
	 *   data.result  = { ...before, ...after }   // shallow merge
	 *
	 *   await success(data, data.result)          // status & chain handling
	 *   return await afterExecution(data, data.result)
	 * } catch (err) {
	 *   try {
	 *     await retry(data)                       // schedules next attempt (backoff)
	 *   } catch (err2) {
	 *     await error(err2, data)                 // enqueue to error list and hook
	 *   }
	 * } finally {
	 *   stopHeartbeat(data)                       // stop VT extensions
	 *   await ack(data).catch(() => {})           // remove from processing/VT
	 * }
	 * return {}
	 * ```
	 *
	 * **Result merging semantics**
	 * - If `data.result` already contains values (e.g., set by `beforeExecution` or a prior stage),
	 *   they are shallow-merged with the new `after` result from {@link execute}.  
	 * - The merge order is `{ ...before, ...after }`, so fields in `after` **override** `before` on conflict.
	 *
	 * **Error semantics**
	 * - Any error thrown by `beforeExecution` or `execute` lands in the `catch` block.  
	 * - `retry(data)` computes a jittered exponential backoff and re-enqueues the task when attempts remain.  
	 * - If `retry()` itself errors (e.g., enqueue failure), `error(err2, data)` records the failure to the
	 *   iteration’s `error:list` and triggers {@link onError}.
	 *
	 * **Cleanup**
	 * - `stopHeartbeat(data)` prevents further visibility extensions for the task.  
	 * - `ack(data)` removes the raw item from the processing list and its VT ZSET entry
	 *   (errors here are swallowed to avoid cascading failures).
	 *
	 * Performance:
	 * - Executes per task and is called under {@link iteration} with bounded {@link concurrency}.  
	 * - The dominant cost is in user-defined {@link execute}. Hooks should remain lightweight.
	 *
	 * Complexity:
	 * - Time: dominated by {@link execute}; wrapper overhead is O(1).  
	 * - Space: O(1) aside from task/result payload sizes.
	 *
	 * Security:
	 * - Validate and sanitize untrusted payloads in {@link beforeExecution} or {@link execute}.  
	 * - Consider redacting sensitive fields in `data.result` before persistence/logging.
	 *
	 * @param task - The {@link Task} to execute (as reserved from Redis). May be enriched by {@link beforeExecution}.
	 *
	 * @returns
	 * A `Promise<TaskResult>` resolving to the final result (after {@link afterExecution}) on success,
	 * or `{}` when the task errored and was handled via retry/error paths in this call.
	 *
	 * @example
	 * ```ts
	 * // Override only `execute`; the `logic` pipeline handles the rest:
	 * class Thumbnailer extends PowerQueue {
	 *   async execute(task: Task): Promise<TaskResult> {
	 *     const { src, size } = task.payload;
	 *     const out = await resizeImage(src, size);
	 *     return { ok: true, outputPath: out };
	 *   }
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Internal Pipeline
	 * @see {@link beforeExecution} - Prepares the task.
	 * @see {@link execute} - Runs the core business logic.
	 * @see {@link afterExecution} - Post-processes successful results.
	 * @see {@link retry} - Schedules retries on failure.
	 * @see {@link error} - Records unrecoverable attempt-level errors.
	 * @see {@link stopHeartbeat} - Stops visibility extension timer.
	 * @see {@link ack} - Acknowledges completion (removes from processing/VT).
	 */
	private async logic(task: Task): Promise<TaskResult> {
		let data = task;

		try {
			data = await this.beforeExecution(task);

			const before = data?.result ?? {};
			const after = await this.execute(data);

			data.result = { 
				...(isObjFilled(before) ? before : {}), 
				...(isObjFilled(after) ?after : {}), 
			};

			await this.success(data, data.result);
			return await this.afterExecution(data, data.result);
		} 
		catch (err) {
			try { 
				await this.retry(data); 
			}
			catch (err2) { 
				await this.error(err2 as Error, data); 
			}
		} 
		finally {
			try { 
				this.stopHeartbeat(data);
				await this.ack(data).catch(() => {});
			} 
			catch {
			}
		}
		return {};
	}

	/**
	 * @summary
	 * Computes an **exponential backoff delay (in seconds)** with a small random **jitter**
	 * for use during task retry scheduling.
	 *
	 * @remarks
	 * When a task fails and needs to be retried, waiting a progressively longer time between
	 * attempts helps avoid overwhelming Redis or external systems (known as *exponential backoff*).
	 *  
	 * This method calculates the retry delay as:
	 * ```
	 * delay = min(maxDelay, base * 2^(attempt - 1) + jitter)
	 * ```
	 * where `base` and `maxDelay` are taken from {@link retryBaseSec} and {@link retryMaxSec}.
	 *
	 * The added **jitter** (random 0…`base` seconds) prevents multiple workers from retrying
	 * failed tasks simultaneously - a common issue called the **thundering herd problem**.
	 *
	 * ---
	 * ### Formula breakdown
	 * | Term | Meaning | Default Example |
	 * |------|----------|----------------|
	 * | `base` | Initial retry delay in seconds | `1` |
	 * | `attempt` | Current retry attempt number (1, 2, 3, …) | `3` |
	 * | `pow` | `base × 2^(attempt - 1)` - exponential growth | `4` |
	 * | `jitter` | Random offset between 0 and `base` | `0–1` |
	 * | `maxD` | Upper cap for delay (`retryMaxSec`) | `3600` |
	 * | **Result** | `min(maxD, pow + jitter)` | e.g., `4.7s` |
	 *
	 * ---
	 * ### Example delay sequence
	 * For `base = 1`, `max = 60`:
	 * ```
	 * Attempt 1 → ~1–2s
	 * Attempt 2 → ~2–3s
	 * Attempt 3 → ~4–5s
	 * Attempt 4 → ~8–9s
	 * Attempt 5 → ~16–17s
	 * Attempt 6 → ~32–33s
	 * Attempt 7+ → capped at 60s
	 * ```
	 *
	 * ---
	 * ### Why this matters
	 * - Reduces retry congestion under high failure rates.  
	 * - Smooths retry timing across multiple workers.  
	 * - Keeps system load and Redis queues stable even during cascading failures.
	 *
	 * Performance:
	 * - O(1); purely arithmetic.
	 *
	 * Complexity:
	 * - Constant time and space. Negligible computational overhead.
	 *
	 * Security:
	 * - No security implications. Uses `Math.random()` for jitter, not cryptographically secure.
	 *
	 * @param attempt - The current retry attempt number (≥ 1).  
	 *   For the first retry attempt, pass `1`; for subsequent retries, increment by one each time.
	 *
	 * @returns
	 * The number of seconds to wait before scheduling the next retry.
	 *
	 * @example
	 * ```ts
	 * const delay1 = this.jitteredBackoffSec(1); // ~1–2 seconds
	 * const delay2 = this.jitteredBackoffSec(3); // ~4–5 seconds
	 * const delay3 = this.jitteredBackoffSec(7); // ~60 seconds (capped)
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Used inside retry logic:
	 * const delaySec = this.jitteredBackoffSec(task.currentAttempt);
	 * await this.addTask(task, delaySec);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Retry Helpers
	 * @see {@link retry} - Uses this delay when rescheduling failed tasks.
	 * @see {@link retryBaseSec} - Minimum retry delay (seconds).
	 * @see {@link retryMaxSec} - Maximum retry delay (cap).
	 */
	private jitteredBackoffSec(attempt: number): number {
		const base = Math.max(1, Number(this.retryBaseSec) || 1);
		const maxD = Math.max(base, Number(this.retryMaxSec) || 3600);
		const pow = Math.min(maxD, base * Math.pow(2, Math.max(0, attempt - 1)));
		const jitter = Math.floor(Math.random() * base);
		
		return Math.min(maxD, pow + jitter);
	}

	/**
	 * @summary
	 * Schedules a **retry** for a failed task using **exponential backoff with jitter**,
	 * or marks it as **permanently failed** when the attempt limit is reached.
	 *
	 * @remarks
	 * This internal helper drives the queue’s retry semantics. It:
	 *
	 * 1) **Validates** the incoming {@link Task} shape (IDs, counters, queue name).  
	 * 2) If the task has **remaining attempts**, increments `currentAttempt`,
	 *    computes a delay via {@link jitteredBackoffSec}, re-enqueues the task
	 *    with {@link addTask}, and triggers the {@link onRetry} hook.  
	 * 3) If there are **no attempts left**, classifies the task as a permanent
	 *    failure by calling {@link fail}, which emits {@link onFail} and
	 *    routes the task to a `:fail:list` queue.  
	 * 4) If re-enqueueing itself throws, it falls back to {@link fail} to make
	 *    the error visible and auditable.
	 *
	 * **Important:** A task’s `maxAttempts` value represents the **total number of executions**
	 * (first attempt + retries). Concretely, when `currentAttempt < (maxAttempts - 1)`,
	 * the task is still eligible for another try.
	 *
	 * ---
	 * ### Validation & error path
	 * - If mandatory fields are missing or invalid (IDs, counters, queue name), this method
	 *   calls {@link error} with `"Task format error."`, placing the task into an
	 *   `:error:list` queue and invoking {@link onError}. Execution then returns early.
	 *
	 * ---
	 * ### Retry timing
	 * The delay for the next attempt is computed by {@link jitteredBackoffSec}:
	 * `min(retryMaxSec, retryBaseSec * 2^(attempt-1) + jitter)`, where `jitter` is
	 * a small random offset (0…`retryBaseSec`) to avoid thundering herds.
	 *
	 * ---
	 * ### Control flow
	 * ```text
	 * validate task
	 *   ├─ invalid → error("Task format error.") → return
	 *   └─ valid
	 *       ├─ currentAttempt < maxAttempts - 1
	 *       │     └─ enqueue next attempt with backoff → onRetry() → return
	 *       └─ else
	 *             └─ fail("The attempt limit has been reached.") → return
	 * ```
	 *
	 * Performance:
	 * - O(1) CPU; primary cost is one Redis write via {@link addTask}.  
	 * - Designed to be extremely lightweight and safe under high throughput.
	 *
	 * Complexity:
	 * - Time: O(1) aside from enqueue I/O.  
	 * - Space: O(1).
	 *
	 * Security:
	 * - Does not mutate payload content; it only advances counters and re-enqueues.  
	 * - Ensure sensitive data handling is done in higher-level hooks/logs.
	 *
	 * @param task - The {@link Task} that just failed this attempt. Must have valid
	 *   `id`, `iterationId`, `queueName`, `currentAttempt`, and `maxAttempts`.
	 *
	 * @returns
	 * A `Promise<void>` that resolves after the task has been re-enqueued for retry
	 * (and {@link onRetry} fired), or after it has been marked as failed via {@link fail}.
	 *
	 * @throws
	 * This method **swallows** most internal errors by routing them to {@link fail} /
	 * {@link error}; it does not rethrow outward. Hook implementations you override
	 * (e.g., {@link onRetry}) should also avoid throwing.
	 *
	 * @example
	 * ```ts
	 * // Inside your worker, you don't call retry() directly.
	 * // The internal pipeline (see logic()) calls it when execute() throws.
	 * //
	 * // However, you can tailor policy via:
	 * this.retryBaseSec = 2;   // start from 2s
	 * this.retryMaxSec  = 300; // cap at 5 min
	 * this.maxAttempts  = 5;   // total tries per task
	 * ```
	 *
	 * @since 2.0.0
	 * @category Retry & Backoff
	 * @see {@link jitteredBackoffSec} - Exponential backoff with jitter.
	 * @see {@link addTask} - Re-enqueues the next attempt with delay.
	 * @see {@link onRetry} - Hook fired after scheduling a retry.
	 * @see {@link fail} - Marks a task as permanently failed.
	 * @see {@link error} - Records malformed tasks or enqueue failures.
	 */
	private async retry(task: Task): Promise<void> {
		if (!isObjFilled(task)
			|| !isStrFilled(task.iterationId)
			|| !isStrFilled(task.id)
			|| !isStrFilled(task.queueName) 
			|| !isNumPZ(task.currentAttempt) 
			|| !isNumPZ(task.maxAttempts)) {
			await this.error(new Error('Task format error.'), task);
			return;
		}
		const maxAttempts: number = task.maxAttempts ?? this.maxAttempts;

		try {
			if (task.currentAttempt < (maxAttempts - 1)) {
				const taskProcessed = { ...task, currentAttempt: task.currentAttempt + 1 };
				const delaySec = this.jitteredBackoffSec(taskProcessed.currentAttempt);

				await this.addTask(taskProcessed, delaySec);
				await this.onRetry(taskProcessed);
				return;
			}
		}
		catch (err) {
			await this.fail(err as Error, task);
			return;
		}
		await this.fail(new Error('The attempt limit has been reached.'), task);
	}

	/**
	 * @summary
	 * Internal handler for **iteration-level failures** that ensures the user hook runs
	 * and then **cleans up local processing state** to avoid leaks.
	 *
	 * @remarks
	 * This method is called by the main loop when an error bubbles up from an iteration
	 * (e.g., failures inside {@link iteration}, {@link data}, {@link reserveMany}, or any
	 * iteration-scoped logic). It performs two actions:
	 *
	 * 1) **Notify** - invokes the user-overridable hook {@link onIterationError} so you can
	 *    log, alert, or collect metrics. Any exception thrown by the hook is **caught and ignored**
	 *    to prevent secondary failures from cascading.
	 *
	 * 2) **Cleanup** - iterates over the provided `data` array (the batch of tasks that
	 *    were in play for this iteration) and removes their `id` entries from the internal
	 *    `processingRaw` map, if present. This prevents stale “in-flight” references from
	 *    lingering in memory after an iteration fails midway.
	 *
	 * > Note: This method does **not** touch Redis state directly; it is a local safeguard.
	 * > Re-visibility and requeue behavior is handled elsewhere by {@link requeueExpired}.
	 *
	 * ---
	 * ### Behavior & guarantees
	 * - Calls {@link onIterationError} **exactly once** per failed iteration.
	 * - Swallows any error from the hook to keep the worker robust.
	 * - Best-effort cleanup of `processingRaw`; only entries with a truthy string `id` are removed.
	 *
	 * Performance:
	 * - O(n) in `data.length` for the cleanup loop; typically small batches.
	 *
	 * Complexity:
	 * - Time: O(n) for cleanup; O(1) for the hook invocation.  
	 * - Space: O(1).
	 *
	 * Security:
	 * - No I/O or payload exposure; this is local state management.  
	 * - Ensure your {@link onIterationError} implementation redacts sensitive data before logging.
	 *
	 * @param err - The iteration-level {@link Error} that occurred.
	 * @param queueName - The logical name of the queue whose iteration failed.
	 * @param data - The batch of tasks that were being processed when the error happened.
	 *   Each item is expected to have an `id` property if it originated from the queue.
	 *
	 * @returns
	 * `Promise<void>` - resolves after the hook (if any) and local cleanup complete.
	 *
	 * @example
	 * ```ts
	 * // You do not normally call this directly. It is used by the loop.
	 * // Override onIterationError instead:
	 * async onIterationError(err: Error, queueName: string): Promise<void> {
	 *   logger.error({ queueName, err }, 'Iteration failure');
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Internal Pipeline
	 * @see {@link onIterationError} - User hook for iteration-level errors.
	 * @see {@link iteration} - The batch execution that may fail and trigger this method.
	 * @see {@link requeueExpired} - Restores visibility for unacknowledged tasks at the Redis level.
	 */
	private async iterationError(err: Error, queueName: string, data: any[]): Promise<void> {
		try {
			await this.onIterationError(err, queueName);
		}
		catch (err2) {
		}
		for (const t of data || []) {
			if (isStrFilled(t.id)) {
				this.processingRaw.delete(t.id);
			}
		}
	}

	/**
	 * @summary
	 * Internal handler for **iteration-level failures** that ensures the user hook runs
	 * and then **cleans up local processing state** to avoid leaks.
	 *
	 * @remarks
	 * This method is called by the main loop when an error bubbles up from an iteration
	 * (e.g., failures inside {@link iteration}, {@link data}, {@link reserveMany}, or any
	 * iteration-scoped logic). It performs two actions:
	 *
	 * 1) **Notify** - invokes the user-overridable hook {@link onIterationError} so you can
	 *    log, alert, or collect metrics. Any exception thrown by the hook is **caught and ignored**
	 *    to prevent secondary failures from cascading.
	 *
	 * 2) **Cleanup** - iterates over the provided `data` array (the batch of tasks that
	 *    were in play for this iteration) and removes their `id` entries from the internal
	 *    `processingRaw` map, if present. This prevents stale “in-flight” references from
	 *    lingering in memory after an iteration fails midway.
	 *
	 * > Note: This method does **not** touch Redis state directly; it is a local safeguard.
	 * > Re-visibility and requeue behavior is handled elsewhere by {@link requeueExpired}.
	 *
	 * ---
	 * ### Behavior & guarantees
	 * - Calls {@link onIterationError} **exactly once** per failed iteration.
	 * - Swallows any error from the hook to keep the worker robust.
	 * - Best-effort cleanup of `processingRaw`; only entries with a truthy string `id` are removed.
	 *
	 * Performance:
	 * - O(n) in `data.length` for the cleanup loop; typically small batches.
	 *
	 * Complexity:
	 * - Time: O(n) for cleanup; O(1) for the hook invocation.  
	 * - Space: O(1).
	 *
	 * Security:
	 * - No I/O or payload exposure; this is local state management.  
	 * - Ensure your {@link onIterationError} implementation redacts sensitive data before logging.
	 *
	 * @param err - The iteration-level {@link Error} that occurred.
	 * @param queueName - The logical name of the queue whose iteration failed.
	 * @param data - The batch of tasks that were being processed when the error happened.
	 *   Each item is expected to have an `id` property if it originated from the queue.
	 *
	 * @returns
	 * `Promise<void>` - resolves after the hook (if any) and local cleanup complete.
	 *
	 * @example
	 * ```ts
	 * // You do not normally call this directly. It is used by the loop.
	 * // Override onIterationError instead:
	 * async onIterationError(err: Error, queueName: string): Promise<void> {
	 *   logger.error({ queueName, err }, 'Iteration failure');
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Internal Pipeline
	 * @see {@link onIterationError} - User hook for iteration-level errors.
	 * @see {@link iteration} - The batch execution that may fail and trigger this method.
	 * @see {@link requeueExpired} - Restores visibility for unacknowledged tasks at the Redis level.
	 */
	private async error(err: Error, task: Task): Promise<void> {
		try {
			await this.addTask({ 
				...task, 
				queueName: [ task.queueName, task.iterationId, 'error', 'list' ].join(':'), 
				currentAttempt: 0, 
				payload: {
					...task.payload,
					errorMessage: String((err as any)?.message ?? ''),
				},
			});
			await this.onError(err, task);
		}
		catch (err2) {
			try {
				await this.onFatal(err2 as Error, task);
			}
			catch {
			}
		}
		try {
			await this.status(task, 'error');
		}
		catch {
		}
	}

	/**
	 * @summary
	 * Marks a task as **permanently failed**, enqueues a copy into a dedicated
	 * **failure list**, triggers hooks, and updates iteration **status counters**.
	 *
	 * @remarks
	 * This internal helper is invoked when a task has **exhausted all retry attempts**
	 * or when retry scheduling itself fails. Its responsibilities are:
	 *
	 * 1) **Persist failure artifact**  
	 *    - Re-enqueues a *copy* of the task into a per-iteration failure list:
	 *      ```
	 *      <queueName>:<iterationId>:fail:list
	 *      ```
	 *    - Resets `currentAttempt` to `0` on the artifact.
	 *    - Augments the artifact’s `payload` with `errorMessage` taken from `err.message`.
	 *
	 * 2) **Fire user hook** - Calls {@link onFail} so applications can log, alert, or
	 *    otherwise react to the permanent failure.  
	 *    - If `onFail` throws, calls {@link onFatal} as a last-resort notifier.
	 *
	 * 3) **Update metrics** - Calls {@link status} with category `"fail"` to increment
	 *    per-iteration counters (keys are TTL’d by {@link expireStatusSec}).
	 *
	 * All actions are wrapped in `try/catch` to keep the worker robust:
	 * - Failure to enqueue the artifact or run hooks **does not crash** the loop.
	 * - Any exception in `onFail` is forwarded to `onFatal` and suppressed afterwards.
	 * - `status()` errors are swallowed.
	 *
	 * ---
	 * ### Step-by-step
	 * | Step | Action | Notes |
	 * |------|--------|-------|
	 * | 1 | Build failure artifact | Copies task, resets `currentAttempt`, adds `payload.errorMessage`. |
	 * | 2 | Enqueue to `:fail:list` | Via {@link addTask} using key `<queueName>:<iterationId>:fail:list`. |
	 * | 3 | Invoke `onFail` | User hook; can log/alert/persist details. |
	 * | 4 | Fallback to `onFatal` | Only if `onFail` throws. |
	 * | 5 | Update `status(...,'fail')` | Increments iteration counters with TTL. |
	 *
	 * ---
	 * ### Why store a failure artifact?
	 * Keeping the failed task (with the error message) in a deterministic Redis list
	 * allows later **inspection**, **replay**, or **forensics** without scanning logs.
	 *
	 * Performance:
	 * - Single enqueue (LIST or ZSET via {@link addTask}, depending on implementation) and a
	 *   small number of constant-time operations; negligible CPU overhead.
	 *
	 * Complexity:
	 * - Time: O(1) excluding Redis I/O.  
	 * - Space: proportional to the size of the serialized failure artifact.
	 *
	 * Security:
	 * - `errorMessage` is copied into the payload; ensure upstream hooks avoid leaking
	 *   sensitive data (sanitize before exposing to users or external systems).
	 *
	 * @param err - The {@link Error} that caused the permanent failure. Its message is
	 *   copied to `payload.errorMessage` on the artifact.
	 * @param task - The original {@link Task} instance that failed permanently.
	 *
	 * @returns
	 * `Promise<void>` - resolves after the artifact has been enqueued (best effort),
	 * hooks have been invoked, and status counters updated.
	 *
	 * @example
	 * ```ts
	 * // You don't call fail() directly; it is used by retry() when attempts are exhausted.
	 * // Implement onFail() to react to permanent failures:
	 * async onFail(err: Error, task: Task): Promise<void> {
	 *   console.error(`Task ${task.id} failed permanently: ${err.message}`);
	 *   await this.redis.incr(`stats:${task.queueName}:permanent_failures`);
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Failure Handling
	 * @see {@link retry} - Decides whether to reschedule or call `fail`.
	 * @see {@link onFail} - Hook invoked after enqueueing the failure artifact.
	 * @see {@link onFatal} - Called if `onFail` throws.
	 * @see {@link status} - Updates per-iteration counters (category `"fail"`).
	 */
	private async fail(err: Error, task: Task): Promise<void> {
		try {
			await this.addTask({ 
				...task, 
				queueName: [ task.queueName, task.iterationId, 'fail', 'list' ].join(':'), 
				currentAttempt: 0, 
				payload: {
					...task.payload,
					errorMessage: String((err as any)?.message ?? ''),
				},
			});
			await this.onFail(err, task);
		}
		catch (err2) {
			try {
				await this.onFatal(err2 as Error, task);
			}
			catch {
			}
		}
		try {
			await this.status(task, 'fail');
		}
		catch {
		}
	}

	/**
	 * @summary
	 * Internal **success handler** that finalizes a completed task, advances
	 * **chain progression** when applicable, updates **status counters**, and
	 * triggers success-related hooks.
	 *
	 * @remarks
	 * This method is called after {@link execute} resolves without throwing and
	 * after the result is merged in the pipeline (see {@link logic}). It:
	 *
	 * 1) **Stamps progress** - sets `progress.successAt = Date.now()` and appends a
	 *    timestamp to `progress.chain` to mark the completion of this chain stage.  
	 * 2) **Handles chaining** (if {@link Task.chain} is defined and valid):
	 *    - If this task is the **last stage** in the chain (`index === queues.length - 1`),
	 *      it updates iteration counters via {@link status} with `"success"` and then
	 *      fires {@link onChainSuccess}.  
	 *    - Otherwise, it **enqueues the next stage** by calling {@link addTask} with
	 *      `queueName = chain.queues[index + 1]`, resets `currentAttempt` to `0`,
	 *      increments the chain `index`, and forwards the accumulated `result`.  
	 *      If the next queue name is invalid, it falls back to {@link fail} with a
	 *      `"Next queue format error."`.  
	 * 3) **Non-chained tasks** - If no valid chain is present, it simply updates
	 *    iteration counters with `"success"`.  
	 * 4) **Hook** - Calls {@link onSuccess} for application-level side effects.
	 *
	 * Any error thrown by the above steps is caught, and the method attempts to:
	 * - Update iteration counters with category `"fatal"` via {@link status}, and
	 * - Invoke {@link onFatal} to report the critical condition.
	 * Both of these error paths are **best-effort** and swallow secondary failures
	 * to keep the worker robust.
	 *
	 * ---
	 * ### Chain control flow
	 * ```text
	 * if (valid chain):
	 *   currentIndex = chain.index
	 *   newIndex     = currentIndex + 1
	 *   progress.chain.push(timestamp)
	 *
	 *   if (currentIndex === queues.length - 1):
	 *     status('success')
	 *     onChainSuccess(taskProcessed, result)
	 *   else if (newIndex <= queues.length - 1 && isStrFilled(queues[newIndex])):
	 *     addTask({...taskProcessed, queueName: queues[newIndex], chain.index = newIndex, currentAttempt = 0, result})
	 *   else:
	 *     fail(new Error('Next queue format error.'), taskProcessed)
	 * else:
	 *   status('success')
	 *
	 * onSuccess(taskProcessed, result)
	 * ```
	 *
	 * ---
	 * ### Behavior summary
	 * | Scenario | Action |
	 * |----------|--------|
	 * | Last chain stage | Increment `"success"` counters; call {@link onChainSuccess}. |
	 * | Middle chain stage | Enqueue next queue with updated `chain.index`; carry `result`. |
	 * | Invalid next stage | Call {@link fail} with `"Next queue format error."`. |
	 * | No chain | Increment `"success"` counters only. |
	 * | Any thrown error in the above | Try `status('fatal')`; then {@link onFatal}. |
	 *
	 * Performance:
	 * - Light bookkeeping + one Redis write for `status`, and possibly one `addTask` write
	 *   when forwarding to the next chain stage.  
	 * - Intended to be minimal overhead relative to the main work in {@link execute}.
	 *
	 * Complexity:
	 * - Time: O(1) (constant control flow; potential single enqueue for chaining).  
	 * - Space: O(1) additional state; `result` is forwarded by reference in memory.
	 *
	 * Security:
	 * - Be cautious with `result` contents in your hooks; avoid logging sensitive fields.  
	 * - Chain forwarding includes the `result` and payload; ensure downstream queues are trusted.
	 *
	 * @param task - The {@link Task} that just completed successfully.
	 * @param result - The accumulated {@link TaskResult} after merging and post-processing.
	 *
	 * @returns
	 * `Promise<void>` - resolves after counters are updated, chaining is handled,
	 * and hooks have been fired (best effort).
	 *
	 * @example
	 * ```ts
	 * // Typical chain: parse → transform → persist
	 * // queues = ['jobs:parse', 'jobs:transform', 'jobs:persist']
	 * // On success at 'jobs:parse', this method enqueues the next stage 'jobs:transform'
	 * // and passes along the merged `result`.
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Non-chained task:
	 * // success() will only call status('success') and then onSuccess()
	 * ```
	 *
	 * @since 2.0.0
	 * @category Internal Pipeline
	 * @see {@link onSuccess} - Post-success application hook.
	 * @see {@link onChainSuccess} - Fired when the last chain stage completes.
	 * @see {@link status} - Updates per-iteration counters ("success" / "fatal").
	 * @see {@link addTask} - Used to forward tasks to the next chain stage.
	 * @see {@link fail} - Invoked on invalid next-stage configuration.
	 */
	private async success(task: Task, result: TaskResult): Promise<void> {
		const taskProcessed = { 
			...task, 
			progress: {
				...task.progress,
				successAt: Date.now(),
			},
		};

		try {
			if (isObjFilled(taskProcessed.chain)
				&& isArrFilled(taskProcessed.chain.queues)
				&& isNumPZ(taskProcessed.chain.index)) {
				const currentIndex = taskProcessed.chain.index;
				const newIndex = currentIndex + 1;

				taskProcessed.progress.chain.push(Date.now());

				if (currentIndex === (taskProcessed.chain.queues.length - 1)) {
					await this.status(taskProcessed, 'success');
					await this.onChainSuccess(taskProcessed, result);
				}
				else if (newIndex <= (taskProcessed.chain.queues.length - 1)) {
					const newQueueName = taskProcessed.chain.queues[newIndex];

					if (isStrFilled(newQueueName)) {
						await this.addTask({ 
							...taskProcessed, 
							queueName: newQueueName, 
							currentAttempt: 0, 
							chain: {
								...taskProcessed.chain,
								index: newIndex,
							},
							result,
						});
					}
					else {
						await this.fail(new Error('Next queue format error.'), taskProcessed);
					}
				}
			}
			else {
				await this.status(taskProcessed, 'success');
			}
			await this.onSuccess(taskProcessed, result);
		}
		catch (err) {
			try {
				await this.status(taskProcessed, 'fatal');
			}
			catch {
			}
			try {
				await this.onFatal(err as Error, taskProcessed);
			}
			catch {
			}
		}
	}

	/**
	 * @summary
	 * Increments **per-iteration status counters** in Redis for a task and
	 * sets a **TTL** on those counters to keep short-lived metrics tidy.
	 *
	 * @remarks
	 * This helper records lightweight iteration metrics for observability:
	 *
	 * - A **processed** counter that tracks how many tasks (of any outcome) were handled
	 *   in this iteration group (`<queueName>:<iterationId>:processed`).
	 * - A **category-specific** counter (e.g., `"success"`, `"fail"`, `"error"`, `"fatal"`)
	 *   keyed as `<queueName>:<iterationId>:<category>`.
	 *
	 * Both keys are incremented with `INCR` and then assigned an expiration using
	 * `EXPIRE` with {@link expireStatusSec} seconds. This ensures iteration stats
	 * naturally roll off after a short window and do not accumulate indefinitely.
	 *
	 * **Key format**
	 * - Processed key: `queue:<queueName>:<iterationId>:processed`  
	 * - Category key:  `queue:<queueName>:<iterationId>:<category>`
	 *
	 * Keys are constructed with {@link toKeyString} for strict namespacing and validation.
	 *
	 * ---
	 * ### Behavior
	 * 1. Validates Redis connectivity via {@link checkConnection}; throws if disconnected.  
	 * 2. Builds the `processed` and `<category>` keys for the given task.  
	 * 3. `INCR` both keys.  
	 * 4. `EXPIRE` both keys to {@link expireStatusSec}.
	 *
	 * The default `category` is `"success"`, but callers may pass `"fail"`, `"error"`, or `"fatal"`
	 * (or any other domain-specific category) to produce separate counters.
	 *
	 * ---
	 * Performance:
	 * - Each call performs **four** lightweight Redis operations: `INCR`, `INCR`, `EXPIRE`, `EXPIRE`.  
	 * - Suitable for high-throughput; keys have short TTL to minimize storage.
	 *
	 * Complexity:
	 * - Time: `O(1)` (constant number of Redis calls).  
	 * - Space: `O(1)` per key until TTL expiry.
	 *
	 * Security:
	 * - No user payload is written; only numeric counters.  
	 * - Key construction uses {@link toKeyString} to avoid unsafe characters.
	 *
	 * @param task - The {@link Task} whose iteration counters should be updated.
	 *   Uses `task.queueName` and `task.iterationId` to namespace the keys.
	 * @param category - Logical outcome category for this event (e.g., `"success"`, `"fail"`, `"error"`, `"fatal"`).  
	 *   {Default: `"success"`}
	 *
	 * @returns
	 * A `Promise<void>` that resolves after counters are incremented and expirations set.
	 *
	 * @throws
	 * - `"Redis connection error."` if the client is disconnected.  
	 * - Any underlying Redis error while executing `INCR`/`EXPIRE`.
	 *
	 * @example
	 * ```ts
	 * // Mark a successful completion:
	 * await this.status(task, 'success');
	 *
	 * // Mark a permanent failure:
	 * await this.status(task, 'fail');
	 *
	 * // Mark an iteration-level fatal condition:
	 * await this.status(task, 'fatal');
	 * ```
	 *
	 * @example
	 * ```ts
	 * // Reading the counters elsewhere:
	 * const processedKey = this.toKeyString(task.queueName, task.iterationId, 'processed');
	 * const successKey   = this.toKeyString(task.queueName, task.iterationId, 'success');
	 * const [processed, success] = await this.redis.mget(processedKey, successKey);
	 * ```
	 *
	 * @since 2.0.0
	 * @category Metrics & Observability
	 * @see {@link expireStatusSec} - TTL applied to iteration counters.
	 * @see {@link toKeyString} - Safe Redis key construction.
	 * @see {@link success} - Calls this to count successful outcomes.
	 * @see {@link fail} - Calls this to count permanent failures.
	 * @see {@link iterationError} - May update fatal/error counters indirectly.
	 */
	private async status(task: Task, category: string = 'success'): Promise<void> {
		if (!this.checkConnection()) {
			throw new Error('Redis connection error.');
		}
		const processedKey = this.toKeyString(task.queueName, task.iterationId, 'processed');
		const categoryKey = this.toKeyString(task.queueName, task.iterationId, category);

		await (this.redis as any)?.incr(processedKey);
		await (this.redis as any)?.incr(categoryKey);

		await (this.redis as any)?.expire(processedKey, this.expireStatusSec);
		await (this.redis as any)?.expire(categoryKey, this.expireStatusSec);
	}

	/**
	 * @summary
	 * Main **worker loop** for a single queue - continuously promotes delayed jobs,
	 * requeues expired jobs, reserves ready tasks, and processes them with bounded concurrency
	 * until `runner.running` becomes `false`.
	 *
	 * @remarks
	 * This method is the **engine** of the queue worker. It must be invoked by {@link run}
	 * (which sets `runner.running = true` and fire-and-forgets the loop).
	 *
	 * On each cycle it:
	 * 1) Verifies Redis connectivity; if disconnected, sleeps for {@link iterationTimeout} and retries.  
	 * 2) Best-effort **promotes** due delayed jobs via {@link promoteDelayed}(delayed → ready).  
	 * 3) Best-effort **requeues** expired in-flight jobs via {@link requeueExpired}(processing:vt → ready).  
	 * 4) Attempts to **reserve** a batch of ready tasks via {@link data} (which uses {@link reserveMany}).  
	 *    - If no tasks are available, sleeps for {@link iterationTimeout} and continues.  
	 *    - Otherwise, calls {@link iteration} to process them with concurrency limits.  
	 * 5) Any iteration-level exception is caught and delegated to {@link iterationError},
	 *    then the loop sleeps for {@link iterationTimeout} before continuing.
	 *
	 * All promotion/requeue steps are wrapped in individual try/catch blocks so transient
	 * failures there do not stop the loop. The loop exits when {@link stop} sets
	 * `runner.running = false`.
	 *
	 * ---
	 * ### Control flow (pseudocode)
	 * ```text
	 * assert queueName valid
	 * derive keys: ready, processing, processingVt, delayed
	 *
	 * while (runner.running):
	 *   if !checkConnection():
	 *     wait(iterationTimeout); continue
	 *
	 *   try promoteDelayed(delayed, ready) catch ignore
	 *   try requeueExpired(processing, processingVt, ready) catch ignore
	 *
	 *   data = []
	 *   try:
	 *     data = await this.data(queueName)    // reserves + heartbeats, etc.
	 *     if data empty:
	 *       wait(iterationTimeout); continue
	 *     await this.iteration(data)           // runs tasks with concurrency cap
	 *   catch err:
	 *     await this.iterationError(err, queueName, data)
	 *     wait(iterationTimeout)
	 * ```
	 *
	 * ---
	 * ### Key design characteristics
	 * - **Resilient**: Promotion/requeue/iteration errors are isolated and do not crash the loop.  
	 * - **Fairness**: Uses visibility timeouts and requeue to avoid lost tasks.  
	 * - **Throughput**: Reservation is batched (see {@link portionLength} in {@link reserveMany}); execution is
	 *   bounded by {@link concurrency}.  
	 * - **Efficiency**: Sleeps only when idle or after errors; otherwise runs continuously.
	 *
	 * Performance:
	 * - Sustained throughput depends on: Redis latency, batch sizes, and {@link concurrency}.  
	 * - Idle periods cost one `wait(iterationTimeout)` sleep per cycle.  
	 * - Promotion and requeue use Lua scripts when available for atomic, low-RTT operations.
	 *
	 * Complexity:
	 * - Per cycle: O(1) overhead + O(k) for promotion/requeue batches + O(n) for processing,
	 *   where `n` is the number of tasks reserved this cycle.  
	 * - Memory: O(n) for the in-memory batch and per-task heartbeats during execution.
	 *
	 * Reliability:
	 * - If a worker crashes mid-task, the item becomes visible again after its VT expires and
	 *   is requeued by {@link requeueExpired}.  
	 * - Heartbeats (started in {@link data}) keep long-running tasks invisible by extending VT.
	 *
	 * Security:
	 * - No direct payload modifications here; processing happens in {@link iteration}/{@link execute}.  
	 * - Keys are derived using safe builders: {@link readyKey}, {@link processingKey}, {@link processingVtKey}, {@link delayedKey}.
	 *
	 * @param queueName - Logical name of the queue to process. Must be a non-empty string.
	 * @param runner - A small control object with a `running` flag; when set to `false`,
	 *   the loop exits gracefully after the current cycle.
	 *
	 * @throws
	 * - `"Queue name is not valid: "<queueName>"..."` if the given `queueName` is empty or invalid.  
	 * - May rethrow only that validation error; all other operational errors are caught internally.
	 *
	 * @example
	 * ```ts
	 * // Not called directly in user code - run() invokes it:
	 * worker.run('emails');   // internally: loop('emails', { running: true })
	 * // Later:
	 * worker.stop('emails');  // loop will exit after the current cycle
	 * ```
	 *
	 * @since 2.0.0
	 * @category Internal Loop
	 * @see {@link run} - Starts the loop.
	 * @see {@link stop} - Signals the loop to stop.
	 * @see {@link data} - Reserves and materializes a batch of Task objects.
	 * @see {@link iteration} - Executes a batch with concurrency limits.
	 * @see {@link promoteDelayed} - Moves due delayed items into the ready list.
	 * @see {@link requeueExpired} - Recycles expired processing items back to ready.
	 * @see {@link iterationError} - Handles and cleans up after iteration-level failures.
	 */
	private async loop(queueName: string, runner: { running: boolean }) {
		if (!isStrFilled(queueName)) {
			throw new Error(`Queue name is not valid: "${queueName}"; Type: "${typeof queueName}".`);
		}
		const ready = this.readyKey(queueName);
		const processing = this.processingKey(queueName);
		const processingVt = this.processingVtKey(queueName);
		const delayed = this.delayedKey(queueName);

		while (runner.running) {
			if (!this.checkConnection()) {
				await wait(this.iterationTimeout);
				continue;
			}
			try {
				await this.promoteDelayed(delayed, ready);
			}
			catch {
			}
			try {
				await this.requeueExpired(processing, processingVt, ready);
			}
			catch {
			}
			let data: Task[] = [];

			try {
				data = await this.data(queueName);

				if (!isArrFilled(data)) {
					await wait(this.iterationTimeout);
					continue;
				}
				await this.iteration(data);
			}
			catch (err) {
				await this.iterationError(err as Error, queueName, data);
				await wait(this.iterationTimeout);
			}
		}
	}

	/**
	 * @summary
	 * Reserves a batch of raw items from Redis for **this queue**, materializes them
	 * into typed {@link Task} objects, and **starts heartbeats** to maintain visibility.
	 *
	 * @remarks
	 * This method is called by the main {@link loop} before each {@link iteration}.
	 * It performs three responsibilities:
	 *
	 * 1) **Reservation** - Calls {@link reserveMany} to atomically move up to
	 *    {@link portionLength} items from the queue’s **ready** LIST to its **processing**
	 *    LIST and records their visibility deadlines in the **processing:vt** ZSET.
	 * 2) **Deserialization & validation** - Converts each reserved raw payload with
	 *    `fromPayload(raw)` and ensures the minimal task shape is valid:
	 *    `iterationId`, `id`, `queueName`, `maxAttempts`, `currentAttempt`.
	 *    Invalid/partial items are **skipped**.
	 * 3) **In-flight tracking** - For each valid task:
	 *    - Caches the raw payload in **`processingRaw`** keyed by `task.id`.
	 *    - Starts a **heartbeat** via {@link startHeartbeat} to extend its visibility
	 *      before the VT expires (prevents premature requeue while executing).
	 *
	 * The returned array contains only the **valid** `Task` objects that are now
	 * considered “in-flight” for the current worker.
	 *
	 * ---
	 * ### Keys used
	 * - `ready`        → {@link readyKey}(queueName)  
	 * - `processing`   → {@link processingKey}(queueName)  
	 * - `processingVt` → {@link processingVtKey}(queueName)
	 *
	 * ---
	 * ### Visibility & heartbeats
	 * - Each reserved item receives a deadline of `now + visibilityTimeoutSec`.  
	 * - {@link startHeartbeat} refreshes that deadline periodically
	 *   (≈ 40% of `visibilityTimeoutSec`) until the task is acknowledged in {@link ack}.
	 *
	 * ---
	 * Performance:
	 * - Single batched reservation call via {@link reserveMany}; Lua-backed for atomicity
	 *   when available, with a client-side fallback.  
	 * - Deserialization/validation is linear in the number of reserved items.
	 *
	 * Complexity:
	 * - Time: `O(n)` where `n = raws.length` (deserialization + validation).  
	 * - Space: `O(n)` for the returned task list and `processingRaw` entries.
	 *
	 * Security:
	 * - Payloads are treated as untrusted input; validation ensures minimal fields only.  
	 * - Business-level validation should occur in {@link beforeExecution}/{@link execute}.
	 *
	 * @param queueName - Logical name of the queue to fetch tasks from. Must be valid;
	 *   the method derives `ready`, `processing`, and `processingVt` keys from it.
	 *
	 * @returns
	 * A promise resolving to an array of **validated** {@link Task} objects that were
	 * reserved and are now under this worker’s responsibility for the current iteration.
	 * Returns an **empty array** if no items were reserved.
	 *
	 * @example
	 * ```ts
	 * // Inside the main loop:
	 * const tasks = await this.data('emails');
	 * if (tasks.length) {
	 *   await this.iteration(tasks);
	 * } else {
	 *   await wait(this.iterationTimeout);
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Internal Pipeline
	 * @see {@link reserveMany} - Atomic batch reservation from ready → processing + VT.
	 * @see {@link startHeartbeat} - Keeps processing items invisible while executing.
	 * @see {@link ack} - Removes the item from processing & VT once done.
	 * @see {@link iteration} - Consumes the tasks returned here.
	 */
	private async data(queueName: string): Promise<Task[]> {
		const ready = this.readyKey(queueName);
		const processing = this.processingKey(queueName);
		const processingVt = this.processingVtKey(queueName);
		const raws = await this.reserveMany(ready, processing, processingVt, this.portionLength, this.visibilityTimeoutSec);

		if (!isArrFilled(raws)) {
			return [];
		}
		const tasks: Task[] = [];

		for (const raw of raws) {
			const obj = this.fromPayload(raw);

			if (isObjFilled(obj) 
				&& isStrFilled((obj as any).iterationId)
				&& isStrFilled((obj as any).id) 
				&& isStrFilled((obj as any).queueName)
				&& isNumPZ((obj as any).maxAttempts)
				&& isNumPZ((obj as any).currentAttempt)) {
				const t = obj as Task;

				this.processingRaw.set(t.id, raw);
				this.startHeartbeat(t);
				tasks.push(t);
			}
		}
		return tasks;
	}

	/**
	 * @summary
	 * Acknowledges a **completed task** by removing its raw payload from the
	 * in-memory tracking map and from Redis **processing structures** (LIST + VT ZSET).
	 *
	 * @remarks
	 * This internal helper finalizes a task after its execution pipeline finishes
	 * (success, fail, or error path). It performs a **best-effort** ACK:
	 *
	 * 1) **Lookup** the raw serialized payload for `task.id` in the local
	 *    `processingRaw` map (set earlier in {@link data}).  
	 * 2) If found, **delete** the entry from `processingRaw` to prevent memory leaks.  
	 * 3) **Atomically remove** the payload from the Redis **processing** LIST and its
	 *    corresponding **processing:vt** ZSET member via {@link ackProcessing}.
	 *
	 * If the raw entry is missing (e.g., already cleaned up or never reserved), the
	 * method simply returns. All errors are **suppressed** to avoid disrupting the
	 * main worker loop - downstream reliability relies on visibility timeouts and
	 * requeue logic anyway.
	 *
	 * ---
	 * ### When is this called?
	 * It is invoked from the `finally` block of the per-task pipeline
	 * (see {@link logic}), right after the heartbeat is stopped:
	 * - **Success path**: after {@link success} and {@link afterExecution}.  
	 * - **Failure path**: after {@link retry}/{@link error} handling.  
	 * - Always attempts to ACK to keep Redis processing state clean.
	 *
	 * ---
	 * ### Redis keys used
	 * - `processing`   → {@link processingKey}(task.queueName)  
	 * - `processingVt` → {@link processingVtKey}(task.queueName)
	 *
	 * Removal is delegated to {@link ackProcessing}, which executes a small Redis
	 * transaction (`LREM` + `ZREM`) to delete the raw payload from both structures.
	 *
	 * ---
	 * Performance:
	 * - O(1) local operations; a single small Redis transaction inside {@link ackProcessing}.  
	 * - Designed to be lightweight; failures are swallowed to maintain loop stability.
	 *
	 * Complexity:
	 * - Time: O(1). Space: O(1).
	 *
	 * Security:
	 * - Only handles identifiers and raw payload tokens; does not expose user data.  
	 * - Raw payload values should be considered opaque/serialized blobs.
	 *
	 * @param task - The {@link Task} that just completed processing (success or failure).
	 *   Its `id` is used to locate the cached raw payload; `queueName` derives the Redis keys.
	 *
	 * @returns
	 * A `Promise<void>` that resolves after best-effort cleanup. Errors are intentionally
	 * suppressed to avoid cascading failures.
	 *
	 * @example
	 * ```ts
	 * // You do not call ack() directly; it is called by the pipeline:
	 * try {
	 *   // ... run execute(), handle success/retry ...
	 * } finally {
	 *   this.stopHeartbeat(task);
	 *   await this.ack(task).catch(() => {}); // best-effort
	 * }
	 * ```
	 *
	 * @since 2.0.0
	 * @category Internal Pipeline
	 * @see {@link data} - Sets `processingRaw` and starts heartbeats for reserved tasks.
	 * @see {@link ackProcessing} - Removes the raw payload from Redis processing structures.
	 * @see {@link stopHeartbeat} - Stops visibility extension before acknowledging.
	 * @see {@link processingKey} | {@link processingVtKey} - Keys used for cleanup.
	 */
	private async ack(task: Task): Promise<void> {
		try {
			const raw = this.processingRaw.get(task.id);

			if (!isStrFilled(raw)) {
				return;
			}
			this.processingRaw.delete(task.id);

			await this.ackProcessing(this.processingKey(task.queueName), this.processingVtKey(task.queueName), raw);
		}
		catch {
		}
	}
}