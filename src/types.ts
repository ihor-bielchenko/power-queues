import type { Jsonish } from 'power-redis';

/**
 * @summary
 * A normalized map representing the **result data** returned by a task execution.
 *
 * @remarks
 * Each task may produce structured output (status objects, partial results, metrics, etc.)
 * that are merged across multiple lifecycle hooks — such as
 * {@link PowerQueue.afterExecution} and {@link PowerQueue.onSuccess}.
 *
 * The keys are arbitrary strings, and the values can be any **JSON-like**
 * (serializable) value supported by {@link Jsonish}.
 *
 * @example
 * ```ts
 * const result: TaskResult = {
 *   status: 'ok',
 *   durationMs: 152,
 *   response: { code: 200, message: 'OK' }
 * };
 * ```
 *
 * @since 2.0.0
 * @category Task Model
 */
export type TaskResult = Record<string, Jsonish>;

/**
 * @summary
 * Describes the **multi-queue execution chain** configuration for a task.
 *
 * @remarks
 * A task chain represents a sequential flow of queues.  
 * When a task completes successfully in one queue, it is automatically
 * enqueued into the next queue in the chain — until all queues are processed.
 *
 * Example: `['resize', 'compress', 'upload']`  
 * means that after a task finishes in the `"resize"` queue, it will
 * automatically move to `"compress"`, then `"upload"`.
 *
 * @property queues - Ordered list of queue names forming the execution chain.
 * @property index - Current **0-based index** pointing to the active queue
 *   within {@link TaskChain.queues}.
 *
 * @example
 * ```ts
 * const chain: TaskChain = {
 *   queues: ['prepare', 'process', 'finalize'],
 *   index: 1, // currently in "process"
 * };
 * ```
 *
 * @since 2.0.0
 * @category Task Model
 */
export type TaskChain = {
	/** Ordered list of queue names forming the execution pipeline. */
	queues: string[];

	/** Current 0-based position within {@link TaskChain.queues}. */
	index: number;
};

/**
 * @summary
 * Tracks the **temporal progress** and retry lifecycle of a task instance.
 *
 * @remarks
 * Each task stores its processing timestamps and retry moments for precise
 * auditability and performance tracking.
 *
 * The timestamps are expressed in **milliseconds since epoch (UNIX time)**.
 *
 * @property createdAt - Time when the task object was first created.
 * @property successAt - Time when the task (or its chain) completed successfully (0 if not yet).
 * @property errorAt - Time when a non-fatal recoverable error occurred (0 if never).
 * @property failAt - Time when all attempts were exhausted and the task was moved to a `fail` list (0 if never).
 * @property fatalAt - Time when an unrecoverable fatal condition occurred (0 if never).
 * @property retries - Array of timestamps when each retry attempt was initiated.
 * @property chain - Array of timestamps when each chain hop successfully completed.
 *
 * @example
 * ```ts
 * const progress: TaskProgress = {
 *   createdAt: Date.now(),
 *   successAt: 0,
 *   errorAt: 0,
 *   failAt: 0,
 *   fatalAt: 0,
 *   retries: [],
 *   chain: []
 * };
 * ```
 *
 * @since 2.0.0
 * @category Task Model
 */
export type TaskProgress = {
	/** Milliseconds since epoch when the task object was created. */
	createdAt: number;

	/** Milliseconds when the task chain completed successfully (0 if not yet). */
	successAt: number;

	/** Milliseconds when a non-fatal error was observed (0 if never). */
	errorAt: number;

	/** Milliseconds when task exhausted attempts and moved to a *fail* list (0 if never). */
	failAt: number;

	/** Milliseconds when an unrecoverable fatal condition occurred (0 if never). */
	fatalAt: number;

	/** Retry timestamps (ms) per attempt performed. */
	retries: number[];

	/** Timestamps (ms) when each chain hop completed. */
	chain: number[];
};

/**
 * @summary
 * Represents a **single job (task)** managed by the `PowerQueue` framework.
 *
 * @remarks
 * A `Task` object encapsulates:
 * - Its queue and iteration context.
 * - Retry and chain configuration.
 * - Mutable progress tracking.
 * - Payload (input) and result (output) data.
 *
 * It is the fundamental unit of work passed through the queue system
 * and processed by worker logic implemented in {@link PowerQueue.execute}.
 *
 * ### Life cycle
 * 1. **Enqueued** — via {@link PowerQueue.enqueue} or {@link PowerQueue.addTask}.
 * 2. **Reserved** — moved from `ready` to `processing` via Lua script.
 * 3. **Executed** — processed by worker code (`execute()`).
 * 4. **Acknowledged** — upon success (removed from `processing`).
 * 5. **Requeued or Failed** — upon error or exceeded retry limit.
 *
 * ### Field overview
 * | Property | Description |
 * |-----------|-------------|
 * | `queueName` | Queue identifier (base Redis key prefix). |
 * | `iterationId` | Correlation ID grouping tasks within a batch or iteration. |
 * | `iterationLength` | Total number of tasks in this iteration (informational). |
 * | `id` | Unique task identifier (UUID). |
 * | `maxAttempts` | Max retry attempts before marking as failed. |
 * | `currentAttempt` | Current 0-based retry attempt counter. |
 * | `chain` | Optional chain configuration for multi-queue routing. |
 * | `progress` | Mutable structure tracking timestamps and retries. |
 * | `payload` | Arbitrary JSON-compatible input object. |
 * | `result` | Mutable JSON-compatible output accumulator. |
 *
 * ### Serialization
 * Tasks are stored in Redis as JSON strings.
 * Both `payload` and `result` must be serializable and compact to avoid overhead.
 *
 * @example
 * ```ts
 * const task: Task = {
 *   queueName: 'emails',
 *   iterationId: 'iter-2025-10-21',
 *   iterationLength: 10,
 *   id: 'a47fdf38-1234-4c55-a9f9-cd89e42a7baf',
 *   maxAttempts: 3,
 *   currentAttempt: 0,
 *   chain: { queues: ['emails', 'logs'], index: 0 },
 *   progress: {
 *     createdAt: Date.now(),
 *     successAt: 0,
 *     errorAt: 0,
 *     failAt: 0,
 *     fatalAt: 0,
 *     retries: [],
 *     chain: [],
 *   },
 *   payload: { to: 'user@example.com', subject: 'Welcome!' },
 *   result: {},
 * };
 * ```
 *
 * @since 2.0.0
 * @category Task Model
 * @see {@link PowerQueue} — Abstract base class that manages task lifecycles.
 * @see {@link TaskProgress} — Tracks time-based lifecycle data.
 * @see {@link TaskChain} — Defines sequential queue execution flow.
 */
export type Task = {
	/** Logical queue name (base prefix used for keys). */
	queueName: string;

	/** Iteration correlation identifier for analytics and grouping. */
	iterationId: string;

	/** Number of tasks planned for the current iteration (informational). */
	iterationLength: number;

	/** Unique task id (UUID). */
	id: string;

	/** Max attempts permitted before moving to *fail* list. */
	maxAttempts: number;

	/** Current 0-based attempt index. */
	currentAttempt: number;

	/** Optional chain configuration. */
	chain: TaskChain;

	/** Mutable time/attempt history. */
	progress: TaskProgress;

	/**
	 * User payload: strongly recommended to be JSON-serializable.
	 * @defaultValue {}
	 */
	payload: Record<string, any>;

	/**
	 * Execution result accumulator (merged across before/after hooks and execute()).
	 * @defaultValue {}
	 */
	result: Record<string, any>;
};
