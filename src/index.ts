import type {
	TaskChain,
	TaskProgress,
	Task,
	TaskResult,
} from './types';
import { PowerQueue } from './PowerQueue';

export type {
	TaskChain,
	TaskProgress,
	Task,
	TaskResult,
};
export {
	PowerQueue,
};

/**
 * @packageDocumentation
 * @module PowerQueue
 *
 * @summary
 * A production-ready, **Redis-backed queue runner** with visibility timeouts, delayed scheduling,
 * chainable queues, retries with exponential backoff + jitter, and heartbeat renewals — built
 * on top of a thin `PowerRedis` abstraction.
 *
 * @remarks
 * This module exposes a base abstract class {@link PowerQueue} which you extend and implement
 * {@link PowerQueue.execute | execute()} (your business logic). It supports:
 *
 * - FIFO reservation (`LMOVE LEFT->RIGHT` preferred, `RPOPLPUSH` fallback) via Lua scripts or client loop.
 * - **Visibility timeout (VT)** with periodic heartbeat (ZSET scores store deadlines).
 * - **Delayed tasks** (ZSET with future timestamps) and promotion to ready list.
 * - **Retry policy** with exponential backoff + jitter and a **fail** queue after max attempts.
 * - **Chain mode**: automatically forward a task through a list of queues and call lifecycle hooks.
 * - Iteration loop with promote/requeue passes, concurrency slicing, and status counters with TTL.
 *
 * @since 2.0.0
 * @see {@link PowerRedis} for the Redis client wrapper this extends.
 * @see {@link https://redis.io/commands/lmove/ | Redis LMOVE}
 * @see {@link https://redis.io/commands/rpoplpush/ | Redis RPOPLPUSH}
 * @see {@link https://redis.io/commands/zadd/ | Redis ZADD}
 */