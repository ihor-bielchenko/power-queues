
/**
 * A highly optimized Lua script used by {@link PowerQueues} to push multiple
 * entries into a Redis Stream in a **single XADD call per entry**, while also
 * applying optional trimming rules (`MAXLEN`, `MINID`, `LIMIT`, `NOMKSTREAM`).
 *
 * This script enables extremely fast bulk insertion and efficient memory
 * management, and is one of the core reasons `PowerQueues` can handle very
 * high throughput.
 *
 * ---
 * # What the script does
 *
 * For each batch of tasks:
 *
 * 1. Builds a shared list of trim options (e.g. `MAXLEN`, `MINID`, `LIMIT`,
 *    approximate `~` or exact `=` modes).
 * 2. Computes optional **MINID time-window trimming** using server time.
 * 3. Iterates over all N tasks in the batch:
 *    - Takes the task-provided ID (`*` or custom).
 *    - Appends all field/value pairs.
 *    - Issues a Redis `XADD` with all options + payload.
 * 4. Collects and returns the generated Stream IDs as an array.
 *
 * Each XADD is executed *inside Redis* without round-trips, making this
 * script significantly faster than repeated client-side XADD calls.
 *
 * ---
 * # Supported trimming modes
 *
 * The script can apply:
 *
 * - **MAXLEN** - keep only last N entries.  
 * - **MINID** - remove entries older than timestamp cutoff.  
 * - **LIMIT** - bound how many entries may be removed during trimming.  
 * - **NOMKSTREAM** - prevent auto-creation of the stream.  
 * - **Exact or approximate** trims (`=` or `~`).  
 *
 * All trimming logic is compiled once and reused for the entire batch,
 * drastically reducing overhead for large insert workloads.
 *
 * ---
 * # Parameters (from PowerQueues.payloadBatch)
 *
 * **KEYS[1]**  
 * The target Redis Stream name.
 *
 * **ARGV** contains:
 *
 * | Index | Meaning |
 * |-------|---------|
 * | 1     | `maxlen` (0 = disabled) |
 * | 2     | `approxFlag` (`1` = ~, `0` = exact unless overridden) |
 * | 3     | Number of entries (`n`) being added |
 * | 4     | `exactFlag` (`1` = =) |
 * | 5     | `nomkstream` (`1` = do not auto-create stream) |
 * | 6     | `trimLimit` (0 = no limit) |
 * | 7     | `minidWindowMs` (0 = disabled) |
 * | 8     | `minidExact` (`1` = use exact MINID) |
 * | 9+    | For each entry: ID, number of pairs, then field/value items |
 *
 * ---
 * # Performance characteristics
 *
 * - **CPU efficient**: avoids rebuilding trim options for each entry.
 * - **Network efficient**: only one `EVALSHA` round-trip per *batch*.
 * - **Memory efficient**: uses Lua tables with linear iteration.
 * - **Throughput**: supports thousands of messages per second per worker.
 *
 * ---
 * # Example usage (internal)
 *
 * ```ts
 * await this.runScript('XAddBulk', [ queueName ], argv, XAddBulk);
 * ```
 *
 * The script itself is stored as a raw string literal and loaded into Redis
 * once using `SCRIPT LOAD`. After that, all calls use `EVALSHA`.
 */
export const XAddBulk = `
	local UNPACK = table and table.unpack or unpack

	local stream = KEYS[1]
	local maxlen = tonumber(ARGV[1])
	local approxFlag = tonumber(ARGV[2]) == 1
	local n = tonumber(ARGV[3])
	local exactFlag = tonumber(ARGV[4]) == 1
	local nomkstream = tonumber(ARGV[5]) == 1
	local trimLimit = tonumber(ARGV[6])
	local minidWindowMs = tonumber(ARGV[7]) or 0
	local minidExact = tonumber(ARGV[8]) == 1
	local idx = 9
	local out = {}

	local common_opts = {}
	local co_len = 0

	if nomkstream then
		co_len = co_len + 1; common_opts[co_len] = 'NOMKSTREAM'
	end

	if minidWindowMs > 0 then
		local tm = redis.call('TIME')
		local now_ms = (tonumber(tm[1]) * 1000) + math.floor(tonumber(tm[2]) / 1000)
		local cutoff_ms = now_ms - minidWindowMs
		if cutoff_ms < 0 then cutoff_ms = 0 end
		local cutoff_id = tostring(cutoff_ms) .. '-0'

		co_len = co_len + 1; common_opts[co_len] = 'MINID'
		co_len = co_len + 1; common_opts[co_len] = (minidExact and '=' or '~')
		co_len = co_len + 1; common_opts[co_len] = cutoff_id
		if trimLimit and trimLimit > 0 then
			co_len = co_len + 1; common_opts[co_len] = 'LIMIT'
			co_len = co_len + 1; common_opts[co_len] = trimLimit
		end
	elseif maxlen and maxlen > 0 then
		co_len = co_len + 1; common_opts[co_len] = 'MAXLEN'
		if exactFlag then
			co_len = co_len + 1; common_opts[co_len] = '='
		elseif approxFlag then
			co_len = co_len + 1; common_opts[co_len] = '~'
		end
		co_len = co_len + 1; common_opts[co_len] = maxlen
		if trimLimit and trimLimit > 0 then
			co_len = co_len + 1; common_opts[co_len] = 'LIMIT'
			co_len = co_len + 1; common_opts[co_len] = trimLimit
		end
	end

	for e = 1, n do
		local id = ARGV[idx]; idx = idx + 1
		local num_pairs = tonumber(ARGV[idx]); idx = idx + 1

		local a = {}
		local a_len = 0

		for i = 1, co_len do a_len = a_len + 1; a[a_len] = common_opts[i] end

		a_len = a_len + 1; a[a_len] = id

		for j = 1, (num_pairs * 2) do
			a_len = a_len + 1; a[a_len] = ARGV[idx]; idx = idx + 1
		end

		local addedId = redis.call('XADD', stream, UNPACK(a))
		out[#out+1] = addedId or ''
	end

	return out
`;

/**
 * A Lua script that acknowledges and optionally deletes processed Redis Stream
 * entries for a given consumer group.
 *
 * This script is used internally by {@link PowerQueues} after successful task
 * execution. It ensures consistent and efficient cleanup while gracefully
 * handling Redis limitations around bulk deletion.
 *
 * ---
 * # What the script does
 *
 * 1. Extracts:
 *    - the target Stream name (`KEYS[1]`),
 *    - the consumer group (`ARGV[1]`),
 *    - whether deletion is requested (`ARGV[2]`),
 *    - and the list of message IDs to approve (`ARGV[3+]`).
 *
 * 2. Calls `XACK` to mark all messages as processed for the given group.
 *
 * 3. If deletion (`delFlag = 1`) is enabled:
 *    - Attempts a single `XDEL` call with all IDs.
 *    - If Redis returns an error (e.g., too many arguments),  
 *      it **falls back** to deleting entries **one by one**, ensuring reliability.
 *
 * 4. Returns the number of messages successfully acknowledged.
 *
 * ---
 * # Why this script exists
 *
 * Redis Stream consumers must manually:
 * - acknowledge processed messages (`XACK`),
 * - optionally delete entries to reduce retention (`XDEL`).
 *
 * Doing this in JavaScript for thousands of messages would require multiple
 * network round-trips. This script performs both operations **inside Redis**
 * in a single atomic execution.
 *
 * Benefits:
 * - Lower latency  
 * - Fewer Redis round-trips  
 * - Graceful fallback for large XDEL sets  
 * - Cleaner Stream memory usage when `removeOnExecuted = true`
 *
 * ---
 * # Parameters
 *
 * **KEYS[1]**  
 * The Stream name from which messages are being acknowledged.
 *
 * **ARGV[1]**  
 * Consumer group name.
 *
 * **ARGV[2]**  
 * Delete flag (`"1"` = delete entries after ack, `"0"` = keep entries).
 *
 * **ARGV[3...]**  
 * List of message IDs to acknowledge (and optionally delete).
 *
 * ---
 * # Return value
 *
 * Returns the number of acknowledged messages as an integer.
 *
 * ---
 * # Example usage (internal)
 *
 * ```ts
 * const approved = await this.runScript(
 *   'Approve',
 *   [this.stream],
 *   [this.group, '1', ...ids],
 *   Approve
 * );
 * ```
 *
 * ---
 * # Error handling
 *
 * The script is defensive:
 *
 * - If `XDEL` with many IDs fails (common when argument list too long),
 *   it automatically retries deletion entry-by-entry.
 * - Ensures correctness even when Redis returns partial results.
 *
 * ---
 */
export const Approve = `
	local stream = KEYS[1]
	local group = ARGV[1]
	local delFlag = tonumber(ARGV[2]) == 1

	local acked = 0
	local nids = #ARGV - 2
	if nids > 0 then
		acked = tonumber(redis.call('XACK', stream, group, unpack(ARGV, 3))) or 0
		if delFlag and nids > 0 then
			local ok, deln = pcall(redis.call, 'XDEL', stream, unpack(ARGV, 3))
			if not ok then
				deln = 0
				for i = 3, #ARGV do
					deln = deln + (tonumber(redis.call('XDEL', stream, ARGV[i])) or 0)
				end
			end
		end
	end
	return acked
`;

/**
 * Lua script that performs the **first step** of idempotent task execution:
 * it decides whether a worker is allowed to execute a task, based on the
 * current idempotency keys.
 *
 * This is used internally by {@link PowerQueues.idempotencyAllow} before
 * any actual work is done for a given `idemKey`.
 *
 * ---
 * # What the script checks
 *
 * 1. **Is the task already completed?**
 *    - If `doneKey` exists → the task has been processed in the past.
 *    - In this case, the script returns `1` ("already done, skip").
 *
 * 2. **Can the worker acquire a lock?**
 *    - If `doneKey` is missing, the script attempts to set `lockKey` with
 *      a unique token and TTL.
 *    - Uses `SET lockKey token NX PX ttl`.
 *    - On success, it optionally sets `startKey` (with the same TTL) and
 *      returns `2` ("lock acquired, proceed to execute").
 *
 * 3. **Otherwise**
 *    - If neither `doneKey` exists nor the lock can be acquired, it returns `0`
 *      ("another worker is handling this, do not execute now").
 *
 * ---
 * # Keys
 *
 * **KEYS[1]** - `doneKey`  
 * Indicates that the task has been successfully processed at least once.  
 * If present, the script immediately returns `1`.
 *
 * **KEYS[2]** - `lockKey`  
 * A short-lived lock used to allow **exactly one worker** to start executing
 * the task. Stored with a random token to prevent accidental unlocks.
 *
 * **KEYS[3]** - `startKey`  
 * Optional key signaling that execution has started.  
 * Helps other workers detect that someone is currently processing the task.
 *
 * ---
 * # Arguments
 *
 * **ARGV[1]** - `ttl` (milliseconds)  
 * Lifetime for both `lockKey` and `startKey`. If `ttl <= 0`, the script
 * immediately returns `0` (no lock can be taken).
 *
 * **ARGV[2]** - `token`  
 * A unique value used to identify the worker that holds the lock.
 *
 * ---
 * # Return codes
 *
 * - `0` - **Do not execute**  
 *   - Either `ttl <= 0`, or  
 *   - Lock could not be acquired (another worker holds it).
 *
 * - `1` - **Already completed**  
 *   - `doneKey` exists → the task was processed in a previous run and should
 *     be skipped without doing any work.
 *
 * - `2` - **Lock acquired, proceed**  
 *   - `lockKey` was successfully set.
 *   - `startKey` is also set (if provided) with the same TTL.
 *   - The caller may proceed to {@link IdempotencyStart} / execute the job.
 *
 * ---
 * # Usage (internal)
 *
 * This script is not called directly. It is wrapped by
 * {@link PowerQueues.idempotencyAllow}, which:
 *
 * - builds the key names via {@link PowerQueues.idempotencyKeys},
 * - passes `workerExecuteLockTimeoutMs` as TTL,
 * - interprets the result as `0 | 1 | 2` to decide the next step:
 *   - `1` → short-circuit success,
 *   - `0` → wait / contend,
 *   - `2` → proceed with execution.
 */
export const IdempotencyAllow = `
	local doneKey = KEYS[1]
	local lockKey = KEYS[2]
	local startKey = KEYS[3]

	if redis.call('EXISTS', doneKey) == 1 then
		return 1
	end

	local ttl = tonumber(ARGV[1]) or 0
	if ttl <= 0 then return 0 end

	local ok = redis.call('SET', lockKey, ARGV[2], 'NX', 'PX', ttl)
	if ok then
		if startKey and startKey ~= '' then
			redis.call('SET', startKey, 1, 'PX', ttl)
		end
		return 2
	else
		return 0
	end
`;

/**
 * Lua script that performs the **second step** of idempotent task execution:
 * it confirms that the current worker still owns the lock and (re)starts
 * the execution window by refreshing TTLs.
 *
 * This script is used internally by {@link PowerQueues.idempotencyStart} after
 * {@link IdempotencyAllow} has returned `2` (lock acquired).
 *
 * ---
 * # What the script does
 *
 * 1. Verifies that the worker still owns the lock:
 *    - Compares the value stored in `lockKey` with the provided `token`
 *      (`ARGV[1]`).
 *    - If they do **not** match, another worker has taken over or the lock
 *      has been lost → returns `0` (do not execute).
 *
 * 2. If the lock is valid:
 *    - Reads `ttl` from `ARGV[2]`.
 *    - If `ttl > 0`:
 *      - Sets `startKey = 1` with TTL `ttl` (PX).
 *      - Extends the lock TTL using `PEXPIRE(lockKey, ttl)`.
 *    - If `ttl <= 0`:
 *      - Sets `startKey = 1` without TTL.
 *
 * 3. Returns `1` to indicate that the worker may proceed with execution.
 *
 * This step is especially useful before long-running tasks or when workers
 * want to refresh their execution window to avoid the lock expiring mid-job.
 *
 * ---
 * # Keys
 *
 * **KEYS[1]** - `lockKey`  
 * Stores the unique token of the worker that owns the idempotency lock.
 *
 * **KEYS[2]** - `startKey`  
 * Marks that execution has started (or is still active) for the current worker.
 *
 * ---
 * # Arguments
 *
 * **ARGV[1]** - `token`  
 * The unique token assigned when acquiring the lock in {@link IdempotencyAllow}.  
 * Must match the value stored in `lockKey` for this script to succeed.
 *
 * **ARGV[2]** - `ttl` (milliseconds)  
 * New TTL for both `lockKey` (via `PEXPIRE`) and `startKey` (via `SET` with `PX`).  
 * If `ttl <= 0`, `startKey` is set without TTL, and `lockKey` is left unchanged.
 *
 * ---
 * # Return codes
 *
 * - `0` - **Lock not owned by this worker**  
 *   - `lockKey` does not contain the provided `token`.  
 *   - Caller should treat this as contention and not execute the task.
 *
 * - `1` - **Lock confirmed, TTL refreshed**  
 *   - `lockKey` matches the `token`.  
 *   - `startKey` has been updated (with or without TTL).  
 *   - Safe to proceed with the task.
 *
 * ---
 * # Usage (internal)
 *
 * This script is called from {@link PowerQueues.idempotencyStart}, which:
 *
 * - passes the `token` generated in {@link PowerQueues.idempotencyKeys},
 * - uses `workerExecuteLockTimeoutMs` as the TTL,
 * - expects `1` to proceed with execution, and treats `0` as contention.
 */
export const IdempotencyStart = `
	local lockKey = KEYS[1]
	local startKey = KEYS[2]
	if redis.call('GET', lockKey) == ARGV[1] then
		local ttl = tonumber(ARGV[2]) or 0
		if ttl > 0 then
			redis.call('SET', startKey, 1, 'PX', ttl)
			redis.call('PEXPIRE', lockKey, ttl)
		else
			redis.call('SET', startKey, 1)
		end
		return 1
	end
	return 0
`;

/**
 * Lua script that performs the **final step** of idempotent task execution:
 * it marks the task as completed and safely releases the lock owned by
 * the current worker.
 *
 * This script is used internally by {@link PowerQueues.idempotencyDone} after
 * `onExecute` finishes without throwing an error.
 *
 * ---
 * # What the script does
 *
 * 1. **Marks the task as done**
 *    - Sets `doneKey = 1`.
 *    - Optionally applies a TTL (in **milliseconds**) to `doneKey` so the
 *      completion marker expires after some time.
 *
 * 2. **Releases the lock (if still owned)**
 *    - Reads `lockKey` and compares it with the provided `token` (`ARGV[2]`).
 *    - If the token matches, it:
 *      - deletes `lockKey`,
 *      - deletes `startKey` (if provided).
 *
 * 3. Always returns `1` to indicate that the cleanup step has completed.
 *
 * The presence of `doneKey` tells all future workers that the task has already
 * been successfully processed and can be skipped.
 *
 * ---
 * # Keys
 *
 * **KEYS[1] - `doneKey`**  
 * - Marks the task as "successfully completed".  
 * - A small TTL keeps memory usage under control while still allowing workers
 *   to quickly skip duplicates.
 *
 * **KEYS[2] - `lockKey`**  
 * - Holds the lock token for the current worker.  
 * - Only removed if it still matches the worker’s token.
 *
 * **KEYS[3] - `startKey`**  
 * - Indicates that the task execution was started.  
 * - Removed together with `lockKey` when the worker finishes and still owns
 *   the lock.
 *
 * ---
 * # Arguments
 *
 * **ARGV[1] - `ttlMs`**  
 * - TTL for `doneKey` in **milliseconds**.  
 * - If `ttlMs > 0`, the script calls `PEXPIRE(doneKey, ttlMs)`.  
 * - If `ttlMs <= 0`, `doneKey` is set without TTL and will persist until
 *   manually removed or Redis evicts it.
 *
 * **ARGV[2] - `token`**  
 * - Unique lock token for the worker (generated in {@link PowerQueues.idempotencyKeys}).  
 * - Used to verify that this worker still owns `lockKey` before deleting it.
 *
 * ---
 * # Return value
 *
 * - Always returns `1` after performing its operations.
 *
 * ---
 * # Usage (internal)
 *
 * This script is called by {@link PowerQueues.idempotencyDone}, which passes:
 *
 * - `doneKey`, `lockKey`, `startKey` as KEYS,
 * - `workerCacheTaskTimeoutMs` as `ttlMs`,
 * - the worker’s `token` as the second argument.
 *
 * After this:
 * - The task is marked as completed (idempotent skip-able).
 * - The lock and start markers are cleaned up if the worker still owns them.
 */
export const IdempotencyDone = `
	local doneKey  = KEYS[1]
	local lockKey  = KEYS[2]
	local startKey = KEYS[3]

	redis.call('SET', doneKey, 1)

	local ttlMs = tonumber(ARGV[1]) or 0
	if ttlMs > 0 then
		redis.call('PEXPIRE', doneKey, ttlMs)
	end

	if redis.call('GET', lockKey) == ARGV[2] then
		redis.call('DEL', lockKey)
		if startKey then
			redis.call('DEL', startKey)
		end
	end

	return 1
`;

/**
 * Lua script that **safely releases** idempotency lock keys when a worker
 * decides to give up processing a task (for example, after a failure or
 * when deferring execution).
 *
 * This is used internally by {@link PowerQueues.idempotencyFree} as a
 * counterpart to {@link IdempotencyAllow} and {@link IdempotencyStart}.
 *
 * It does **not** touch the `doneKey` and **does not mark the task as completed**.
 * It only frees the lock so that another worker may later try to process the
 * same idempotency key again.
 *
 * ---
 * # What the script does
 *
 * 1. Reads the value of `lockKey` from Redis.
 * 2. Compares it with the provided `token` (`ARGV[1]`).
 * 3. If the token matches:
 *    - Deletes `lockKey`.
 *    - Deletes `startKey` (if provided).
 *    - Returns `1` (successfully freed).
 * 4. If the token does **not** match:
 *    - Returns `0` (do nothing; another worker owns the lock or it’s gone).
 *
 * This ensures that **only the worker who owns the lock** can release it,
 * preventing accidental unlocks by other workers.
 *
 * ---
 * # Keys
 *
 * **KEYS[1] - `lockKey`**  
 * - The lock key that protects idempotent execution.  
 * - Contains a worker-specific `token` set by {@link IdempotencyAllow}.
 *
 * **KEYS[2] - `startKey`**  
 * - Optional key indicating that execution has started.  
 * - Deleted together with `lockKey` when the worker successfully frees the lock.
 *
 * ---
 * # Arguments
 *
 * **ARGV[1] - `token`**  
 * - The unique token assigned to the worker when it acquired the lock.  
 * - Must match the value stored in `lockKey` for the script to delete it.
 *
 * ---
 * # Return codes
 *
 * - `1` - Lock successfully released (and `startKey` deleted if present).  
 * - `0` - Lock not released (token mismatch or lock missing).
 *
 * ---
 * # When it is used
 *
 * This script is typically called when:
 *
 * - A worker fails to complete the task but wants to **give up ownership**
 *   (for example, after scheduling a retry).
 * - The worker encounters an error during idempotent execution and wants
 *   to allow other workers to attempt the same task later.
 *
 * It is called from {@link PowerQueues.idempotencyFree}, which:
 *
 * - builds the idempotency key set via {@link PowerQueues.idempotencyKeys},
 * - passes the same `token` that was used to acquire the lock.
 */
export const IdempotencyFree = `
	local lockKey  = KEYS[1]
	local startKey = KEYS[2]
	if redis.call('GET', lockKey) == ARGV[1] then
		redis.call('DEL', lockKey)
		if startKey then redis.call('DEL', startKey) end
		return 1
	end
	return 0
`;

/**
 * Lua script that finds and reclaims **stuck** (long-idle) messages from a
 * Redis Stream consumer group, then, if needed, falls back to reading **fresh**
 * messages.
 *
 * This script is used internally by {@link PowerQueues.selectStuck} as the
 * first step in every consumer loop to:
 *
 * - rescue messages that were delivered but never acknowledged
 *   (because a worker crashed, stalled, or lost connection),
 * - keep the queue moving even when some workers die,
 * - respect a small time + iteration budget to avoid blocking Redis too long.
 *
 * ---
 * # High-level behaviour
 *
 * 1. **Try to reclaim stuck messages** using `XAUTOCLAIM`:
 *    - Only messages idle longer than `pendingIdleMs` are considered.
 *    - Reclaims up to `count` messages, iterating in small batches.
 *    - Stops if:
 *      - enough messages were collected, or
 *      - `max_iters` is reached, or
 *      - `timeBudgetMs` (wall-clock time limit) is exceeded.
 *
 * 2. If fewer than `count` messages could be reclaimed:
 *    - Reads **fresh** messages with `XREADGROUP` (using `>`),
 *    - Up to the remaining number (`count - collected`),
 *    - Adds them to the result as well.
 *
 * 3. Returns a flat array of entries in the same structure as returned by
 *    `XREADGROUP` / `XAUTOCLAIM`, ready to be post-processed by
 *    {@link PowerQueues.normalizeEntries}.
 *
 * This makes the worker **prioritize stuck work** but still continue with new
 * messages if there is nothing to reclaim.
 *
 * ---
 * # Keys
 *
 * **KEYS[1] - `stream`**  
 * The name of the Redis Stream being consumed (e.g. `"queue:emails"`).
 *
 * ---
 * # Arguments (ARGV)
 *
 * | Index | Name              | Type    | Description                                                |
 * |-------|-------------------|---------|------------------------------------------------------------|
 * | 1     | `group`           | string  | Consumer group name.                                       |
 * | 2     | `consumer`        | string  | Current consumer name (e.g. `"host:pid"`).                 |
 * | 3     | `pendingIdleMs`   | number  | Minimum idle time in ms for a message to count as "stuck". |
 * | 4     | `count`           | number  | Desired number of messages to reclaim/read.                |
 * | 5     | `timeBudgetMs`    | number  | Max time (ms) the script is allowed to run.                |
 *
 * **Notes:**
 * - `count < 1` is normalized to `1`.
 * - `timeBudgetMs` defaults to `15` ms if not provided.
 *
 * ---
 * # Internal algorithm
 *
 * 1. Initialize:
 *    - `start_ms` - current Redis time in ms (via `TIME`),
 *    - `results` - empty array for collected entries,
 *    - `start_id` - `"0-0"` (starting point for `XAUTOCLAIM`),
 *    - A helper `time_exceeded()` to check if `(now - start_ms) >= timeBudgetMs`,
 *    - `max_iters` - safety cap (at least `16`, or `ceil(count / 100)`).
 *
 * 2. **Reclaim loop** (while `collected < count` and `iters < max_iters`):
 *    - Compute `to_claim = count - collected`.
 *    - Call:
 *      ```lua
 *      XAUTOCLAIM stream group consumer pendingIdleMs start_id COUNT to_claim
 *      ```
 *    - Result format:
 *      - `claim[1]` - `next_id` (where to continue from),
 *      - `claim[2]` - array of entries `[ [id, [field, value, ...]], ... ]`.
 *    - Append all returned entries to `results`.
 *    - Update `start_id`:
 *      - If `next_id` == previous `start_id`, bump sequence part to avoid
 *        infinite loops (`"s-1" → "s-2"` etc.).
 *    - Break if `time_exceeded()` returns true.
 *
 * 3. **If still need more messages**:
 *    - Compute `left = count - collected`.
 *    - If `left > 0`, call:
 *      ```lua
 *      XREADGROUP GROUP group consumer COUNT left STREAMS stream >
 *      ```
 *    - Append all new entries (if any) to `results`.
 *
 * 4. Return `results`:
 *    - An array of raw entries in the same format as returned by `XREADGROUP`.
 *
 * ---
 * # Return value
 *
 * An array like:
 * ```lua
 * {
 *   { "id-1", { "field1", "value1", "field2", "value2", ... } },
 *   { "id-2", { "field1", "value1", ... } },
 *   ...
 * }
 * ```
 *
 * It is later normalized by {@link PowerQueues.normalizeEntries} into a
 * structured `[id, payload, createdAt, job, idemKey]` tuple.
 *
 * ---
 * # Error handling
 *
 * - If the consumer group does not exist (`NOGROUP`), the caller
 *   ({@link PowerQueues.selectStuck}) will handle this by calling
 *   {@link PowerQueues.createGroup} and retrying.
 * - The script itself focuses only on selection/reclaiming and assumes
 *   keys / groups are valid.
 */
export const SelectStuck = `
	local stream = KEYS[1]
	local group = ARGV[1]
	local consumer = ARGV[2]
	local pendingIdleMs = tonumber(ARGV[3])
	local count = tonumber(ARGV[4]) or 0
	if count < 1 then count = 1 end

	local timeBudgetMs = tonumber(ARGV[5]) or 15
	local t0 = redis.call('TIME')
	local start_ms = (tonumber(t0[1]) * 1000) + math.floor(tonumber(t0[2]) / 1000)

	local results = {}
	local collected = 0
	local start_id = '0-0'
	local iters = 0
	local max_iters = math.max(16, math.ceil(count / 100))

	local function time_exceeded()
		local t1 = redis.call('TIME')
		local now_ms = (tonumber(t1[1]) * 1000) + math.floor(tonumber(t1[2]) / 1000)
		return (now_ms - start_ms) >= timeBudgetMs
	end

	while (collected < count) and (iters < max_iters) do
		local to_claim = count - collected
		if to_claim < 1 then break end

		local claim = redis.call('XAUTOCLAIM', stream, group, consumer, pendingIdleMs, start_id, 'COUNT', to_claim)
		iters = iters + 1

		local bucket = nil
		if claim then
			bucket = claim[2]
		end
		if bucket and #bucket > 0 then
			for i = 1, #bucket do
				results[#results+1] = bucket[i]
			end
			collected = #results
		end

		local next_id = claim and claim[1] or start_id
		if next_id == start_id then
			local s, seq = string.match(start_id, '^(%d+)%-(%d+)$')
			if s and seq then
				start_id = s .. '-' .. tostring(tonumber(seq) + 1)
			else
				start_id = '0-1'
			end
		else
			start_id = next_id
		end

		if time_exceeded() then
			break
		end
	end

	local left = count - collected
	if left > 0 then
		local xr = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', left, 'STREAMS', stream, '>')
		if xr and xr[1] and xr[1][2] then
			local entries = xr[1][2]
			for i = 1, #entries do
				results[#results+1] = entries[i]
			end
		end
	end

	return results
`;
