import type Redis from 'ioredis';
import type {
	AddTasksOpts,
	Task,
	Fields,
	EntryKeys,
	LoopOpts,
	SavedScript,
} from './types';
import {
	isArrFilled,
	isArr,
	isStrFilled,
	isObjFilled,
	isNumP,
	isExists,
	wait,
} from 'full-utils';
import {
	ackAndMaybeDeleteBatchScript,
	reservePendingOrNewEntriesScript,
	tryAcquireEntryLockScript,
	markEntryStartedScript,
	completeEntryAndReleaseLockScript,
	releaseEntryLockIfTokenScript,
	heartbeatScript,
	xaddBulkScript,
	names,
} from './scripts';
import {
	waitAbortable,
	randBase,
	randExtra,
} from './helpers';

declare module 'ioredis' {
	interface Redis {
		xaddBulk(key: string, ...args: string[]): Promise<string[]>;
	}
}

export class PowerQueues {
	private readonly scripts: Record<string, SavedScript> = {};
	private lastErrLog: number = 0;
	private lastLog: number = 0;
	private xaddDefined: boolean = false;
	public abort = new AbortController();
	public redis!: Redis;
	public readonly defaultChunkSize: number = 800;
	public readonly defaultParallelJobs: number = 1;
	public readonly maxArgsPerRedisCommand: number = 10000;
	public readonly consumerHostId: string = 'host';
	public readonly maxAckIdsPerBatch: number = 2000;
	public readonly tasksCountPerIteration: number = 100;
	public readonly streamReadTimeoutMs: number = 5000;
	public readonly pendingKeyTimeoutMs: number = 60000;
	public readonly idempotencyKeyTimeoutSec: number = 86400;
	public readonly processingLockTimeoutMs: number = 180000;
	public readonly scriptExecutionTimeLimitMs: number = 80;
	public readonly consoleErrorTimeoutMs: number = 5000;
	public readonly consoleLogTimeoutMs: number = 5000;
	public readonly deleteOnAck: boolean = false;
	public readonly logger: any = { 
		log: (...props) => console.log(...props), 
		error: (...props) => console.error(...props), 
	};

	async onExecute(id: string, data: Fields) {
	}

	async runQueue(stream: string, group: string, opts?: LoopOpts) {
		this.consoleLog(`runQueue: stream=${stream}, group=${group}`, 'runQueue');

		await this.createGroupIfNotExists(stream, group, '0-0');
		await this.consumerLoop(stream, group, this.resolveConsumerLoopOptions(opts));

		this.consoleLog(`runQueue: finished stream=${stream}, group=${group}`, 'runQueue');
	}

	async createGroupIfNotExists(stream: string, group: string, from: '$' | '0-0' = '$') {
		try {
			await (this.redis as any).xgroup('CREATE', stream, group, from, 'MKSTREAM');

			this.consoleLog(`group created: stream=${stream}, group=${group}`, 'createGroupIfNotExists');
		}
		catch (err: any) {
			const msg = String(err?.message || '');
			
			if (!msg.includes('BUSYGROUP')) {
				this.consoleError(err, 'createGroupIfNotExists');
				throw err;
			}
			this.consoleLog(`group exists: stream=${stream}, group=${group}`, 'createGroupIfNotExists');
		}
	}

	async consumerLoop(stream: string, group: string, opts: LoopOpts) {
		while (!opts.abort?.aborted) {
			try {
				const entries = await this.getNextMessagesFromStream(stream, group, opts);

				this.consoleLog(`consumerLoop: fetched entries=${entries.length}`, 'consumerLoop');

				if (!isArrFilled(entries)) {
					await waitAbortable(10, opts.abort);
					continue;
				}
				const { ids, contended } = await this.processEntriesWithIdempotentLocksAndAck(entries, stream, opts);

				this.consoleLog(`consumerLoop:_processed ids=${ids.length}, contended=${contended}`, 'consumerLoop');

				if (isArrFilled(ids)) {
					const approved = await this.ackAndMaybeDeleteBatch(stream, group, ids, opts);

					if (opts.scriptExecutionTimeLimitMs && approved < ids.length) {
						this.consoleLog(`XACK=${approved} < sent=${ids.length} (stream=${stream})`);
					}
					this.consoleLog(`consumerLoop: acked=${approved}`, 'consumerLoop');
				}
				if (!isArrFilled(ids) && contended > (entries.length >> 1)) {
					this.consoleLog(`consumerLoop: backoff contended=${contended}/${entries.length}`, 'consumerLoop');

					await waitAbortable(randBase() + randExtra(contended), opts.abort);
				}
			}
			catch (err: any) {
				this.consoleError(err, 'consumerLoop');
				await wait(Math.max(0, 600));
			}
		}
		this.consoleLog('consumerLoop: stop requested', 'consumerLoop');
	}

	async processEntriesWithIdempotentLocksAndAck(entries: Array<[ string, any[] ]>, stream: string, opts: LoopOpts): Promise<{ ids: string[]; contended: number; }> {
		const ackIds: string[] = [];
		let contended = 0;

		for (const [ id, kv ] of entries) {
			if (!isArrFilled(kv) || ((kv.length & 1) === 1)) {
				this.consoleLog(`skip entry: id=${id} invalid kv`, 'process');

				continue;
			}
			const data: Fields = {};
					
			for (let i = 0; i < kv.length; i += 2) {
				data[kv[i]] = kv[i + 1];
			}
			const rawKey = (data['idemKey'] as string) || (data['idempotencyKey'] as string) || '';

			if (!rawKey) {
				try {
					this.consoleLog(`execute (no-idem): id=${id}`, 'process');
					await this.onExecute(id, data);

					ackIds.push(id);
				}
				catch (err) {
					this.consoleError(err, 'execute');
				}
				continue;
			}
			const keys = this.generateEntryKeysAndToken(stream, opts.consumer, rawKey);
			const acq = await this.tryAcquireEntryLock(keys, opts);

			this.consoleLog(`tryAcquire: id=${id}, acq=${acq}`, 'locks');

			if (acq === 1) {
				ackIds.push(id);

				continue;
			}
			if (acq === 0) {
				contended++;

				let ttl = -2;
						
				try {
					ttl = await (this.redis as any).pttl(keys.startKey);
				}
				catch (err) {
					this.consoleError(err, 'locks');
				}
				await waitAbortable((ttl > 0)
					? (25 + Math.floor(Math.random() * 50))
					: (5 + Math.floor(Math.random() * 15)), opts.abort);
				continue;
			}
			if (!(await this.markEntryStarted(keys, opts))) {
				contended++;
				
				this.consoleLog(`markStart failed: key=${keys.startKey}`, 'locks');
				continue;
			}
			const heartbeatStop = this.heartbeatOne(keys, opts) || (() => {});

			try {
				this.consoleLog(`execute: id=${id}`, 'execute');

				await this.onExecute(id, data);
				await this.completeEntryAndReleaseLock(keys, opts);

				ackIds.push(id);
				this.consoleLog(`execute done & released: id=${id}`, 'execute');
			}
			catch (err: any) {
				this.consoleError(err, 'execute');
				try {
					await this.releaseEntryLockIfToken(keys);
					this.consoleLog(`released on error: key=${keys.lockKey}`, 'locks');
				}
				catch (err2: any) {
					this.consoleError(err2, 'locks');
				}
			}
			finally {
				heartbeatStop();
				this.consoleLog(`heartbeat stopped: key=${keys.startKey}`, 'hb');
			}
		}
		this.consoleLog(`processEntries: ackIds=${ackIds.length}, contended=${contended}`, 'process');
		return { ids: ackIds, contended };
	}

	async ackAndMaybeDeleteBatch(stream: string, group: string, ids: string[], opts: LoopOpts) {
		if (!isArrFilled(ids)) {
			return 0;
		}
		const maxAckIdsPerBatch = Math.max(500, Math.min(4000, this.maxAckIdsPerBatch));
		let total = 0, i = 0;

		while (i < ids.length) {
			const room = Math.min(maxAckIdsPerBatch, ids.length - i);
			const part = ids.slice(i, i + room);
			const acked = await this.runScript(names.AckAndMaybeDeleteBatch, [ stream ], [ group, opts.deleteOnAck ? '1' : '0', ...part ], ackAndMaybeDeleteBatchScript);

			total += Number(acked || 0);
			i += room;
		}
		return total;
	}

	async getNextMessagesFromStream(stream: string, group: string, opts: LoopOpts): Promise<Array<[ string, any[] ]>> {
		let entries: Array<[ string, any[] ]> = this.normalizeEntries(await this.reservePendingOrNewEntries(stream, group, opts));

		if (!isArrFilled(entries)) {
			const res = await (this.redis as any).xreadgroup(
				'GROUP', group, opts.consumer,
				'BLOCK', Math.max(2, opts.streamReadTimeoutMs | 0),
				'COUNT', opts.tasksCountPerIteration,
				'STREAMS', stream, '>',
			);

			if (!isArrFilled(res?.[0]?.[1])) {
				return [];
			}
			entries = this.normalizeEntries(res?.[0]?.[1] ?? []);

			if (!isArrFilled(entries)) {
				return [];
			}
		}
		return entries;
	}

	async reservePendingOrNewEntries(stream: string, group: string, opts: LoopOpts): Promise<any[]> {
		const res = await this.runScript(names.ReservePendingOrNewEntries, [ stream ], [ group, opts.consumer, String(opts.pendingKeyTimeoutMs), String(opts.tasksCountPerIteration), String(opts.scriptExecutionTimeLimitMs) ], reservePendingOrNewEntriesScript);

		return (isArr(res) ? res : []) as any[];
	}

	async tryAcquireEntryLock(keys: EntryKeys, opts: LoopOpts): Promise<0 | 1 | 2> {
		const res = await this.runScript(names.TryAcquireEntryLock, [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(opts.processingLockTimeoutMs), keys.token ], tryAcquireEntryLockScript);

		return Number(res || 0) as 0 | 1 | 2;
	}

	async markEntryStarted(keys: EntryKeys, opts: LoopOpts): Promise<boolean> {
		const res = await this.runScript(names.MarkEntryStarted, [ keys.lockKey, keys.startKey ], [ keys.token, String(opts.processingLockTimeoutMs) ], markEntryStartedScript);

		return Number(res || 0) === 1;
	}

	async completeEntryAndReleaseLock(keys: EntryKeys, opts: LoopOpts): Promise<void> {
		await this.runScript(names.CompleteEntryAndReleaseLock, [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(opts.idempotencyKeyTimeoutSec), keys.token ], completeEntryAndReleaseLockScript);
	}

	async releaseEntryLockIfToken(keys: EntryKeys): Promise<void> {
		await this.runScript(names.ReleaseEntryLockIfToken, [ keys.lockKey, keys.startKey ], [ keys.token ], releaseEntryLockIfTokenScript);
	}

	async heartbeat(keys: EntryKeys, opts: LoopOpts): Promise<number> {
		const res = await this.runScript(names.Heartbeat, [ keys.lockKey, keys.startKey ], [ keys.token, String(opts.processingLockTimeoutMs) ], heartbeatScript);

		return Number(res || 0);
	}

	heartbeatOne(keys: EntryKeys, opts: LoopOpts) {
		if (opts.heartbeatIntervalMs <= 0) {
			return;
		}
		let timer: any, 
			alive = true, 
			hbFails = 0;
		const stop = () => { 
			alive = false; 

			if (timer) {
				clearTimeout(timer);
			} 
		};
		const onAbort = () => stop();

		opts.abort?.addEventListener?.('abort', onAbort, { once: true });

		const tick = async () => {
			if (!alive) {
				return;
			}
			try {
				const r = await this.heartbeat(keys, opts);

				hbFails = r ? 0 : hbFails + 1;

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
			timer = setTimeout(tick, opts.heartbeatIntervalMs).unref?.();
		};

		timer = setTimeout(tick, opts.heartbeatIntervalMs).unref?.();

		return () => {
			opts.abort?.removeEventListener?.('abort', onAbort as any);
			stop();
		};
	}

	resolveConsumerLoopOptions(opts?: LoopOpts): LoopOpts {
		const hostname = this.consumerHostId;
		const pid = process.pid;

		if (!isStrFilled(hostname) || !isNumP(pid)) {
			throw new Error('Hostname or pid is undefined.');
		}
		const consumer = `${hostname}:${pid}`;
		const tasksCountPerIteration = opts.tasksCountPerIteration ?? this.tasksCountPerIteration;
		const streamReadTimeoutMs = opts.streamReadTimeoutMs ?? this.streamReadTimeoutMs;
		const pendingKeyTimeoutMs = opts.pendingKeyTimeoutMs ?? this.pendingKeyTimeoutMs;
		const idempotencyKeyTimeoutSec = opts.idempotencyKeyTimeoutSec ?? this.idempotencyKeyTimeoutSec;
		const processingLockTimeoutMs = opts.processingLockTimeoutMs ?? this.processingLockTimeoutMs;
		const scriptExecutionTimeLimitMs = opts.scriptExecutionTimeLimitMs ?? this.scriptExecutionTimeLimitMs;
		const deleteOnAck = opts.deleteOnAck ?? this.deleteOnAck;
		const safeTtlMs = Math.max(5000, processingLockTimeoutMs | 0);
		const heartbeatIntervalMs = Math.max(1000, Math.floor(safeTtlMs / 4));

		return {
			consumer,
			abort: opts.abort ?? this.abort.signal,
			tasksCountPerIteration: Math.max(1, Math.min(tasksCountPerIteration | 0, 1000)),
			pendingKeyTimeoutMs: Math.max(1000, pendingKeyTimeoutMs | 0),
			processingLockTimeoutMs: safeTtlMs,
			heartbeatIntervalMs,
			streamReadTimeoutMs,
			scriptExecutionTimeLimitMs,
			idempotencyKeyTimeoutSec,
			deleteOnAck,
		};
	}

	generateEntryKeysAndToken(stream: string, consumer: string, rawKey: string): EntryKeys {
		if (!isStrFilled(stream) || !isStrFilled(consumer) || !isStrFilled(rawKey)) {
			throw new Error('stream/consumer/rawKey must be non-empty strings');
		}
		const safe = (s: string) => s.replace(/[^\w:\-]/g, '_');
		const prefix = `q:${safe(stream)}:`;
		const doneKey  = `${prefix}done:${safe(rawKey)}`;
		const lockKey  = `${prefix}lock:${safe(rawKey)}`;
		const startKey = `${prefix}start:${safe(rawKey)}`;
		const token = `${consumer}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2)}`;

		return {
			prefix,
			doneKey,
			lockKey,
			startKey,
			token,
		};
	}

	async loadScript(code: string): Promise<string> {
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

	async runScript(name: string, keys: string[], args: (string|number)[], defaultCode?: string) {
		if (!this.scripts[name]) {
			if (!isStrFilled(defaultCode)) {
				throw new Error(`Undefined script "${name}". Save it before executing.`);
			}
			this.saveScript(name, defaultCode);
		}
		if (!this.scripts[name].sha) {
			this.scripts[name].sha = await this.loadScript(this.scripts[name].source);
		}
		try {
			return await (this.redis as any).evalsha(this.scripts[name].sha!, keys.length, ...keys, ...args);
		}
		catch (err: any) {
			if (String(err?.message || '').includes('NOSCRIPT')) {
				this.scripts[name].sha = await this.loadScript(this.scripts[name].source);

				return await (this.redis as any).evalsha(this.scripts[name].sha!, keys.length, ...keys, ...args);
			}
			throw err;
		}
	}

	saveScript(name: string, source: string): string {
		if (!isStrFilled(source)) {
			throw new Error('Script body is empty.');
		}
		this.scripts[name] = { source };

		return source;
	}

	normalizeEntries(raw: any): Array<[ string, any[] ]> {
		if (!isArr(raw)) {
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
			.filter(([ id, kv ]) => isStrFilled(id) && isArrFilled(kv) && ((kv.length & 1) === 0));
	}

	defineXaddBulkCommandInRedis() {
		if (!this.xaddDefined) {
			this.consoleLog(`defineCommand: xaddBulk`, 'add');
			this.redis.defineCommand('xaddBulk', { numberOfKeys: 1, lua: xaddBulkScript });
			this.xaddDefined = true;
		}
	}
	
	async addTasks(queueName: string, data: Array<Task>, opts: AddTasksOpts = {}): Promise<string[]> {
		if (!isArrFilled(data)) {
			throw new Error('Tasks is not filled.');
		}
		if (!isStrFilled(queueName)) {
			throw new Error('Queue name is required.');
		}
		this.defineXaddBulkCommandInRedis();

		const batches = this.buildBatches(data, opts);
		const result: string[] = new Array(data.length);
		const jobs: Array<() => Promise<void>> = [];
		let cursor = 0;
		
		for (const batch of batches) {
			const start = cursor;
			const end = start + batch.length;
			
			cursor = end;

			jobs.push(async () => {
				const partIds = await this.addTasksChunk(queueName, batch, opts);
				
				for (let k = 0; k < partIds.length; k++) {
					result[start + k] = partIds[k];
				}
			});
		}
		const parallel = Math.max(1, Math.floor(opts.parallel ?? this.defaultParallelJobs));
		const runners = Array.from({ length: Math.min(parallel, jobs.length) }, async () => {
			while (jobs.length) {
				const job = jobs.shift();
					
				if (job) {
					await job();
				}
			}
		});

		await Promise.all(runners);

		this.consoleLog(`addTasks: done total=${result.length}`, 'add');

		return result;
	}

	async addTasksChunk(queueName: string, chunk: Array<Task>, opts: AddTasksOpts): Promise<string[]> {
		return await this.redis.xaddBulk(queueName, ...this.buildArgvForAddTasks(chunk, opts));
	}

	buildArgvForAddTasks(data: Array<Task>, opts: AddTasksOpts): string[] {
		const maxlen = Math.max(0, Math.floor(opts.maxlen ?? 0));
		const approx = opts.exact ? 0 : (opts.approx !== false ? 1 : 0);
		const exact = opts.exact ? 1 : 0;
		const nomkstream = opts.nomkstream ? 1 : 0;
		const trimLimit = Math.max(0, Math.floor(opts.trimLimit ?? 0));
		const minidWindowMs = Math.max(0, Math.floor(opts.minidWindowMs ?? 0));
		const minidExact = opts.minidExact ? 1 : 0;
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
			let flat: Array<string | number | boolean | null | undefined>;

			if ('flat' in entry && isArrFilled(entry.flat)) {
				flat = entry.flat;
				
				if (flat.length % 2 !== 0) {
					throw new Error('Property "flat" must contain an even number of tokens (field/value pairs).');
				}
			}
			else if ('fields' in entry && isObjFilled(entry.fields)) {
				flat = [];
				
				for (const [ k, v ] of Object.entries(entry.fields)) {
					flat.push(k, v as any);
				}
			}
			else {
				throw new Error('Task must have "fields" or "flat".');
			}
			const pairs = flat.length / 2;

			if (pairs <= 0) {
				throw new Error('Each task must contain at least one field/value pair.');
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

	buildBatches(data: Task[], opts: AddTasksOpts) {
		const maxTasksPerChunk = Math.max(1, Math.floor((opts.chunkSize ?? this.defaultChunkSize)));
		const maxTokens = Math.max(2000, this.maxArgsPerRedisCommand);
		const batches: Task[][] = [];
		let cur: Task[] = [],
			tokens = 0;

		const estPairs = (t: Task) => {
			const flat = 'flat' in t && isArrFilled(t.flat)
				? t.flat.length
				: Object.keys((t as any).fields ?? {}).length * 2;
			
			return 2 + flat;
		};

		for (const t of data) {
			const need = estPairs(t);
			
			if (cur.length && (cur.length >= maxTasksPerChunk || tokens + need > maxTokens)) {
				batches.push(cur); 
				cur = []; 
				tokens = 0;
			}
			cur.push(t); 
			tokens += need;
		}
		if (cur.length) {
			batches.push(cur);
		}
		return batches;
	}

	private consoleError(err: unknown, tag = 'consumerLoop') {
		const now = Date.now();

		if (now - this.lastErrLog > this.consoleErrorTimeoutMs) {
			this.lastErrLog = now;
			this.logger.error(`[${tag}]`, err);
		}
	}

	private consoleLog(err: unknown, tag = 'consumerLoop') {
		const now = Date.now();

		if (now - this.lastLog > this.consoleLogTimeoutMs) {
			this.lastLog = now;
			this.logger.log(`[${tag}]`, err);
		}
	}
}
