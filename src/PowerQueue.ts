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

	private nowSec(): number {
		return Math.floor(Date.now() / 1000);
	}

	private readyKey(queueName: string): string {
		return this.toKeyString(queueName);
	}

	private processingKey(queueName: string): string {
		return this.toKeyString(queueName, 'processing');
	}

	private processingVtKey(queueName: string): string {
		return this.toKeyString(queueName, 'processing', 'vt');
	}

	private delayedKey(queueName: string): string {
		return this.toKeyString(queueName, 'delayed');
	}

	private totalKey(queueName: string): string {
		return this.toKeyString(queueName, 'total');
	}

	toKeyString(...parts: Array<string | number>): string {
		return super.toKeyString('queue', ...parts);
	}

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

	private stopHeartbeat(task: Task) {
		const t = this.heartbeatTimers.get(task.id);

		if (t) {
			clearInterval(t);
		}
		this.heartbeatTimers.delete(task.id);
	}

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

	async enqueue(total: string, ready: string, delayed: string, payload: any, delaySec?: number): Promise<number> {
		if (!this.checkConnection()) {
			throw new Error('Redis connection error.');
		}
		const raw = this.toPayload(payload);

		await (this.redis as any)?.incr(total);
		await (this.redis as any)?.expire(total, this.expireStatusSec);

		if (isNumP(delaySec) && delaySec > 0) {
			const score = this.nowSec() + delaySec;

			return await (this.redis as any)?.zadd(delayed, score, raw);
		}
		const done = await (this.redis as any)?.rpush(ready, raw);

		if (isNumP(done)) {
			await (this.redis as any)?.incr(total);
			await (this.redis as any)?.expire(total, this.expireStatusSec);
		}
		return done;
	}

	async extendVisibility(processingVt: string, raw: string, visibilitySec: number): Promise<void> {
		const deadline = this.nowSec() + Math.max(1, visibilitySec);

		await this.zaddCompatXXCH(processingVt, deadline, raw);
	}

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

	stop(queueName: string) {
		const r = this.runners.get(queueName);

		if (r) {
			r.running = false;
			
			this.runners.delete(queueName);
		}
	}

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

	async addTask(data: Partial<Task>, delaySec?: number): Promise<number> {
		const ready = this.readyKey(String(data.queueName));
		const delayed = this.delayedKey(String(data.queueName));
		const total = this.totalKey(String(data.queueName));

		return await this.enqueue(total, ready, delayed, this.buildTask(data), isNumP(delaySec) ? delaySec : 0);
	}

	async addTasks(data: {
		queueName: string;
		payloads: Array<any | Partial<Task>>;
		delaySec?: number | number[];
	}): Promise<number> {
		if (!this.checkConnection()) {
			throw new Error('Redis connection error.');
		}
		if (!isObjFilled(data) || !isStrFilled(data.queueName)) {
			throw new Error('Queue name is not valid.');
		}
		if (!isArrFilled(data.payloads)) {
			return 0;
		}
		const queueName = String(data.queueName);
		const ready = this.readyKey(queueName);
		const delayed = this.delayedKey(queueName);
		const total = this.totalKey(queueName);
		const now = this.nowSec();
		const uniformDelay = isNumP(data.delaySec) ? Math.max(0, Number(data.delaySec)) : undefined;
		const perItemDelays = isArr(data.delaySec) ? (data.delaySec as number[]).map((v) => Math.max(0, Number(v || 0))) : undefined;
		const batchSize = Math.max(1, Math.min(this.portionLength, 1000));
		let idx = 0,
			done = 0;

		while (idx < data.payloads.length) {
			const end = Math.min(idx + batchSize, data.payloads.length);
			const tx = (this.redis as any)?.multi();

			for (let i = idx; i < end; i++) {
				const item = data.payloads[i];
				let partial: Partial<Task>;
				
				if (isObjFilled(item) && Object.prototype.hasOwnProperty.call(item as any, 'payload')) {
					partial = { ...(item as Partial<Task>), queueName };
				} 
				else {
					partial = { queueName, payload: item };
				}
				const task = this.buildTask(partial);
				const raw = this.toPayload(task);
				let d = 0;
				
				if (isNumP(uniformDelay)) {
					d = uniformDelay;
				}
				else if (isArr(perItemDelays)) {
					d = Number(perItemDelays[i] || 0);
				}
				if (d > 0) {
					tx.zadd(delayed, now + d, raw);
				} 
				else {
					tx.rpush(ready, raw);
				}
				done++;
			}
			tx.set(total, done);
			tx.expire(total, this.expireStatusSec);
			idx = end;
			
			await tx.exec();
		}
		return done;
	}

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

	async beforeIterationExecution(data: Array<Task>): Promise<Array<Task>> {
		return data;
	}

	async afterIterationExecution(data: Array<Task>, results: Array<TaskResult>): Promise<void> {
	}

	async beforeExecution(task: Task): Promise<Task> {
		return task;
	}

	async afterExecution(task: Task, result: TaskResult): Promise<TaskResult> {
		return result;
	}

	async execute(task: Task): Promise<TaskResult> {
		return {};
	}

	async onRetry(task: Task): Promise<void> {
	}

	async onError(err: Error, task: Task): Promise<void> {
	}

	async onFail(err: Error, task: Task): Promise<void> {
	}

	async onFatal(err: Error, task: Task): Promise<void> {
	}

	async onSuccess(task: Task, result: TaskResult): Promise<void> {
	}

	async onChainSuccess(task: Task, result: TaskResult): Promise<void> {
	}

	async onIterationError(err: Error, queueName: string): Promise<void> {
	}

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

	private jitteredBackoffSec(attempt: number): number {
		const base = Math.max(1, Number(this.retryBaseSec) || 1);
		const maxD = Math.max(base, Number(this.retryMaxSec) || 3600);
		const pow = Math.min(maxD, base * Math.pow(2, Math.max(0, attempt - 1)));
		const jitter = Math.floor(Math.random() * base);
		
		return Math.min(maxD, pow + jitter);
	}

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