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
	public abort = new AbortController();
	public redis!: IORedisLike;
	public readonly strictCheckingConnection: boolean = [ 'true', 'on', 'yes', 'y', '1' ].includes(String(process.env.REDIS_STRICT_CHECK_CONNECTION ?? '').trim().toLowerCase());
	public readonly scripts: Record<string, SavedScript> = {};
	public readonly addingBatchTasksCount: number = 800;
	public readonly addingBatchKeysLimit: number = 10000;
	public readonly workerExecuteLockTimeoutMs: number = 180000;
	public readonly workerCacheTaskTimeoutMs: number = 60;
	public readonly approveBatchTasksCount: number = 2000;
	public readonly removeOnExecuted: boolean = false;
	public readonly executeBatchAtOnce: boolean = false;
	public readonly executeJobStatus: boolean = false;
	public readonly executeJobStatusTtlMs: number = 300000;
	public readonly consumerHost: string = 'host';
	public readonly stream: string = 'stream';
	public readonly group: string = 'group';
	public readonly workerBatchTasksCount: number = 200;
	public readonly recoveryStuckTasksTimeoutMs: number = 60000;
	public readonly workerLoopIntervalMs: number = 5000;
	public readonly workerSelectionTimeoutMs: number = 80;
	public readonly workerMaxRetries: number = 5;
	public readonly workerClearAttemptsTimeoutMs: number = 86400000;

	async onSelected(data: Array<[ string, any[], number, string, string ]>) {
		return data;
	}

	async onExecute(id: string, payload: any, createdAt: number, job: string, key: string, attempt: number) {
	}

	async onExecuted(data: Array<[ string, any[], number, string, string ]>) {
	}

	async onSuccess(id: string, payload: any, createdAt: number, job: string, key: string) {
	}

	async onBatchError(err: any, tasks?: Array<[ string, any[], number, string, string ]>) {
	}

	async onError(err: any, id: string, payload: any, createdAt: number, job: string, key: string) {
	}

	async onRetry(err: any, id: string, payload: any, createdAt: number, job: string, key: string, attempts: number) {
	}

	async runQueue() {
		await this.createGroup('0-0');
		await this.consumerLoop();
	}

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

				if (isArrFilled(tasks)) {
					await this.approve(ids);
				}
			}
			catch (err: any) {
				await this.batchError(err);
				await wait(600);
			}
		}
	}

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

	private saveScript(name: string, codeBody: string): string {
		if (!isStrFilled(codeBody)) {
			throw new Error('Script body is empty.');
		}
		this.scripts[name] = { codeBody };

		return codeBody;
	}

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

	private async xaddBatch(queueName: string, ...batches: string[]): Promise<string[]> {
		return await this.runScript('XAddBulk', [ queueName ], batches, XAddBulk);
	}

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
				throw new Error('Task must have "payload" or "flat".');
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

	private keysLength(task: Task): number {
		return 2 + (('flat' in task && Array.isArray(task.flat) && task.flat.length) ? task.flat.length : Object.keys(task).length * 2);
	}

	private attemptsKey(id: string): string {
		const safeStream = this.stream.replace(/[^\w:\-]/g, '_');
		const safeId = id.replace(/[^\w:\-]/g, '_');

		return `q:${safeStream}:attempts:${safeId}`;
	}

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

	private async getAttempts(id: string): Promise<number> {
		const key = this.attemptsKey(id);
		const v = await (this.redis as any).get(key);

		return Number(v || 0);
	}

	private async clearAttempts(id: string): Promise<void> {
		const key = this.attemptsKey(id);

		try {
			await (this.redis as any).del(key);
		}
		catch (e) {
		}
	}

	private async success(id: string, payload: any, createdAt: number, job: string, key: string) {
		if (this.executeJobStatus) {
			const prefix = `${this.stream}:${job}:`;

			await this.incr(`${prefix}ok`, this.executeJobStatusTtlMs);
			await this.incr(`${prefix}ready`, this.executeJobStatusTtlMs);
		}
		await this.onSuccess(id, payload, createdAt, job, key);
	}

	private async batchError(err: any, tasks?: Array<[ string, any[], number, string, string ]>) {
		await this.onBatchError(err, tasks);
	}

	private async error(err: any, id: string, payload: any, createdAt: number, job: string, key: string, attempt: number) {
		if (this.executeJobStatus && attempt >= this.workerMaxRetries) {
			const prefix = `${this.stream}:${job}:`;

			await this.incr(`${prefix}err`, this.executeJobStatusTtlMs);
			await this.incr(`${prefix}ready`, this.executeJobStatusTtlMs);
		}
		await this.onError(err, id, payload, createdAt, job, key);
	}

	private async attempt(err: any, id: string, payload: any, createdAt: number, job: string, key: string, attempt: number) {
		await this.onRetry(err, id, payload, createdAt, job, key, attempt);
	}

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
			await this.onExecuted(tasks);

			if (!isArrFilled(result) && contended > (tasks.length >> 1)) {
				await this.waitAbortable((15 + Math.floor(Math.random() * 35)) + Math.min(250, 15 * contended + Math.floor(Math.random() * 40)));
			}
		}
		catch (err) {
			await this.batchError(err, tasks);
		}
		return result;
	}

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

	private async idempotencyAllow(keys: IdempotencyKeys): Promise<0 | 1 | 2> {
		const res = await this.runScript('IdempotencyAllow', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.workerExecuteLockTimeoutMs), keys.token ], IdempotencyAllow);

		return Number(res || 0) as 0 | 1 | 2;
	}

	private async idempotencyStart(keys: IdempotencyKeys): Promise<boolean> {
		const res = await this.runScript('IdempotencyStart', [ keys.lockKey, keys.startKey ], [ keys.token, String(this.workerExecuteLockTimeoutMs) ], IdempotencyStart);

		return Number(res || 0) === 1;
	}

	private async idempotencyDone(keys: IdempotencyKeys): Promise<void> {
		await this.runScript('IdempotencyDone', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.workerCacheTaskTimeoutMs), keys.token ], IdempotencyDone);
	}

	private async idempotencyFree(keys: IdempotencyKeys): Promise<void> {
		await this.runScript('IdempotencyFree', [ keys.lockKey, keys.startKey ], [ keys.token ], IdempotencyFree);
	}

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

	private async select(): Promise<Array<[ string, any[], number, string, string ]>> {
		let entries: Array<[ string, any[], number, string, string ]> = await this.selectStuck();

		if (!isArrFilled(entries)) {
			entries = await this.selectFresh();
		}
		return this.normalizeEntries(entries);
	}

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

	private async waitAbortable(ttl: number) {
		return new Promise<void>((resolve) => {
			const signal = this.signal();

			if (signal?.aborted) {
				return resolve();
			}
			const t = setTimeout(() => {
				if (signal) {
					signal.removeEventListener('abort', onAbort as any);
				}
				resolve();
			}, (ttl > 0)
				? (25 + Math.floor(Math.random() * 50))
				: (5 + Math.floor(Math.random() * 15)));
			(t as any).unref?.();

			function onAbort() { 
				clearTimeout(t); 
				resolve(); 
			}
			signal?.addEventListener('abort', onAbort, { once: true });
		});
	}

	private heartbeat(keys: IdempotencyKeys) {
		if (this.workerExecuteLockTimeoutMs <= 0) {
			return;
		}
		let timer: any, 
			alive = true, 
			hbFails = 0;
		const workerHeartbeatTimeoutMs = Math.max(1000, Math.floor(Math.max(5000, this.workerExecuteLockTimeoutMs | 0) / 4));
		const stop = () => { 
			alive = false; 

			if (timer) {
				clearTimeout(timer);
			} 
		};
		const onAbort = () => stop();
		const signal = this.signal();

		signal?.addEventListener?.('abort', onAbort, { once: true });

		const tick = async () => {
			if (!alive) {
				return;
			}
			try {
				const r = await this.heartbeat(keys);

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
			timer = setTimeout(tick, workerHeartbeatTimeoutMs).unref?.();
		};

		timer = setTimeout(tick, workerHeartbeatTimeoutMs).unref?.();

		return () => {
			signal?.removeEventListener?.('abort', onAbort as any);
			stop();
		};
	}

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

	private values(value: any[]) {
		const result: any = {};

		for (let i = 0; i < value.length; i += 2) {
			result[value[i]] = value[i + 1];
		}
		return result;
	}

	private payload(data: any): any {
		try {
			return jsonDecode(data);
		}
		catch (err) {
		}
		return data;
	}

	private signal() {
		return this.abort.signal;
	}

	private consumer(): string {
		return `${String(this.consumerHost || 'host')}:${process.pid}`;
	}
}