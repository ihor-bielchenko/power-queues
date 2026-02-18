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
	public abort = new AbortController();
	public redis!: IORedisLike;
	public readonly scripts: Record<string, SavedScript> = {};
	public readonly host: string = 'host';
	public readonly group: string = 'gr1';
	public readonly selectStuckCount: number = 200;
	public readonly selectStuckTimeout: number = 60000;
	public readonly selectStuckMaxTimeout: number = 80;
	public readonly selectCount: number = 200;
	public readonly selectTimeout: number = 3000;
	public readonly buildBatchCount: number = 800;
	public readonly buildBatchMaxCount: number = 10000;
	public readonly retryCount: number = 1;
	public readonly executeSync: boolean = false;
	public readonly idemLockTimeout: number = 180000;
	public readonly idemDoneTimeout: number = 60000;
	public readonly logStatus: boolean = false;
	public readonly logStatusTimeout: number = 1800000;
	public readonly approveCount: number = 2000;
	public readonly removeOnExecuted: boolean = true;

	private signal() {
		return this.abort.signal;
	}

	private consumer(): string {
		return this.host +':'+ process.pid;
	}

	async runQueue(queueName: string, from: '$' | '0-0' = '0-0') {
		setMaxListeners(0, this.abort.signal);

		await this.createGroup(queueName, from);
		await this.consumerLoop(queueName, from);
	}

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

	async select(queueName: string, from: '$' | '0-0' = '0-0'): Promise<any[]> {
		let selected = await this.selectS(queueName, from);

		if (!isArrFilled(selected)) {
			selected = await this.selectF(queueName, from);
		}
		return this.selectP(selected);
	}

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

	private values(value: any[]) {
		const result: any = {};

		for (let i = 0; i < value.length; i += 2) {
			result[value[i]] = value[i + 1];
		}
		return result;
	}

	private payload(data: any): any {
		try {
			return JSON.parse(data);
		}
		catch (err) {
		}
		return data;
	}

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

	private async idempotencyAllow(keys: IdempotencyKeys): Promise<0 | 1 | 2> {
		const res = await this.runScript('IdempotencyAllow', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.idemLockTimeout), keys.token ], IdempotencyAllow);

		return Number(res || 0) as 0 | 1 | 2;
	}

	private async idempotencyStart(keys: IdempotencyKeys): Promise<boolean> {
		const res = await this.runScript('IdempotencyStart', [ keys.lockKey, keys.startKey ], [ keys.token, String(this.idemLockTimeout) ], IdempotencyStart);

		return Number(res || 0) === 1;
	}

	private async idempotencyDone(keys: IdempotencyKeys): Promise<void> {
		await this.runScript('IdempotencyDone', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.idemDoneTimeout), keys.token ], IdempotencyDone);
	}

	private async idempotencyFree(keys: IdempotencyKeys): Promise<void> {
		await this.runScript('IdempotencyFree', [ keys.lockKey, keys.startKey ], [ keys.token ], IdempotencyFree);
	}

	private async success(queueName: string, task: Task) {
		if (this.logStatus) {
			const statusKey = `${queueName}:${task.job}:`;

			await this.incr(statusKey +'ok', this.logStatusTimeout);
			await this.incr(statusKey +'ready', this.logStatusTimeout);
		}
		return await this.onSuccess(queueName, task);
	}

	private async error(err: any, queueName: string, task: Task): Promise<Task> {
		const taskP: any = { ...task };

		if (!(taskP.attempt >= (this.retryCount - 1))) {
			await this.onRetry(err, queueName, taskP);
			await this.addTasks(queueName, [{ ...taskP.payload, idemKey: taskP.idemKey, }], {
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
			await this.addTasks(dlqKey, [{ ...taskP.payload, idemKey: taskP.idemKey, }], {
				createdAt: taskP.createdAt,
				job: taskP.job,
				attempt: taskP.attempt,
			});
		}
		return await this.onError(err, queueName, { ...taskP, attempt: (taskP.attempt || 0) + 1 });
	}

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

	private keysLength(task: Task): number {
		return 2 + Object.keys(task as any).length * 2;
	}

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

	async beforeExecute(queueName: string, tasks: Array<[ string, any, number, string, string, number ]>) {
		return tasks;
	}

	async onExecute(queueName: string, task: Task): Promise<Task> {
		return task;
	}

	async onBatchReady(queueName: string, tasks: Task[]) {
	}

	async onSuccess(queueName: string, task: Task): Promise<Task> {
		return task;
	}

	async onError(err: any, queueName: string, task: Task): Promise<Task> {
		return task;
	}

	async onBatchError(err: any, queueName: string, tasks: Array<[ string, any, number, string, string, number ]>) {
	}

	async onRetry(err: any, queueName: string, task: Task) {
	}
}
