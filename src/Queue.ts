import * as crypto from 'crypto';
import { v4 as uuid } from 'uuid';
import {
	isArrFilled,
	isArr,
	isObjFilled,
	isObj,
	isNumP,
	fromJSON,
	toJSON,
} from 'full-utils';
import { 
	LockOptsInterface,
	DistLockInterface,
	TasksInterface,
	TaskInterface, 
} from './types';

const UNLOCK_LUA = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`;

const EXTEND_LUA = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`;

export class Queue {
	public readonly redisService;
	public readonly attempts: number = 2;
	public readonly portion: number = 1;
	public readonly timeout: number;
	public running = false;

	wait(ms: number) { 
		return new Promise((r) => setTimeout(r, ms)); 
	}

	key(...parts: Array<string | number>): string {
		return parts.join(':');
	}

	connection(db: string) {
		return this.redisService['clients'].get(db);
	}

	checkConnection(db: string): boolean {
		return !!this.connection(db) 
			&& ((this.connection(db) as any).status === 'ready'
				|| (this.connection(db) as any).status === 'connecting');
	}

	start(db: string, queue: string, attempt: number = 0) {
		if (this.running) {
			return;
		}
		this.running = true;
		this.loop(db, queue, attempt).catch((err) => {
			this.running = false;
		});
	}

	async drop(db: string, pattern: string): Promise<number> {
		if (!this.checkConnection(db)) {
			throw new Error(`Redis connection error "${db}".`);
		}
		try {
			let cursor = '0',
				total = 0;

			do {
				const [ next, keys ] = await this.connection(db).scan(cursor, 'MATCH', pattern, 'COUNT', 500);

				cursor = next;

				if (isArrFilled(keys)) {
					total += keys.length;

					for (let i = 0; i < keys.length; i += 500) {
						const chunk = keys.slice(i, i + 500);

						if (typeof (this.connection(db) as any).unlink === 'function') {
							await (this.connection(db) as any).unlink(...chunk);
						} 
						else {
							await this.connection(db).del(...chunk);
						}
					}
				}
			} 
			while (cursor !== '0');
			return total;
		} 
		catch (err) {
		}
		throw new Error(`Redis clear error "${db}".`);
	}

	async unlock(db: string, key: string, token: string): Promise<boolean> {
		return Number(await this.connection(db).eval(UNLOCK_LUA, 1, key, token)) === 1;
	}

	async lock(db: string, key: string, opts?: LockOptsInterface): Promise<DistLockInterface | null> {
		if (!this.checkConnection(db)) {
			throw new Error(`Redis connection error "${db}".`);
		}
		const token = crypto.randomBytes(16).toString('hex');
		const retries = Math.max(0, opts?.retries ?? 5);
		const minDelay = Math.max(5, opts?.minDelayMs ?? 20);
		const maxDelay = Math.max(minDelay, opts?.maxDelayMs ?? 60);
		const ttlMs = Number(opts?.ttlMs ?? 3000);
		const lockKey = this.key('lock', key);
		let attempt = 0;

		while (attempt < retries) {
			const ok = await this.connection(db).set(lockKey, token, 'PX', ttlMs, 'NX');
			
			if (ok === 'OK') {
				return { 
					token,
					key: lockKey,  
					ttlMs: opts.ttlMs, 
					unlock: async () => await this.unlock(db, lockKey, token),
				};
			}
			attempt++;

			if (attempt < retries) {
				await this.wait(minDelay + Math.floor(Math.random() * (maxDelay - minDelay + 1)));
			}
		}
		return null;
	}

	async write(db: string, key: string, value: any, ttlMs?: number) {
		if (!this.checkConnection(db)) {
			throw new Error(`Redis connection error "${db}".`);
		}
		const payload = (isArr(value) || isObj(value))
			? toJSON(value)
			: String(value);

		return isNumP(ttlMs)
			? await this.connection(db).set(key, payload, 'EX', ttlMs)
			: await this.connection(db).set(key, payload);
	}

	async addOne(db: string, key: string, task: TaskInterface): Promise<void> {
		if (!this.checkConnection(db)) {
			throw new Error(`Redis connection error "${db}".`);
		}
		const opts = task.opts || {};
		const id = task.id || uuid();

		await this.connection(db).rpush(key, toJSON({
			...task,
			opts: {
				...opts,
				attempt: opts.attempt || 0,
			},
			id,
			enqueuedAt: Date.now(),
		}));
	}

	async addMany(db: string, key: string, data: TasksInterface): Promise<void> {
		if (!this.checkConnection(db)) {
			throw new Error(`Redis connection error "${db}".`);
		}
		const opts = data.opts || {};
		const id = data.id || uuid();

		if (opts.progress) {
			await this.write(db, this.key(id, 'ready'), 0, 300000);
			await this.write(db, this.key(id, 'total'), data.payloads.length, 300000);
		}
		await this.connection(db).rpush(key, ...data
			.payloads
			.map((payload) => {
				return toJSON({ 
					payload,
					opts: {
						...opts,
						attempt: opts.attempt || 0,
					},
					id,
					enqueuedAt: Date.now(),
				});
			}));
	}

	protected async onStart(queue: string, portion: Array<TaskInterface>): Promise<void> {
	}

	protected async onSuccess(queue: string, task: TaskInterface, result: any): Promise<void> {
	}

	protected async onError(queue: string, task: TaskInterface, err: any): Promise<void> {
	}

	protected async onFatal(queue: string, task: TaskInterface, err: any): Promise<void> {
	}

	protected async onEnd(queue: string, result: any): Promise<void> {
	}

	protected async beforeExecute(queue: string, task: TaskInterface): Promise<any> {
		return task;
	}

	protected async afterExecute(queue: string, portion: Array<TaskInterface>, result: any): Promise<any> {
		return result;
	}

	protected async callback(queue: string, task: TaskInterface): Promise<any> {
		return true;
	}

	protected async execute(queue: string, task: TaskInterface): Promise<any> {
		return await this.callback(queue, task);
	}

	protected async retry(db: string, queue: string, task: TaskInterface): Promise<void> {
		try {
			const attempt = (task.opts?.attempt || 0) + 1;
			const key = this.key('queue', queue, attempt);

			await this.connection(db).rpush(key, toJSON({
				...task,
				opts: {
					...task.opts || {},
					attempt,
				},
			}));
		}
		catch (err) {
		}
	}

	private async select(db: string, queue: string, attempt: number): Promise<TaskInterface[] | null> {
		const arr = await this.connection(db).rpop(this.key('queue', queue, attempt), this.portion);

		if (!isArrFilled(arr)) {
			return null;
		}
		return arr
			.map((item) => {
				try {
					const v: TaskInterface = fromJSON(item);
				
					if (!isObjFilled(v)) {
						return null;
					}			
					return v;
				}
				catch (err) {
				}
				return null;
			})
			.filter((item) => !!item);
	}

	private async process(db: string, queue: string, task: TaskInterface): Promise<void> {
		let res: any;

		try {
			await this.onSuccess(queue, task, res = await this.execute(queue, await this.beforeExecute(queue, task)));
		}
		catch (err) {
			await this.onError(queue, task, err);
			
			if (task.opts?.attempt < (this.attempts - 1)) {
				await this.retry(db, queue, task);
			}
			else {
				await this.onFatal(queue, task, err);
			}
		}
		await this.onEnd(queue, res);
		return res;
	}

	private async loop(db: string, queue: string, attempt: number) {
		while (this.running) {
			if (!this.checkConnection(db)) {
				await this.wait(1000);
				continue;
			}
			const tasks = await this.select(db, queue, attempt);

			if (!isArrFilled(tasks)) {
				await this.wait(1000);
				continue;
			}
			await this.onStart(queue, tasks);
			await this.afterExecute(queue, tasks, await Promise.all(tasks.map((task: TaskInterface) => this.process(db, queue, task))));
			
			if (this.timeout > 0) {
				await this.wait(this.timeout);
			}
		}
	}
}
