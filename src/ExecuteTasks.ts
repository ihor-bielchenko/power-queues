import type { 
	Constructor,
	IdempotencyKeys,
	Task,
} from './types';
import {
	Approve,
	IdempotencyAllow,
	IdempotencyStart,
	IdempotencyDone,
	IdempotencyFree,
} from './scripts';
import { mix } from './mix';
import { Script } from './Script';
import { PowerRedis } from 'power-redis';

export function ExecuteTasks<TBase extends Constructor>(Base: TBase) {
	return class extends mix(Base, Script) {
		public abort = new AbortController();
		public redis!: PowerRedis;
		public readonly workerExecuteLockTimeoutMs: number = 180000;
		public readonly workerCacheTaskTimeoutMs: number = 60;
		public readonly approveBatchTasksCount: number = 2000;
		public readonly removeOnExecuted: boolean = false;
		public readonly executeBatchAtOnce: boolean = false;
		public readonly executeJobStatus: boolean = false;
		public readonly executeJobStatusTtlSec: number = 300;
		public readonly consumerHost: string = 'host';
		public readonly stream: string = 'stream';
		public readonly group: string = 'group';

		async onExecute(id: string, payload: any, createdAt: number, job: string, key: string) {
		}

		async onExecuted(data: Array<[ string, any[], number, string, string ]>) {
		}

		async onSuccess(id: string, payload: any, createdAt: number, job: string, key: string) {
		}

		async success(id: string, payload: any, createdAt: number, job: string, key: string) {
			if (this.executeJobStatus) {
				await this.status(id, payload, createdAt, job, key);
			}
			await this.onSuccess(id, payload, createdAt, job, key);
		}

		async status(id: string, payload: any, createdAt: number, job: string, key: string) {
			const prefix = `s:${this.stream}:`;
			const { ready = 0, ok = 0 } = await this.getMany(prefix);
			
			await this.redis.setMany([{ key: `${prefix}ready`, value: ready + 1 }, { key: `${prefix}ok`, value: ok + 1 }], this.executeJobStatusTtlSec);
		}
		
		async execute(tasks: Array<[ string, any[], number, string, string ]>): Promise<string[]> {
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

				if ((!Array.isArray(result) || !(result.length > 0)) && contended > (tasks.length >> 1)) {
					await this.waitAbortable((15 + Math.floor(Math.random() * 35)) + Math.min(250, 15 * contended + Math.floor(Math.random() * 40)));
				}
			}
			catch (err) {
			}
			return result;
		}

		async executeProcess(id: string, payload: any, createdAt: number, job: string, key: string): Promise<any> {
			if (key) {
				return await this.idempotency(id, payload, createdAt, job, key);
			}
			else {
				try {
					await this.onExecute(id, payload, createdAt, job, key);
					await this.success(id, payload, createdAt, job, key);
					return { id };
				}
				catch (err) {
				}
			}
			return {};
		}

		async approve(ids: string[]) {
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

		async idempotency(id: string, payload: any, createdAt: number, job: string, key: string) {
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
				await this.onExecute(id, payload, createdAt, job, key);
				await this.idempotencyDone(keys);
				await this.success(id, payload, createdAt, job, key);

				return { id };
			}
			catch (err: any) {
				try {
					await this.idempotencyFree(keys);
				}
				catch (err2: any) {
				}
			}
			finally {
				heartbeat();
			}
		}

		idempotencyKeys(key: string): IdempotencyKeys {
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

		async idempotencyAllow(keys: IdempotencyKeys): Promise<0 | 1 | 2> {
			const res = await this.runScript('IdempotencyAllow', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.workerExecuteLockTimeoutMs), keys.token ], IdempotencyAllow);

			return Number(res || 0) as 0 | 1 | 2;
		}

		async idempotencyStart(keys: IdempotencyKeys): Promise<boolean> {
			const res = await this.runScript('IdempotencyStart', [ keys.lockKey, keys.startKey ], [ keys.token, String(this.workerExecuteLockTimeoutMs) ], IdempotencyStart);

			return Number(res || 0) === 1;
		}

		async idempotencyDone(keys: IdempotencyKeys): Promise<void> {
			await this.runScript('IdempotencyDone', [ keys.doneKey, keys.lockKey, keys.startKey ], [ String(this.workerCacheTaskTimeoutMs), keys.token ], IdempotencyDone);
		}

		async idempotencyFree(keys: IdempotencyKeys): Promise<void> {
			await this.runScript('IdempotencyFree', [ keys.lockKey, keys.startKey ], [ keys.token ], IdempotencyFree);
		}

		async waitAbortable(ttl: number) {
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

		heartbeat(keys: IdempotencyKeys) {
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

		values(value: any[]): Task {
			const result: any = {};

			for (let i = 0; i < value.length; i += 2) {
				result[value[i]] = value[i + 1];
			}
			return result;
		}

		payload(data): any {
			try {
				return JSON.parse(data.payload);
			}
			catch (err) {
			}
			return data;
		}

		consumer(): string {
			return `${String(this.consumerHost || 'host')}:${process.pid}`;
		}

		signal() {
			return this.abort.signal;
		}
	}
}
