import type { 
	Constructor,
	AddTasksOptions, 
	Task,
} from './types';
import type { JsonPrimitiveOrUndefined } from 'power-redis';
import { PowerRedis } from 'power-redis';
import { v4 as uuid } from 'uuid';
import { XAddBulk } from './scripts';
import { mix } from './mix';
import { Script } from './Script';


export function AddTasks<TBase extends Constructor>(Base: TBase) {
	return class extends mix(Base, Script) {
		public redis!: PowerRedis;
		public readonly addingBatchTasksCount: number = 800;
		public readonly addingBatchKeysLimit: number = 10000;
		public readonly idemOn: boolean = true;
		public readonly idemKey: string = '';

		async addTasks(queueName: string, data: any[], opts: AddTasksOptions): Promise<string[]> {
			if (!Array.isArray(data) || !(data.length > 0)) {
				throw new Error('Tasks is not filled.');
			}
			if (typeof queueName !== 'string' || !(queueName.length > 0)) {
				throw new Error('Queue name is required.');
			}
			const batches = this.buildBatches(data);
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

			await Promise.all(runners);
			return result;
		}

		async xaddBatch(queueName: string, ...batches: string[]): Promise<string[]> {
			return await this.runScript('XAddBulk', [ queueName ], batches, XAddBulk);
		}

		payloadBatch(data: Array<Task>, opts: AddTasksOptions): string[] {
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

				if ('flat' in entry && Array.isArray(entry.flat) && entry.flat.length > 0) {
					flat = entry.flat;
					
					if (flat.length % 2 !== 0) {
						throw new Error('Property "flat" must contain an even number of realKeysLength (field/value pairs).');
					}
				}
				else if ('payload' in entry && typeof entry.payload === 'object' && Object.keys(entry.payload || {}).length > 0) {
					flat = [];
				
					for (const [ k, v ] of Object.entries(entry.payload)) {
						flat.push(k, v as any);
					}
				}
				else {
					throw new Error('Task must have "payload" or "flat".');
				}
				const pairs = flat.length / 2;

				if (pairs <= 0) {
					throw new Error('Task must have "payload" or "flat".');
				}
				argv.push(String(id));
				argv.push(String(pairs));

				for (const token of flat) {
					argv.push(!token
						? ''
						: (typeof token === 'string' && token.length > 0)
							? token
							: String(token));
				}
			}
			return argv;
		}

		buildBatches(tasks: Task[]): Task[][] {
			const job = uuid();
			const batches: Task[][] = [];
			let batch: Task[] = [],
				realKeysLength = 0;

			for (let task of tasks) {
				let entry: any = task;

				if (this.idemOn) {
					const createdAt = entry?.createdAt || Date.now();
					let idemKey = entry?.idemKey || uuid();

					if (typeof entry.payload === 'object') {
						if (this.idemKey && typeof entry.payload[this.idemKey] === 'string' && entry.payload[this.idemKey].length > 0) {
							idemKey = entry.payload[this.idemKey];	
						}
						entry = { 
							...entry, 
							payload: { 
								payload: JSON.stringify(entry.payload),  
								createdAt,
								job,
								idemKey,
							}, 
						};
					}
					else if (Array.isArray(entry.flat)) {
						entry.flat.push('createdAt');
						entry.flat.push(String(createdAt));
						entry.flat.push('job');
						entry.flat.push(job);
						entry.flat.push('idemKey');
						entry.flat.push(idemKey);
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

		keysLength(task: Task): number {
			return 2 + (('flat' in task && Array.isArray(task.flat) && task.flat.length) ? task.flat.length : Object.keys(task).length * 2);
		}
	}
}
