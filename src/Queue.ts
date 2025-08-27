import { 
	isArrFilled,
	isStrFilled,
	isObjFilled,
	fromJSON,
	toJSON, 
} from 'full-utils';
import { RedisManager } from './RedisManager';
import { Processor } from './Processor';
import { TaskInterface } from './types';

export class Queue {
	public readonly redisManager: RedisManager;
	public readonly db: string;
	public readonly attempts: number = 3;
	public readonly portionSize: number = 1;
	public running = false;
	private processors: Array<Processor> = [];

	protected async execute(queue: string, task: TaskInterface): Promise<any> {
		const processor = this.getProcessor(task.processor);

		if (!processor) {
			throw new Error('Undefined processor.');
		}
		return await processor.execute.call(processor, task);
	}

	protected async beforeExecute(queue: string, task: TaskInterface): Promise<any> {
		return task;
	}

	protected async afterExecute(queue: string, task: TaskInterface, result: any): Promise<any> {
		return result;
	}

	protected async onSuccess(queue: string, task: TaskInterface, result: any): Promise<void> {
	}

	protected async onError(queue: string, task: TaskInterface, err: any): Promise<void> {
	}

	protected async onFatal(queue: string, task: TaskInterface, err: any): Promise<void> {
	}

	protected async onStart(queue: string, task: TaskInterface): Promise<void> {
	}

	protected async onEnd(queue: string, result: any): Promise<void> {
	}

	private async process(queue: string, task: TaskInterface): Promise<void> {
		let res: any;

		try {
			await this.onStart(queue, task);
			await this.onSuccess(queue, task, res = await this.afterExecute(queue, task, await this.execute(queue, await this.beforeExecute(queue, task))));
		}
		catch (err) {
			await this.onError(queue, task, err);
			
			if (task.attempt < this.attempts) {
				await this.retry(queue, task);
			}
			else {
				await this.onFatal(queue, task, err);
			}
		}
		await this.onEnd(queue, res);
	}

	private async loop(queue: string, attempt: number) {
		while (this.running) {
			if (!this.redisManager.checkConnection(this.db)) {
				await this.redisManager.wait(1000);
				continue;
			}
			const tasks = await this.select(this.db, queue, attempt);

			if (!isArrFilled(tasks)) {
				await this.redisManager.wait(1000);
				continue;
			}
			await Promise.all(tasks.map((task: TaskInterface) => this.process(queue, task)));
		}
	}

	protected async retry(queue: string, task: TaskInterface): Promise<void> {
		await this.push(this.db, queue, { ...task, attempt: (task.attempt || 1) + 1 });
	}

	setProcessors(processors: Processor[]): this {
		this.processors = [ ...processors ];
		return this;
	}

	getProcessors(): Processor[] {
		return [ ...this.processors ];
	}

	getProcessor(name: string): Processor | null {
		return this.getProcessors().find((processor: Processor) => processor.name === name) ?? null;
	}

	start(queue: string, attempt: number) {
		if (this.running) {
			return;
		}
		this.running = true;
		this.loop(queue, attempt).catch((err) => {
			this.running = false;
		});
	}

	stop() { 
		this.running = false; 
	}

	key(name: string, attempt: number): string {
		return this.redisManager.key('queue', `${name}:${attempt}`);
	}

	async select(db: string, queue: string, attempt: number): Promise<TaskInterface[] | null> {
		const arr = await this.redisManager.connection(db).rpop(this.key(queue, attempt), this.portionSize);

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

	async push(db: string, queue: string, task: TaskInterface): Promise<void> {
		const attempt = task.attempt || 1;
		const payload = toJSON({ ...task, attempt, enqueuedAt: Date.now(), });

		if (!isStrFilled(payload)) {
			throw new Error('Empty payload.');
		}
		await this.redisManager.connection(db).rpush(this.key(queue, attempt), payload);
	}
}
