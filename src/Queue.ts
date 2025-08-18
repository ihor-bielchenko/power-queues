import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';

export class Queue {
	protected readonly redis: Redis | null;
	protected readonly threadId: string = uuidv4();
	protected readonly timeout: number = 10;
	protected readonly attempts: number = 1;
	protected readonly displayLog: boolean = true;
	protected readonly displayError: boolean = true;
	protected readonly displayErrorObj: boolean = false;
	protected readonly displayErrorData: boolean = false;

	timestamp(date = new Date()): string {
		const pad = (n: number) => String(n).padStart(2, '0');

		const year = date.getFullYear();
		const month = pad(date.getMonth() + 1);
		const day = pad(date.getDate());

		const hours = pad(date.getHours());
		const minutes = pad(date.getMinutes());
		const seconds = pad(date.getSeconds());

		return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
	}

	queueKey(queueName: string, attemptIndex: number): string {
		return `queue.${queueName}.${attemptIndex}`;
	}

	readyKey(queueName: string, attemptIndex: number): string {
		return `ready.${queueName}.${attemptIndex}`;
	}

	start(queueName: string): void {
		this.listen(queueName);
	}

	listen(queueName: string): void {
		let i = 0;

		while (i < this.attempts) {
			this.attempt(queueName, i);
			i++;
		}
	}

	async wait(timeout?: number): Promise<void> {
		await new Promise((resolve) => setTimeout(resolve, timeout ?? this.timeout));
	}

	async attempt(queueName: string, attemptIndex: number): Promise<void> {
		const readyKey = this.readyKey(queueName, attemptIndex);
		const threadId = this.threadId;

		await this.redis.rpush(readyKey, this.threadId);
		await this.wait();

		setImmediate(() => this.process(queueName, attemptIndex));
		return;
	}

	async process(queueName: string, attemptIndex: number): Promise<void> {
		try {
			await this.processOne(queueName, attemptIndex);
		}
		catch (err) {
			await this.errorWrapper(queueName, attemptIndex, null, err);
		}
		await this.wait();

		setImmediate(() => this.process(queueName, attemptIndex));
		return;
	}

	async processOne(queueName: string, attemptIndex: number): Promise<void> {
		const readyKey = this.readyKey(queueName, attemptIndex);
		const readyThreadId = await this.redis.lpop(readyKey);

		if (readyThreadId) {
			if (readyThreadId === this.threadId) {
				const data = await this.select(queueName, attemptIndex);
				const allow = await this.allow(queueName, attemptIndex, data);

				if (allow) {
					try {
						await this.excecuteWrapper(queueName, attemptIndex, data);
					}
					catch (err) {
						this.retry(queueName, attemptIndex, data, err);
					}
				}
			}
			try {
				await this.redis.rpush(readyKey, this.threadId);
			}
			catch (err) {
				console.log('eeeeeeeeeeee', err);
			}
		}
		else {
			console.log('????????????????????/', readyKey, readyThreadId, Date.now());
		}
	}

	async retry(queueName: string, attemptIndex: number, data: any, err): Promise<number> {
		try {
			if (attemptIndex <= (this.attempts - 1)) {
				const queueKey = this.queueKey(queueName, attemptIndex + 1);
				const dataProcessed = JSON.stringify(data);

				if (attemptIndex < (this.attempts - 1)) {
					return await this.redis.rpush(queueKey, dataProcessed);
				}
			}
			await this.errorWrapper(queueName, attemptIndex, data, err);
		}
		catch (err) {
		}
		return 0;
	}

	async select(queueName: string, attemptIndex: number): Promise<any> {
		const queueKey = this.queueKey(queueName, attemptIndex);
		const data = await this.redis.lpop(queueKey);
		const output = await this.selectAfter(queueName, data);

		return output;
	}

	async selectAfter(queueName: string, data: any): Promise<any> {
		try {
			const parsed = JSON.parse(data);

			return parsed;
		}
		catch (err) {
		}
		return null;
	}

	async allow(queueName: string, attemptIndex: number, data: any): Promise<boolean> {
		return !!data;
	}

	async excecuteWrapper(queueName: string, attemptIndex: number, data: any): Promise<void> {
		await this.excecute(queueName, attemptIndex, data);
	}

	async excecute(queueName: string, attemptIndex: number, data: any): Promise<any> {
		return await this.successWrapper(queueName, attemptIndex, data);
	}

	async successWrapper(queueName: string, attemptIndex: number, data: any): Promise<void> {
		return await this.success(queueName, attemptIndex, data);
	}

	async success(queueName: string, attemptIndex: number, data: any): Promise<void> {
		return data;
	}

	async errorWrapper(queueName: string, attemptIndex: number, data: any, err): Promise<void> {
		try {
			await this.error(queueName, attemptIndex, data, err);
		}
		catch (err) {
			await this.errorMessage(queueName, attemptIndex, data, err);
		}
	}

	async errorMessage(queueName: string, attemptIndex: number, data: any, err): Promise<void> {
		if (this.displayError) {
			console.log(`\n-------------------------------------`);
			console.error(`[ERR]`, this.timestamp());
			console.error(`     `, `Очередь:`, queueName);
			console.error(`     `, `Попытка:`, attemptIndex);
			console.error(`     `, `Результат:`, typeof data);
			console.error(`     `, `Сообщение:`, err.message);

			// if (this.displayErrorObj) {
			// 	console.error(`     `, `Ошибка:`, err);
			// }
			// if (this.displayErrorData) {
			// 	console.error(`     `, `Данные:`, data);
			// }
		}
	}

	async error(queueName: string, attemptIndex: number, data: any, err): Promise<void> {
	}

	async dropKeys(pattern: string, opts?: { count?: number; batch?: number; pauseMs?: number; useUnlink?: boolean; }): Promise<number> {
		const count = opts?.count ?? 1000;
		const batch = opts?.batch ?? 5000;
		const pauseMs = opts?.pauseMs ?? 0;
		const cmd = opts?.useUnlink ?? true ? `unlink` : `del`;
		const stream = this.redis.scanStream({ match: pattern, count });
		let buffer: string[] = [],
			deleted = 0;

		for await (const keys of stream as AsyncIterable<string[]>) {
			buffer.push(...keys);

			while (buffer.length >= batch) {
				const chunk = buffer.splice(0, batch);
				const n = await (this.redis as any)[cmd](...chunk);

				deleted += Number(n) || 0;

				if (pauseMs) {
					await new Promise((r) => setTimeout(r, pauseMs));
				}
			}
		}
		if (buffer.length) {
			const n = await (this.redis as any)[cmd](...buffer);

			deleted += Number(n) || 0;
		}
		return deleted;
	}
}
