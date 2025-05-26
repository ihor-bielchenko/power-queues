import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';

export class QueueService {
	protected readonly redisService: Redis | null;
	public readonly threadId: string = uuidv4();
	public readonly loopTimeout: number = 0;
	public readonly attempts: number = 1;

	listen(queueName: string): void {
		let i = 0;

		while (i < this.attempts) {
			this.listenByAttempt(queueName, i);
			i++;
		}
	}

	async listenByAttempt(queueName: string, attemptIndex: number): Promise<void> {
		await this.redisService.rpush(`ready.${queueName}.${attemptIndex}`, this.threadId);

		setTimeout(() => this.loop(queueName, attemptIndex), this.loopTimeout);
		return;
	}

	async loop(queueName: string, attemptIndex: number): Promise<void> {
		try {
			const readyThreadId = await this.redisService.lpop(`ready.${queueName}.${attemptIndex}`);

			if (readyThreadId) {
				if (readyThreadId === this.threadId) {
					const inputData = await this.select(queueName, attemptIndex);

					if (await this.allowExcecute(queueName, attemptIndex, inputData)) {
						try {
							await this.excecute(queueName, attemptIndex, inputData);
						}
						catch (err) {
							this.retry(queueName, attemptIndex, inputData, err);
						}
					}
				}
				await this.redisService.rpush(`ready.${queueName}.${attemptIndex}`, readyThreadId);
			}
		}
		catch (err) {
			await this.error(queueName, attemptIndex, null, err);
		}
		setTimeout(() => this.loop(queueName, attemptIndex), this.loopTimeout);
		return;
	}

	async select(queueName: string, attemptIndex: number): Promise<any> {
		return await this.dataProcessingAfterSelect(await this.redisService.lpop(`queue.${queueName}.${attemptIndex}`));
	}

	async dataProcessingAfterSelect(inputData: any): Promise<any> {
		try {
			return JSON.parse(inputData);
		}
		catch (err) {
		}
		return null;
	}

	async allowExcecute(queueName: string, attemptIndex: number, inputData: any): Promise<boolean> {
		return !!inputData;
	}

	async excecute(queueName: string, attemptIndex: number, inputData: any): Promise<void> {
		this.success(queueName, attemptIndex, inputData, await this.excecuteWrapper(queueName, attemptIndex, inputData));
	}

	async excecuteWrapper(queueName: string, attemptIndex: number, inputData: any): Promise<any> {
		return {};
	}

	async success(queueName: string, attemptIndex: number, inputData: any, resultData: any): Promise<void> {
	}

	async error(queueName: string, attemptIndex: number, inputData: any, err): Promise<void> {
	}

	async retry(queueName: string, attemptIndex: number, inputData: any, err): Promise<number> {
		try {
			if (attemptIndex < (this.attempts - 1)) {
				return await this.redisService.rpush(`queue.${queueName}.${attemptIndex + 1}`, JSON.stringify(inputData));
			}
			this.error(queueName, attemptIndex, inputData, err);
		}
		catch (err) {
		}
		return 0;
	}
}
