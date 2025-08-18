import { Queue  } from './Queue';

export class QueuePortion extends Queue  {
	protected readonly portionSize: number = 1;

	async allow(queueName: string, attemptIndex: number, data: Array<any>): Promise<boolean> {
		return data.length > 0 && data.filter((item) => !!item).length === data.length;
	}

	async select(queueName: string, attemptIndex: number): Promise<Array<any>> {
		const queueKey = this.queueKey(queueName, attemptIndex);
		const data = await this.redis.lpop(queueKey, this.portionSize);

		return await this.selectAfter(queueName, data);
	}

	async selectAfter(queueName: string, data: Array<any> = []): Promise<Array<any>> {
		let i = 0,
			output = [];

		while (i < data.length) {
			output.push(await super.selectAfter(queueName, data));
		}
		return output;
	}

	async excecuteWrapper(queueName: string, attemptIndex: number, data: Array<any>): Promise<void> {
		let i = 0;

		while (i < data.length) {
			await this.excecute(queueName, attemptIndex, data[i]);
			i++;
		}
	}
}
