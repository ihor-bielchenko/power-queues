import { Queue  } from './Queue';

export class QueuePortion extends Queue  {
	protected readonly portionSize: number = 1;

	async allow(queueName: string, attemptIndex: number, data: Array<any>): Promise<boolean> {
		return data.length > 0 && data.filter((item) => !!item).length === data.length;
	}

	async select(queueName: string, attemptIndex: number): Promise<Array<any>> {
		const queueKey = this.queueKey(queueName, attemptIndex);
		const data = await this.redis.lpop(queueKey, this.portionSize);

		if (Array.isArray(data) && data.length > 0) {
			return await this.selectAfter(queueName, data);
		}
		return [];
	}

	async selectAfter(queueName: string, data: Array<any> = []): Promise<Array<any>> {
		let i = 0,
			output = [];

		while (i < data.length) {
			output.push(await super.selectAfter(queueName, data[i]));
			i++;
		}
		return output;
	}
}
