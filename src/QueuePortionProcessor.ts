import { QueueProcessor  } from './QueueProcessor';
import { Processor } from './Processor';

export class QueuePortionProcessor extends QueueProcessor  {
	protected readonly portionSize: number = 1;

	async allow(queueName: string, attemptIndex: number, data: Array<any>): Promise<boolean> {
		return data.length > 0 && data.filter((item) => !!item).length === data.length;
	}

	async select(queueName: string, attemptIndex: number): Promise<Array<any>> {
		const queueKey = this.queueKey(queueName, attemptIndex);
		const data = await this.redis.lpop(queueKey, this.portionSize);

		return await this.selectAfter(data);
	}

	async selectAfter(data: Array<any>): Promise<Array<any>> {
		let i = 0,
			output = [];

		while (i < data.length) {
			output.push(await super.selectAfter(data));
		}
		return output;
	}

	async excecuteWrapper(queueName: string, attemptIndex: number, data: Array<any>): Promise<Array<any>> {
		const keyData = this.keyData(queueName);
		const processorName = keyData.processorName;
		const processor = this.getProcessorByName(processorName);

		if (!processor || !processor.excecute) {
			return;
		}
		let i = 0,
			output = [];

		while (i < data.length) {
			output.push(await this.successWrapper(queueName, attemptIndex, await this.excecute(queueName, attemptIndex, await processor.excecute.call(processor, attemptIndex, data[i]))));
			i++;
		}
		return output;
	}
}
