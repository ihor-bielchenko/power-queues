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
		const output = await this.selectAfter(queueName, data);

		return output;
	}

	async selectAfter(queueName: string, data: Array<any>): Promise<Array<any>> {
		let i = 0,
			output = [];

		if (Array.isArray(data) && data.length > 0) {
			while (i < data.length) {
				output.push(await super.selectAfter(queueName, data[i]));
				i++;
			}
		}
		return output;
	}

	async excecuteWrapper(queueName: string, attemptIndex: number, data: Array<any>): Promise<void> {
		const keyData = this.keyData(queueName);
		const processorName = keyData.processorName;
		const processor = this.getProcessorByName(processorName);

		if (!processor 
			|| !processor.excecute 
			|| !Array.isArray(data)
			|| !(data.length > 0)) {
			return;
		}
		await (new Promise((resolve, reject) => {
			let length = data.length,
				ready = 0,
				i = 0;

			while (i < length) {
				const item = data[i];

				processor
					.excecute
					.call(processor, queueName, attemptIndex, item)
					.then((result) => {
						this.excecute(queueName, attemptIndex, result)
							.then(() => {
								if (ready >= (length - 1)) {
									resolve(ready);
								}
								ready += 1;
							})
							.catch((err) => {
								this.retry(queueName, attemptIndex, item, err);

								if (ready >= (length - 1)) {
									resolve(ready);
								}
								ready += 1;
							});
					})
					.catch((err) => {
						this.retry(queueName, attemptIndex, item, err);

						if (ready >= (length - 1)) {
							resolve(ready);
						}
						ready += 1;
					});
				i++;
			}
		}));
	}
}
