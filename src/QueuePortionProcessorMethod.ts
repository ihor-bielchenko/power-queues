import { QueuePortionProcessor } from './QueuePortionProcessor';

export class QueuePortionProcessorMethod extends QueuePortionProcessor {
	keyData(queueName: string): { processorName: string; methodIndex: number; } {
		const queueNameSplit = queueName.split(`.`);
		const processorName = queueNameSplit[queueNameSplit.length - 2];
		const methodIndex = Number(queueNameSplit[queueNameSplit.length - 1]);

		return {
			processorName,
			methodIndex,
		};
	}

	start(queueName: string): void {
		const processors = this.getProcessors();
		let i = 0;

		while (i < processors.length) {
			const processor = processors[i];
			const methods = processor.getMethods();
			let ii = 0;

			while (ii < methods.length) {
				this.listen(`${queueName}.${processor.name}.${ii}`);
				ii++;
			}
			i++;
		}
	}

	async excecuteWrapper(queueName: string, attemptIndex: number, data: Array<any>): Promise<void> {
		const keyData = this.keyData(queueName);
		const processorName = keyData.processorName;
		const methodIndex = keyData.methodIndex;
		const processor = this.getProcessorByName(processorName);

		if (!processor 
			|| !processor.excecute 
			|| !Array.isArray(data)
			|| !(methodIndex >= 0)
			|| !(data.length > 0)) {
			return;
		}
		const method = processor.getMethod(methodIndex);
		let i = 0;

		if (!method) {
			return;
		}
		await (new Promise((resolve, reject) => {
			let length = data.length,
				ready = 0,
				i = 0;

			while (i < length) {
				const item = data[i];

				method
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

	async successWrapper(queueName: string, attemptIndex: number, data: any): Promise<void> {
		const keyData = this.keyData(queueName);
		const processorName = keyData.processorName;
		const methodIndex = keyData.methodIndex;
		const processor = this.getProcessorByName(processorName);

		if (!processor) {
			return;
		}
		const methodsLength = processor.getMethodsLength();
		
		if (methodIndex === (methodsLength - 1)) {
			await super.successWrapper(queueName, attemptIndex, data);
		}
		else {
			const nextMethodIndex = methodIndex + 1;
			const nextMethod = processor.getMethod(nextMethodIndex);

			if (nextMethod) {
				const queueKey = await this.queueKey(`poll.${processorName}.${nextMethodIndex}`, 0);
				const dataProcessed = JSON.stringify(data);

				await this.redis.rpush(queueKey, dataProcessed);
				await this.successMethod(queueName, attemptIndex, data);
			}
		}
		return data;
	}

	async successMethod(queueName: string, attemptIndex: number, data: any): Promise<void> {
	}

	async errorWrapper(queueName: string, attemptIndex: number, data: any, err): Promise<void> {
		const keyData = this.keyData(queueName);
		const processorName = keyData.processorName;
		const processor = this.getProcessorByName(processorName);

		if (!processor) {
			return;
		}		
		await super.errorWrapper(queueName, attemptIndex, data, err);
	}
}
