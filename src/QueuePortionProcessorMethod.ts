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

	async successWrapper(queueName: string, attemptIndex: number, data: any): Promise<void> {
		const keyData = this.keyData(queueName);
		const processorName = keyData.processorName;
		const methodIndex = keyData.methodIndex;
		const processor = this.getProcessorByName(processorName);

		if (!processor) {
			return;
		}
		const nextMethodIndex = methodIndex + 1;
		const nextMethod = processor.getMethod(nextMethodIndex);
		const methodsLength = processor.getMethodsLength();

		if (nextMethod) {
			const queueKey = await this.queueKey(`${queueName}.${nextMethodIndex}`, attemptIndex);
			const dataProcessed = JSON.stringify(data);

			await this.redis.rpush(queueKey, dataProcessed);
			await this.successMethod(queueName, attemptIndex, data);
		}
		else if (nextMethodIndex === (methodsLength - 1)) {
			await super.successWrapper(queueName, attemptIndex, data);
		}
	}

	async successMethod(queueName: string, attemptIndex: number, data: any): Promise<void> {
	}

	async errorWrapper(queueName: string, attemptIndex: number, data: any, err): Promise<void> {
		const keyData = this.keyData(queueName);
		const processorName = keyData.processorName;
		const methodIndex = keyData.methodIndex;
		const processor = this.getProcessorByName(processorName);

		if (!processor) {
			return;
		}
		const method = processor.getMethod(methodIndex);
		const isErrorMethod = processor.isErrorMethod(method);

		if (isErrorMethod) {
			await super.errorWrapper(queueName, attemptIndex, data, err);
		}
	}
}
