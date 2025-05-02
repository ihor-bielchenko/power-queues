import { QueuePortionProcessorService } from './QueuePortionProcessorService';

export class QueuePortionProcessorOrderService extends QueuePortionProcessorService {
	async success(queueName: string, attemptIndex: number, inputData: any, resultData: any): Promise<void> {
		const queueNameSplit = queueName.split(`.`);
		const processorName = queueNameSplit[queueNameSplit.length - 2];
		const methodIndex = Number(queueNameSplit[queueNameSplit.length - 1]);
		const processor = this.getProcessorByName(processorName);

		if (processor) {
			const methods = processor.orderMethods();
			const newMethodIndex = methodIndex + 1;

			if (typeof methods[newMethodIndex] === `function`) {
				const key = `${queueNameSplit.slice(0, -2).join(`.`)}.${processorName}.${newMethodIndex}.0`;

				await this.redisService.rpush(key, JSON.stringify({ 
					...inputData, 
					resultData: { 
						...(inputData.resultData || {}), 
						...(resultData || {}), 
					}, 
				}));
			}
			else if (newMethodIndex > (methods.length - 1)) {
				await this.catchProcessorOrderSuccess(queueName, attemptIndex, inputData, resultData);
			}
		}
		await super.success(queueName, attemptIndex, inputData, resultData);
	}

	async error(queueName: string, attemptIndex: number, inputData: any, err): Promise<void> {
		const queueNameSplit = queueName.split(`.`);
		const processorName = queueNameSplit[queueNameSplit.length - 2];
		const methodIndex = queueNameSplit[queueNameSplit.length - 1];
		const processor = this.getProcessorByName(processorName);

		if (processor) {
			const criticalMethods = processor.criticalOrderMethods();

			criticalMethods[methodIndex]
				? await this.catchCriticalError(queueName, attemptIndex, inputData, err)
				: await this.success(queueName, attemptIndex, inputData, {});
		}
	}

	async catchProcessorOrderSuccess(queueName: string, attemptIndex: number, inputData: any, resultData: any): Promise<void> {
	}

	async catchCriticalError(queueName: string, attemptIndex: number, inputData: any, err): Promise<void> {
	}
}
