import { QueueService  } from './QueueService';
import { ProcessorService } from './ProcessorService';

export class QueueProcessorService extends QueueService  {
	private providedProcessors: Array<ProcessorService> = [];

	setProcessors(processorServices: Array<ProcessorService>): QueueProcessorService {
		this.providedProcessors = [ ...processorServices ];
		return this;
	}

	getProcessors(): Array<ProcessorService> {
		return [ ...this.providedProcessors ];
	}

	getProcessorByName(name: string): ProcessorService | null {
		return (this.providedProcessors ?? []).find((processor: ProcessorService) => processor.name === name) ?? null;
	}

	listen(queueName: string): void {
		const processors = this.getProcessors();
		let i = 0;

		while (i < processors.length) {
			const methods = processors[i].orderMethods();
			let ii = 0;

			while (ii < methods.length) {
				super.listen(`${queueName}.${processors[i].name}.${ii}`);
				ii++;
			}
			i++;
		}
	}

	async excecute(queueName: string, attemptIndex: number, inputData: any): Promise<void> {
		const queueNameSplit = queueName.split(`.`);
		const processorName = queueNameSplit[queueNameSplit.length - 2];
		const methodIndex = queueNameSplit[queueNameSplit.length - 1];
		const processor = this.getProcessorByName(processorName);

		if (processor) {
			const methods = processor.orderMethods();
			const method = methods[methodIndex];

			if (typeof method === `function`) {
				return await this.excecuteProcessorMethod(processor, method, queueName, attemptIndex, inputData);
			}
		}
	}

	async excecuteProcessorMethod(processor: ProcessorService, method: Function, queueName: string, attemptIndex: number, inputData: any): Promise<void> {
		try {
			await this.success(queueName, attemptIndex, inputData, await processor.result(await method.call(processor, attemptIndex, inputData)));
		}
		catch (err) {
			this.retry(queueName, attemptIndex, inputData, err);
		}
	}

	async successOrder(queueName: string, attemptIndex: number, inputData: any, resultData: any) {
	}

	async errorCritical(queueName: string, attemptIndex: number, inputData: any, err) {
	}
}
