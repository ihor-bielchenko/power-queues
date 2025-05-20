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
		this.error(queueName, attemptIndex, inputData, new Error(`Undefined processor or method is not a function.`));
	}

	async excecuteProcessorMethod(processor: ProcessorService, method: Function, queueName: string, attemptIndex: number, inputData: any): Promise<void> {
		this.success(queueName, attemptIndex, inputData, await processor.result(await method.call(processor, attemptIndex, inputData)));
	}
}
