import { QueuePortionService  } from './QueuePortionService';
import { ProcessorService } from './ProcessorService';

export class QueuePortionProcessorService extends QueuePortionService  {
	private providedProcessors: Array<ProcessorService> = [];

	setProcessors(processorServices: Array<ProcessorService>): QueuePortionProcessorService {
		this.providedProcessors = [ ...processorServices ];
		return this;
	}

	getProcessors(): Array<ProcessorService> {
		return [ ...this.providedProcessors ];
	}

	getProcessorIndexByName(name: string): number {
		return (this.providedProcessors ?? []).findIndex((item) => item.name === name);
	}

	getProcessorByIndex(index: number): ProcessorService | null {
		return (this.providedProcessors ?? [])[index] ?? null;
	}

	getProcessorByName(name: string): ProcessorService | null {
		return (this.providedProcessors ?? []).find((processor: ProcessorService) => processor.name === name) ?? null;
	}

	getProcessorByDataItem(dataItem: any): ProcessorService | null {
		return null;
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

	async excecute(queueName: string, attemptIndex: number, inputData: Array<any>): Promise<void> {
		const queueNameSplit = queueName.split(`.`);
		const processorName = queueNameSplit[queueNameSplit.length - 2];
		const methodIndex = queueNameSplit[queueNameSplit.length - 1];
		const processor = this.getProcessorByName(processorName);
		let isBadReturn = true;

		if (processor) {
			const methods = processor.orderMethods();
			const method = methods[methodIndex];

			if (typeof method === `function`) {
				let i = 0;

				while (i < inputData.length) {
					try {
						this.success(queueName, attemptIndex, inputData[i], await method.call(processor, attemptIndex, inputData[i]));
						isBadReturn = false;
					}
					catch (err) {
						this.retry(queueName, attemptIndex, inputData[i], err);
					}
					i++;
				}
			}
		}
		isBadReturn 
			&& this.error(queueName, attemptIndex, inputData, new Error(`Undefined processor or method is not a function.`));
	}
}
