import { QueueProcessorService  } from './QueueProcessorService';
import { ProcessorService } from './ProcessorService';

export class QueuePortionProcessorService extends QueueProcessorService  {
	public readonly portionSize: number = 1;

	async select(queueName: string, attemptIndex: number): Promise<Array<any>> {
		return (await this.redisService.lpop(`queue.${queueName}.${attemptIndex}`, this.portionSize))
			.map((item) => this.dataProcessingAfterSelect(item));
	}

	async allowExcecute(queueName: string, attemptIndex: number, inputData: Array<any>): Promise<boolean> {
		return inputData.length > 0 && inputData.filter((item) => !!item).length === inputData.length;
	}
	
	async excecuteProcessorMethod(processor: ProcessorService, method: Function, queueName: string, attemptIndex: number, inputData: Array<any>): Promise<void> {
		let i = 0;

		while (i < inputData.length) {
			super.excecuteProcessorMethod(processor, method, queueName, attemptIndex, inputData[i]);
			i++;
		}
	}
}
