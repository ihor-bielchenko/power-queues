import { QueueService  } from './QueueService';

export class QueuePortionService extends QueueService  {
	public readonly portionSize: number = 1;

	async select(queueName: string, attemptIndex: number): Promise<Array<any>> {
		let i = 0,
			output = [];

		while (i < this.portionSize) {
			const item = await super.select(queueName, attemptIndex);

			if (item) {
				output.push(item);
			}
			i++;
		}
		return output;
	}

	async allowExcecute(queueName: string, attemptIndex: number, inputData: Array<any>): Promise<boolean> {
		return inputData.length > 0 && inputData.filter((item) => !!item).length === inputData.length;
	}

	async excecute(queueName: string, attemptIndex: number, inputData: Array<any>): Promise<void> {
		let i = 0;

		while (i < inputData.length) {
			try {
				this.success(queueName, attemptIndex, inputData[i], await super.excecute(queueName, attemptIndex, inputData[i]));
			}
			catch (err) {
				this.retry(queueName, attemptIndex, inputData[i], err);
			}
			i++;
		}
	}
}
