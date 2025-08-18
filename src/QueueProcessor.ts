import { Queue  } from './Queue';
import { Processor } from './Processor';

export class QueueProcessor extends Queue  {
	private processors: Array<Processor> = [];

	keyData(queueName: string): { processorName: string; } {
		const queueNameSplit = queueName.split(`.`);
		const processorName = queueNameSplit[queueNameSplit.length - 1];

		return {
			processorName,
		};
	}

	setProcessors(processors: Array<Processor>): QueueProcessor {
		this.processors = [ ...processors ];
		return this;
	}

	getProcessors(): Array<Processor> {
		return [ ...this.processors ];
	}

	getProcessorByName(name: string): Processor | null {
		return this.getProcessors().find((processor: Processor) => processor.name === name) ?? null;
	}

	start(queueName: string): void {
		const processors = this.getProcessors();
		let i = 0;

		while (i < processors.length) {
			const processor = processors[i];
			const queueKey = `${queueName}.${processor.name}`;
			
			this.listen(queueKey);
			i++;
		}
	}

	async excecuteWrapper(queueName: string, attemptIndex: number, data: any): Promise<void> {
		const keyData = this.keyData(queueName);
		const processorName = keyData.processorName;
		const processor = this.getProcessorByName(processorName);

		if (!processor || !processor.excecute) {
			return;
		}
		await this.excecute(queueName, attemptIndex, await processor.excecute.call(processor, attemptIndex, data));
	}

	listen(queueName: string): void {
		const processors = this.getProcessors();
		let i = 0;

		while (i < processors.length) {
			const processor = processors[i];
			let ii = 0;

			while (ii < this.attempts) {
				this.attempt(queueName, ii);
				ii++;
			}
			i++;
		}
	}
}
