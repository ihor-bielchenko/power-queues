import { TaskInterface } from './types';

export class Processor {
	public readonly name: string;

	async execute(queueName: string, task: TaskInterface): Promise<any> {
		return true;
	}

	methods(): Array<Function> {
		return [];
	}

	method(index: number): Function {
		return this.methods()[index];
	}
}
