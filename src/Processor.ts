
export class Processor {
	public readonly name: string;

	async execute(queueName: string, attemptIndex: number, data: any): Promise<any> {
		return data;
	}

	methods(): Array<Function> {
		return [];
	}

	method(index: number): Function {
		return this.methods()[index];
	}
}
