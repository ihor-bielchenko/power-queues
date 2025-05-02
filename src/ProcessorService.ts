
export class ProcessorService {
	public readonly customName: string;

	orderMethods(): Array<Function> {
		return [];
	}

	criticalOrderMethods(): Array<Function> {
		return [];
	}

	async result(data: any): Promise<any> {
		return data;
	}

	get name(): string {
		return this.customName ?? this.constructor.name;
	}
}
