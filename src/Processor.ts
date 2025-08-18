
export class Processor {
	public readonly defaultName: string;

	get name(): string {
		return this.defaultName ?? this.constructor.name;
	}

	async excecute(queueName: string, attemptIndex: number, data: any): Promise<any> {
		return data;
	}

	async success(data: any): Promise<any> {
		return data;
	}

	async successMethod(data: any): Promise<any> {
		return data;
	}

	timestamp(date = new Date()): string {
		const pad = (n: number) => String(n).padStart(2, '0');

		const year = date.getFullYear();
		const month = pad(date.getMonth() + 1);
		const day = pad(date.getDate());

		const hours = pad(date.getHours());
		const minutes = pad(date.getMinutes());
		const seconds = pad(date.getSeconds());

		return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
	}

	orderMethods(): Array<Function> {
		return [];
	}

	errorMethods(): Array<Function> {
		return [];
	}

	getMethods(): Array<Function> {
		return [ ...this.orderMethods() ];
	}

	getMethodsLength(): number {
		return this.orderMethods().length;
	}

	getMethod(index: number): Function {
		return this.orderMethods()[index];
	}

	isErrorMethod(method: Function): boolean {
		return !!this.errorMethods().find((item) => item.name === method.name);
	}
}
