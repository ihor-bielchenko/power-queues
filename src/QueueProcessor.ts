import { Queue } from './Queue';
import { Processor } from './Processor';
import { TaskInterface } from './types';

export class QueueProcessor extends Queue {
	private processors: Array<Processor> = [];
	protected methods: Array<Function> = [];

	setProcessors(processors: Processor[]): this {
		this.processors = [ ...processors ];
		return this;
	}

	getProcessors(): Processor[] {
		return [ ...this.processors ];
	}

	getProcessor(name: string): Processor | null {
		return this.getProcessors().find((processor: Processor) => processor.name === name) ?? null;
	}

	protected async execute(queue: string, task: TaskInterface): Promise<any> {
		const processor = this.getProcessor(task.opts?.processor);

		if (!processor) {
			throw new Error('Неизвестный процессор.');
		}
		const method = processor.method(task.opts?.method);

		if (!method) {
			throw new Error('Неизвестный метод.');
		}
		return await method.call(processor, queue, task);
	}
}
