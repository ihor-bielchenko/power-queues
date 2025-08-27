import { 
	isStrFilled,
	isObjFilled,
	isFunc, 
} from 'full-utils';
import { RedisManager } from './RedisManager';
import { Queue } from './Queue';
import { TaskInterface } from './types';

export class QueueMethod extends Queue {
	public readonly redisManager: RedisManager;
	public readonly db: string;

	protected async execute(queue: string, task: TaskInterface): Promise<any> {
		const processor = this.getProcessor(task.processor);

		if (!processor) {
			throw new Error('Неизвестный процессор.');
		}
		const method = processor.method(task.method);

		if (!isFunc(method)) {
			throw new Error('Неизвестный метод.');
		}
		return await method.call(processor, task);
	}

	protected async onSuccess(queue: string, task: TaskInterface, result: any): Promise<void> {
		const processor = this.getProcessor(task.processor);

		if (!processor) {
			throw new Error('Неизвестный процессор.');
		}
		if ((processor.methods().length - 1) > task.method) {
			await this.push(this.db, queue, { ...task, method: (task.method || 0) + 1 });
		}
	}
}
