import { wait } from 'full-utils';
import { PowerRedis } from 'power-redis';
import { Script } from './Script';
import { AddTasks } from './AddTasks';
import { SelectTasks } from './SelectTasks';
import { ExecuteTasks } from './ExecuteTasks';
import { mix } from './mix';

class Base {
}

export class PowerQueues extends mix(Base, AddTasks, SelectTasks, ExecuteTasks) {
	public abort = new AbortController();
	public redis!: PowerRedis;

	async onSelected(data: Array<[ string, any[], number, string, string ]>) {
		return data;
	}

	async onExecute(id: string, payload: any, createdAt: number, job: string, key: string) {
	}

	async onExecuted(data: Array<[ string, any[], number, string, string ]>) {
	}

	async onSuccess(id: string, payload: any, createdAt: number, job: string, key: string) {
	}

	async runQueue() {
		await this.createGroup('0-0');
		await this.consumerLoop();
	}

	async consumerLoop() {
		const signal = this.signal();

		while (!signal?.aborted) {
			try {
				const tasks = await this.select();

				if (!Array.isArray(tasks) || !(tasks.length > 0)) {
					await wait(600);
					continue;
				}
				const tasksP = await this.onSelected(tasks);
				const ids = await this.execute((Array.isArray(tasksP) && tasksP.length > 0) ? tasksP : tasks);

				if (Array.isArray(ids) && ids.length > 0) {
					await this.approve(ids);
				}
			}
			catch (err: any) {
				await wait(600);
			}
		}
	}
}