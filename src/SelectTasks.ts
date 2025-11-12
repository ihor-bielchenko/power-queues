import type { Constructor } from './types';
import { SelectStuck } from './scripts';
import { mix } from './mix';
import { Script } from './Script';
import { PowerRedis } from 'power-redis';

export function SelectTasks<TBase extends Constructor>(Base: TBase) {
	return class extends mix(Base, Script) {
		public redis!: PowerRedis;
		public readonly consumerHost: string = 'host';
		public readonly stream: string = 'stream';
		public readonly group: string = 'group';
		public readonly workerBatchTasksCount: number = 200;
		public readonly recoveryStuckTasksTimeoutMs: number = 60000;
		public readonly workerLoopIntervalMs: number = 5000;
		public readonly workerSelectionTimeoutMs: number = 80;

		async createGroup(from: '$' | '0-0' = '$') {
			try {
				await (this.redis as any).xgroup('CREATE', this.stream, this.group, from, 'MKSTREAM');
			}
			catch (err: any) {
				const msg = String(err?.message || '');
				
				if (!msg.includes('BUSYGROUP')) {
					throw err;
				}
			}
		}

		async select(): Promise<Array<[ string, any[], number, string, string ]>> {
			let entries: Array<[ string, any[], number, string, string ]> = await this.selectStuck();

			if (!entries?.length) {
				entries = await this.selectFresh();
			}
			return this.normalizeEntries(entries);
		}

		async selectStuck(): Promise<any[]> {
			try {
				const res = await this.runScript('SelectStuck', [ this.stream ], [ this.group, this.consumer(), String(this.recoveryStuckTasksTimeoutMs), String(this.workerBatchTasksCount), String(this.workerSelectionTimeoutMs) ], SelectStuck);

				return (Array.isArray(res) ? res : []) as any[];
			}
			catch (err: any) {
				if (String(err?.message || '').includes('NOGROUP')) {
					await this.createGroup();
				}
			}
			return [];
		}

		async selectFresh(): Promise<any[]> {
			let entries: Array<[ string, any[], number, string, string ]> = [];

			try {
				const res = await (this.redis as any).xreadgroup(
					'GROUP', this.group, this.consumer(),
					'BLOCK', Math.max(2, this.workerLoopIntervalMs | 0),
					'COUNT', this.workerBatchTasksCount,
					'STREAMS', this.stream, '>',
				);

				if (!res?.[0]?.[1]?.length) {
					return [];
				}
				entries = res?.[0]?.[1] ?? [];

				if (!entries?.length) {
					return [];
				}
			}
			catch (err: any) {
				if (String(err?.message || '').includes('NOGROUP')) {
					await this.createGroup();
				}
			}
			return entries;
		}

		normalizeEntries(raw: any): Array<[ string, any[], number, string, string ]> {
			if (!Array.isArray(raw)) {
				return [];
			}
			return Array
				.from(raw || [])
				.map((e) => {
					const id = Buffer.isBuffer(e?.[0]) ? e[0].toString() : e?.[0];
					const kvRaw = e?.[1] ?? [];
					const kv = Array.isArray(kvRaw) ? kvRaw.map((x: any) => (Buffer.isBuffer(x) ? x.toString() : x)) : [];
					
					return [ id as string, kv, 0, '', '' ] as [ string, any[], number, string, string ];
				})
				.filter(([ id, kv ]) => typeof id === 'string' && id.length > 0 && Array.isArray(kv) && (kv.length & 1) === 0)
				.map(([ id, kv ]) => {
					const values = this.values(kv);
					const { idemKey = '', createdAt, job, ...data } = this.payload(values);

					return [ id, data, createdAt, job, idemKey ];
				});
		}

		consumer(): string {
			return `${String(this.consumerHost || 'host')}:${process.pid}`;
		}
	}
}
