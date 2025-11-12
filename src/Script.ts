import type { 
	Constructor,
	SavedScript, 
} from './types';
import {
	XAddBulk,
	Approve,
	IdempotencyAllow,
	IdempotencyStart,
	IdempotencyDone,
	IdempotencyFree,
	SelectStuck,
} from './scripts';
import { PowerRedis } from 'power-redis';

export function Script<TBase extends Constructor>(Base: TBase) {
	return class extends Base {
		public readonly strictCheckingConnection: boolean = [ 'true', 'on', 'yes', 'y', '1' ].includes(String(process.env.REDIS_STRICT_CHECK_CONNECTION ?? '').trim().toLowerCase());
		public readonly scripts: Record<string, SavedScript> = {};
		public redis!: PowerRedis;

		async runScript(name: string, keys: string[], args: (string|number)[], defaultCode?: string) {
			if (!this.scripts[name]) {
				if (typeof defaultCode !== 'string' || !(defaultCode.length > 0)) {
					throw new Error(`Undefined script "${name}". Save it before executing.`);
				}
				this.saveScript(name, defaultCode);
			}
			if (!this.scripts[name].codeReady) {
				this.scripts[name].codeReady = await this.loadScript(this.scripts[name].codeBody);
			}
			try {
				return await (this.redis as any).evalsha(this.scripts[name].codeReady!, keys.length, ...keys, ...args);
			}
			catch (err: any) {
				if (String(err?.message || '').includes('NOSCRIPT')) {
					this.scripts[name].codeReady = await this.loadScript(this.scripts[name].codeBody);

					return await (this.redis as any).evalsha(this.scripts[name].codeReady!, keys.length, ...keys, ...args);
				}
				throw err;
			}
		}

		async loadScripts(full: boolean = false): Promise<void> {
			const scripts = full
				? [
					[ 'XAddBulk', XAddBulk ],
					[ 'Approve', Approve ],
					[ 'IdempotencyAllow', IdempotencyAllow ],
					[ 'IdempotencyStart', IdempotencyStart ],
					[ 'IdempotencyDone', IdempotencyDone ],
					[ 'IdempotencyFree', IdempotencyFree ],
					[ 'SelectStuck', SelectStuck ]
				]
				: [
					[ 'XAddBulk', XAddBulk ],
				];

			for (const [ name, code ] of scripts) {
				await this.loadScript(this.saveScript(name, code));
			}
		}

		async loadScript(code: string): Promise<string> {
			for (let i = 0; i < 3; i++) {
				try {
					return await this.redis.script('LOAD', code);
				}
				catch (e) {
					if (i === 2) {
						throw e;
					}
					await new Promise((r) => setTimeout(r, 10 + Math.floor(Math.random() * 40)));
				}
			}
			throw new Error('Load lua script failed.');
		}

		saveScript(name: string, codeBody: string): string {
			if (typeof codeBody !== 'string' || !(codeBody.length > 0)) {
				throw new Error('Script body is empty.');
			}
			this.scripts[name] = { codeBody };

			return codeBody;
		}
	}
}
