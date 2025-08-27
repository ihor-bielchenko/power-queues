import * as crypto from 'crypto';
import Redis from 'ioredis';
import { RedisService } from '@nestjs-labs/nestjs-ioredis';
import {
	Injectable,
	Logger,
} from '@nestjs/common';
import { format } from 'date-fns';
import { 
	isNumPZ,
	toJSON, 
} from 'full-utils';
import { DistLockInterface } from './types';

@Injectable()
export class RedisManager {
	private readonly logger: Logger = new Logger('RedisManager');
	private readonly connections: any = {};

	constructor(
		private readonly redis: RedisService,
	) {
	}

	timestamp(value?: number): string {
		return format(new Date(value ?? Date.now()), 'yyyy-MM-dd HH');
	}

	wait(ms: number) { 
		return new Promise((r) => setTimeout(r, ms)); 
	}

	key(path: string, name: string): string {
		return `${path}:${name}`;
	}

	connection(db: string): Redis | undefined | null {
		return this.redis['clients'].get(db);
	}

	checkConnection(db: string): boolean {
		return !!this.connection(db) 
			&& ((this.connection(db) as any).status === 'ready'
				|| (this.connection(db) as any).status === 'connecting');
	}

	async dropKeys(db: string, pattern: string): Promise<number> {
		if (!this.checkConnection(db)) {
			throw new Error(`Redis connection error "${db}".`);
		}
		try {
			let cursor = '0',
				total = 0;

			do {
				const [ next, keys ] = await this.connection(db).scan(cursor, 'MATCH', pattern, 'COUNT', 500);

				cursor = next;

				if (keys.length) {
					total += keys.length;

					for (let i = 0; i < keys.length; i += 500) {
						const chunk = keys.slice(i, i + 500);

						if (typeof (this.connection(db) as any).unlink === 'function') {
							await (this.connection(db) as any).unlink(...chunk);
						} 
						else {
							await this.connection(db).del(...chunk);
						}
					}
				}
			} 
			while (cursor !== '0');
			return total;
		} 
		catch (err) {
		}
		throw new Error(`Redis clear error "${db}".`);
	}

	async acquireLock(db: string, logicalKey: string, ttlMs: number = 3000, opts?: { retries?: number; minDelayMs?: number; maxDelayMs?: number; }): Promise<DistLockInterface | null> {
		if (!this.checkConnection(db)) {
			throw new Error(`Redis connection error "${db}".`);
		}
		const lockKey = `lock:${logicalKey}`;
		const token = crypto.randomBytes(16).toString('hex');
		const retries = Math.max(0, opts?.retries ?? 5);
		const minDelay = Math.max(5, opts?.minDelayMs ?? 20);
		const maxDelay = Math.max(minDelay, opts?.maxDelayMs ?? 60);
		let attempt = 0;

		while (attempt < retries) {
			const ok = await this.connection(db).set(lockKey, token, 'PX', ttlMs, 'NX');
			
			if (ok === 'OK') {
				const extend = async (newTtlMs?: number): Promise<boolean> => {
					const ms = Math.max(1, newTtlMs ?? ttlMs);
					const res = await this.connection(db).eval(this.EXTEND_LUA, 1, lockKey, token, ms);
					
					return Number(res) === 1;
				};
				const unlock = async (): Promise<boolean> => {
					const res = await this.connection(db).eval(this.UNLOCK_LUA, 1, lockKey, token);
					
					return Number(res) === 1;
				};
				return { key: lockKey, token, ttlMs, extend, unlock };
			}
			attempt++;

			if (attempt < retries) {
				const sleep = minDelay + Math.floor(Math.random() * (maxDelay - minDelay + 1));
				
				await new Promise((retry) => setTimeout(retry, sleep));
			}
		}
		return null;
	}

	private readonly UNLOCK_LUA = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`;

	private readonly EXTEND_LUA = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`;
}
