import type Redis from 'ioredis';
import { RedisService } from '@nestjs-labs/nestjs-ioredis';
import { 
	Injectable,
	OnModuleInit, 
	OnModuleDestroy, 
	Logger,
} from '@nestjs/common';
import { isFunc } from 'full-utils';
import { PowerQueues } from './PowerQueues';
import {
	ackAndMaybeDeleteBatchScript,
	reservePendingOrNewEntriesScript,
	tryAcquireEntryLockScript,
	markEntryStartedScript,
	completeEntryAndReleaseLockScript,
	releaseEntryLockIfTokenScript,
	heartbeatScript,
	names,
} from './scripts';

@Injectable()
export class NestPowerQueues extends PowerQueues implements OnModuleInit, OnModuleDestroy {
	public readonly logger: Logger = new Logger('NestPowerQueues');
	public abort = new AbortController();
	public redis!: Redis;
	public readonly allowToRun: boolean = false;
	public readonly prefer: string = 'queues';

	constructor(
		private readonly redisService: RedisService,
	) {
		super();

		this.provideRedisConnection();
	}

	private provideRedisConnectionSelect(map: Map<string, any>, name?: string) {
		const definedClient = name ? map.get?.(name) : map.values?.().next?.().value;

		return (definedClient && isFunc((definedClient as any).defineCommand) && isFunc((definedClient as any).xgroup)) ? definedClient as Redis : undefined;
	}

	private provideRedisConnection() {
		const prefer = process.env.REDIS_CLIENT_NAME ?? 'queues';
		const map: Map<string, any> | undefined = (this.redisService as any).clients;

		if (!map?.size) {
			throw new Error('No Redis clients available in RedisService.');
		}
		this.redis = this.provideRedisConnectionSelect(map, prefer) ?? this.provideRedisConnectionSelect(map);

		if (!this.redis) {
			throw new Error(`Invalid Redis client "${prefer}".`);
		}
		this.logger.log?.(`Using Redis client: ${prefer in (map as any) ? prefer : '[first available]'}`);
	}

	async onModuleInit() {
		if (this.allowToRun) {
			const pairs = [
				[ names.AckAndMaybeDeleteBatch, ackAndMaybeDeleteBatchScript ],
				[ names.ReservePendingOrNewEntries, reservePendingOrNewEntriesScript ],
				[ names.TryAcquireEntryLock, tryAcquireEntryLockScript ],
				[ names.MarkEntryStarted, markEntryStartedScript ],
				[ names.CompleteEntryAndReleaseLock, completeEntryAndReleaseLockScript ],
				[ names.ReleaseEntryLockIfToken, releaseEntryLockIfTokenScript ],
				[ names.Heartbeat, heartbeatScript ],
			];

			for (const [ name, code ] of pairs) {
				await this.loadScript(this.saveScript(name, code));
			}
		}
	}

	async onModuleDestroy() {
		if (this.allowToRun) {
			this.abort.abort();
		}
	}
}