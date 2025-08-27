
export interface DistLockInterface {
	key: string;
	token: string;
	ttlMs: number;
	extend: (newTtlMs?: number) => Promise<boolean>;
	unlock: () => Promise<boolean>;
}

export interface TaskInterface {
	payload: object;
	attempt?: number;
	processor?: string;
	method?: number;
	enqueuedAt?: number;
}
