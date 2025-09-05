
export interface DistLockInterface {
	key: string;
	token: string;
	ttlMs: number;
	unlock: () => Promise<boolean>;
}

export interface LockOptsInterface { 
	ttlMs: number;
	retries?: number; 
	minDelayMs?: number; 
	maxDelayMs?: number; 
}

export interface TaskOptsInterface {
	attempt?: number;
	method?: number;
	processor?: string;
	progress?: boolean;
}

export interface TaskInterface {
	payload: any;
	id?: string;
	opts?: TaskOptsInterface;
	enqueuedAt?: number;
}

export interface TasksInterface {
	payloads: Array<any>;
	id?: string;
	opts?: TaskOptsInterface;
	enqueuedAt?: number;
}
