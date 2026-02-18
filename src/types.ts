
export type Constructor<T = {}> = new (...args: any[]) => T;

export type SavedScript = { 
	codeReady?: string; 
	codeBody: string; 
};

export type AddTasksOptions = {
	nomkstream?: boolean;
	maxlen?: number;
	minidWindowMs?: number;
	minidExact?: boolean;
	approx?: boolean;
	exact?: boolean;
	trimLimit?: number;
	idemKey?: string;
	job?: string;
	status?: boolean;
	statusTimeoutMs?: number;
	attempt?: number;
	createdAt?: number;
};

export type IdempotencyKeys = {
	prefix: string; 
	doneKey: string; 
	lockKey: string; 
	startKey: string; 
	token: string;
};

export type Task = { 
	job: string; 
	id?: string; 
	createdAt?: number; 
	payload: any; 
	idemKey?: string; 
	attempt: number; 
};
