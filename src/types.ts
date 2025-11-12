import type { JsonPrimitiveOrUndefined } from 'power-redis';

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
	id?: string;
};

export type IdempotencyKeys = {
	prefix: string; 
	doneKey: string; 
	lockKey: string; 
	startKey: string; 
	token: string;
};

export type Task =
	| { job: string; id?: string; createdAt?: number; payload: any; idemKey?: string; }
	| { job: string; id?: string; createdAt?: number; flat: JsonPrimitiveOrUndefined[]; idemKey?: string; };
