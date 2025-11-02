
export type Field = string | number | boolean | null | undefined;
export type Fields = Record<string, Field>;

export type SavedScript = { 
	sha?: string; 
	source: string; 
};

export type LoopOpts = {
	consumer: string;
	abort?: AbortSignal;
	tasksCountPerIteration: number;
	pendingKeyTimeoutMs: number;
	processingLockTimeoutMs: number;
	heartbeatIntervalMs: number;
	streamReadTimeoutMs: number;
	scriptExecutionTimeLimitMs: number;
	idempotencyKeyTimeoutSec: number;
	deleteOnAck: boolean | number;
};

export type EntryKeys = {
	prefix: string; 
	doneKey: string; 
	lockKey: string; 
	startKey: string; 
	token: string;
};

export type AddTasksOpts = {
	maxlen?: number;
	minidWindowMs?: number;
	approx?: boolean;
	exact?: boolean;
	nomkstream?: boolean;
	chunkSize?: number;
	parallel?: number;
	trimLimit?: number;
	minidExact?: boolean;
}

export type Task =
	| { id?: string; fields: Fields; }
	| { id?: string; flat: Field[]; };