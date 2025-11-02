
export function waitAbortable(ms: number, abort?: AbortSignal) {
	return new Promise<void>((resolve) => {
		if (abort?.aborted) {
			return resolve();
		}
		const t = setTimeout(() => {
			if (abort) {
				abort.removeEventListener('abort', onAbort as any);
			}
			resolve();
		}, ms);
		(t as any).unref?.();

		function onAbort() { 
			clearTimeout(t); 
			resolve(); 
		}
		abort?.addEventListener('abort', onAbort, { once: true });
	});
}

export function randBase(): number {
	return 15 + Math.floor(Math.random() * 35);
}

export function randExtra(contended: number): number {
	return Math.min(250, 15 * contended + Math.floor(Math.random() * 40));
}
