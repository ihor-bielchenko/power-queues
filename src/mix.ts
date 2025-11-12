import type { Constructor } from './types';

export function mix<BaseT extends Constructor, Mixins extends Array<(b: Constructor) => Constructor>>(Base: BaseT, ...mixins: Mixins): any {
	return mixins.reduce((acc, mixin) => mixin(acc), Base);
}
