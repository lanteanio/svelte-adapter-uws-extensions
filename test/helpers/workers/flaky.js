/**
 * Worker fixture that fails on the first attempt and succeeds on
 * subsequent attempts. The failure mode is selectable via input:
 *
 *   - `mode: 'throw'`  -- throw a regular Error (recoverable via retry)
 *   - `mode: 'exit'`   -- call process.exit(1) (simulates worker crash)
 *
 * Use to test retry-on-throw and recovery-on-crash behaviors.
 */
export default async function flakyHandler(ctx) {
	if (ctx.attempt === 1) {
		const mode = ctx.input?.mode;
		if (mode === 'exit') {
			process.exit(1);
		}
		const err = new Error('flaky: failing attempt 1');
		err.code = 'FLAKY_FAIL';
		throw err;
	}
	return { recovered: true, attempt: ctx.attempt };
}
