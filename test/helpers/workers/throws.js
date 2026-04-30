export default async function throwHandler(ctx) {
	const err = new Error('worker handler exploded');
	err.code = 'WORKER_BOOM';
	err.detail = ctx.input;
	throw err;
}
