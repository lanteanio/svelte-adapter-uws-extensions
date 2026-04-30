export default async function abortableHandler(ctx) {
	// Wait until signal fires or 5s elapses
	return await new Promise((resolve, reject) => {
		const timer = setTimeout(() => resolve({ completed: true, aborted: false }), 5000);
		ctx.signal.addEventListener('abort', () => {
			clearTimeout(timer);
			reject(new Error('aborted by signal: ' + (ctx.signal.reason?.message || 'no reason')));
		}, { once: true });
	});
}
