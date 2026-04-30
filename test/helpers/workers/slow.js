export default async function slowHandler(ctx) {
	await new Promise((r) => setTimeout(r, ctx.input?.delayMs ?? 100));
	return { ok: true, fence: ctx.fence };
}
