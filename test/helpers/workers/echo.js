export default async function echoHandler(ctx) {
	return {
		echoed: ctx.input,
		fence: ctx.fence,
		idempotencyKey: ctx.idempotencyKey,
		attempt: ctx.attempt
	};
}
