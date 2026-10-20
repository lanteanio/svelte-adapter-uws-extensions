import { environment, getServices, liveServices, revision, shutdown, state } from '#lib/server/services.js';

export { shutdown };

export function init() {
	state.initialized = true;
}

export async function upgrade({ cookies }) {
	const identity = await getServices().sessions.get(cookies.token || '');
	return identity || false;
}

export async function open(ws, ctx) {
	const services = getServices();
	await services.bus.hooks.open(ws, ctx);
	await services.registry.hooks.open(ws, ctx);
	ctx.platform.send(ws, 'fixture', 'ready', {
		environment, graph: state.id, initialized: state.initialized, revision,
		userId: ws.getUserData().userId,
		registry: services.registry.instanceId
	});
}

export async function message(ws, { data, platform }) {
	const command = JSON.parse(Buffer.from(data).toString());
	const services = getServices();
	const distributed = services.bus.wrap(platform);
	state.messages++;
	if (command.operation === 'publish') {
		distributed.publish(command.topic, 'message', command.value);
	}
	if (command.operation === 'redis') {
		await services.redisReplay.publish(distributed, command.topic, 'message', command.value);
	}
	if (command.operation === 'postgres') {
		await services.postgresReplay.publish(distributed, command.topic, 'message', command.value);
	}
	if (command.operation === 'send') {
		await services.registry.send(command.userId, command.topic, 'direct', command.value);
	}
	platform.send(ws, 'fixture', 'handled', { ref: command.ref });
}

// A connection that closes after shutdown must not create new clients.
export async function close(ws, ctx) {
	await liveServices()?.registry.hooks.close(ws, ctx);
}
