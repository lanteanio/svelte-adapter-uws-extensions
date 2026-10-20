import { json } from '@sveltejs/kit';
import { environment, getServices, revision, state } from '#lib/server/services.js';

// The development edit check rewrites this value.
const route = 'initial';

export function GET() {
	return json({
		environment, graph: state.id, initialized: state.initialized, messages: state.messages,
		revision, route, listeners: process.listenerCount('sveltekit:shutdown')
	});
}

export async function POST({ request }) {
	const command = await request.json();
	const services = getServices();
	switch (command.operation) {
		case 'session-set':
			await services.sessions.set(command.token, command.value);
			return json({ stored: true });
		case 'session-get':
			return json(await services.sessions.get(command.token));
		case 'session-delete':
			return json(await services.sessions.delete(command.token));
		case 'lookup':
			return json(await services.registry.lookup(command.userId));
		case 'history':
			return json(await services[command.backend + 'Replay'].since(command.topic, 0));
		default:
			return json({ error: 'Unknown operation' }, { status: 400 });
	}
}
