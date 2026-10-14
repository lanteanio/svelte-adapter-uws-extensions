import { createServer } from 'vite';

const server = await createServer({
	server: { host: '127.0.0.1', port: Number(process.env.PORT), strictPort: true }
});
await server.listen();
process.on('message', async (message) => {
	if (message === 'stop') {
		await server.close();
		process.disconnect();
	}
});
