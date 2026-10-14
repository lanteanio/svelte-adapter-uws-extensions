import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import adapter from 'svelte-adapter-uws';
import uws from 'svelte-adapter-uws/vite';

const instance = process.env.KIT3_INSTANCE;
export default defineConfig({
	cacheDir: 'node_modules/.vite-' + (instance || 'build'),
	server: { watch: { ignored: ['**/.svelte-kit*/**'] } },
	plugins: [
		sveltekit({
			outDir: instance ? '.svelte-kit-' + instance : '.svelte-kit',
			adapter: adapter({ websocket: { handler: 'src/hooks.ws.js' } })
		}),
		uws({ handler: 'src/hooks.ws.js' })
	]
});
