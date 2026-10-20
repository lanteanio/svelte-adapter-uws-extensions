// Windows cannot deliver SIGTERM to a child process, so the runner asks for
// the server's graceful shutdown over IPC instead.
process.on('message', (message) => {
	if (message === 'stop') process.emit('SIGTERM');
});
await import('./build/index.js');
