export async function finishPostgres(...resources) {
	const active = resources.filter(Boolean);
	// Stop timers, then drain constructor migrations before another test resets tables.
	for (const resource of active) resource.destroy();
	const results = await Promise.allSettled(active.map((resource) => resource.ready()));
	const failed = results.find((result) => result.status === 'rejected');
	if (failed) throw failed.reason;
}
