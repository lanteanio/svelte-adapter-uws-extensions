// Wraps svelte-adapter-bunserve's real platform object with both buses and
// checks that each wrap carries exactly the forwarded members bunserve has.
// Prints a JSON report and exits 1 on any mismatch or failed wrap.
//
// Run by the cross-repo gate's bunserve-wrap rung, from this repository, as
//   node --conditions=bunserve-test test/helpers/bunserve-wrap-probe.mjs <bunserve checkout>
// `bunserve-test` is the export condition bunserve's own unit tests use to load
// its runtime under Node.

import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { FORWARDED, wrapMismatches } from './wrap-surface.js';

// Defined by bunserve's build and read by its handler modules at import.
globalThis.ENV_PREFIX = '';
globalThis.WS_OPTIONS = null;
globalThis.WS_PATH = '/ws';

const { platform } = await import(pathToFileURL(join(process.argv[2], 'src/runtime/handler/platform.js')).href);

/** Every member reachable on `target`, as the kind of value it reads. */
function kinds(target) {
	/** @type {Record<string, string>} */
	const out = {};
	for (let o = target; o && o !== Object.prototype; o = Object.getPrototypeOf(o)) {
		for (const key of Object.getOwnPropertyNames(o)) {
			if (key in out) continue;
			let value;
			try {
				value = target[key];
			} catch {
				out[key] = 'throws';
				continue;
			}
			out[key] = value === null ? 'null' : typeof value;
		}
	}
	return out;
}

const platformKinds = kinds(platform);
// `compared` is how many forwarded members were checked; `present` how many
// of those the platform has.
const report = {
	compared: FORWARDED.length,
	present: FORWARDED.filter((k) => (platformKinds[k] ?? 'undefined') !== 'undefined').length,
	wraps: {}
};
let ok = platformKinds.publish === 'function' && platformKinds.subscribe === 'function';
if (!ok) report.error = 'the loaded module exported no platform with publish and subscribe';
for (const [name, create] of [['pubsub', createPubSubBus], ['sharded', createShardedBus]]) {
	try {
		const mismatches = wrapMismatches(platformKinds, kinds(create(mockRedisClient('probe:')).wrap(platform)));
		report.wraps[name] = { mismatches };
		if (mismatches.length) ok = false;
	} catch (err) {
		report.wraps[name] = { error: String((err && err.stack) || err) };
		ok = false;
	}
}

// bunserve's modules hold timers that keep the process alive; exit once the
// report is flushed.
process.stdout.write(JSON.stringify(report), () => process.exit(ok ? 0 : 1));
