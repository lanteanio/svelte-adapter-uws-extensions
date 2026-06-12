// Backend-clock waits for the TTL / expiry-window integration tests.
//
// Those tests write expiry stamps on the BACKEND's clock (Redis EX/PX/TIME,
// Postgres now()) but a plain `setTimeout` wait elapses on the HOST clock.
// The two clocks are not the same machine: the Docker Desktop backends run
// inside a VM whose clock stalls in jumps under sustained CPU load (observed
// multiple seconds behind the host, re-drifting within minutes of a resync),
// so N ms of host wait can cover far less backend time and a short TTL window
// is missed. These helpers wait until the backend's OWN clock reports the
// requested duration elapsed, which is drift-immune by construction: the wait
// and the expiry deadline read the same clock.
//
// Both helpers poll the backend every 50ms of host time and throw once a
// host-side safety bound passes without the backend clock getting there -
// that bound exists to fail loudly on a dead backend, not to time the wait,
// so it is floored generously enough to ride out a multi-second VM clock
// stall on a short window.
import { isCluster } from '../../../shared/cluster.js';

const POLL_MS = 50;
const MIN_TIMEOUT_MS = 10_000;

function hostWait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

/**
 * Current Redis server time in (fractional) milliseconds. TIME replies as
 * [seconds, microseconds] string pairs on ioredis. Exported so a test can
 * measure an ELAPSED duration on the same clock a PX/EX deadline lives on
 * (a host-clock stopwatch around a backend deadline carries the drift).
 *
 * Accepts the suites' RedisClient wrapper or a raw connection; on a Cluster
 * the sample is pinned to the first master so consecutive readings measure
 * one clock, never a mix of nodes.
 *
 * @param {import('../../../redis/index.js').RedisClient | import('ioredis').Redis} client
 * @returns {Promise<number>}
 */
export async function redisNowMs(client) {
	const conn = client && client.redis ? client.redis : client;
	const redis = isCluster(conn) ? /** @type {any} */ (conn).nodes('master')[0] : conn;
	const [sec, usec] = await redis.time();
	return Number(sec) * 1000 + Number(usec) / 1000;
}

/**
 * Wait until the Redis server's own clock (the TIME command) reports that at
 * least `ms` milliseconds have elapsed. Use instead of a host-side sleep
 * whenever the deadline being waited out lives in Redis (SET EX/PX expiry,
 * HPEXPIRE field TTLs, TIME-stamped windows inside Lua scripts).
 *
 * On a Cluster client the samples are pinned to a single master node so the
 * elapsed time is measured against one clock, not a mix of nodes.
 *
 * @param {import('../../../redis/index.js').RedisClient | import('ioredis').Redis} client
 *   The suites' RedisClient wrapper ({ redis, key, ... }) or a raw ioredis
 *   connection - both shapes are accepted.
 * @param {number} ms - Backend-clock milliseconds to let elapse.
 * @param {number} [timeoutMs] - Host-side safety bound; defaults to
 *   max(ms * 10, 10s) and only guards against a dead backend.
 * @returns {Promise<void>}
 */
export async function waitRedisMs(client, ms, timeoutMs = Math.max(ms * 10, MIN_TIMEOUT_MS)) {
	const start = await redisNowMs(client);
	const hostDeadline = Date.now() + timeoutMs;
	for (;;) {
		const elapsed = (await redisNowMs(client)) - start;
		if (elapsed >= ms) return;
		if (Date.now() >= hostDeadline) {
			throw new Error(
				`waitRedisMs: Redis clock advanced only ${Math.round(elapsed)}ms of the ` +
				`requested ${ms}ms within the ${timeoutMs}ms host-side bound`
			);
		}
		await hostWait(POLL_MS);
	}
}

/**
 * Wait until the Postgres server's own clock (now()) reports that at least
 * `ms` milliseconds have elapsed. Use instead of a host-side sleep whenever
 * the deadline being waited out lives in Postgres (expires_at / claimed_until
 * columns compared against now(), interval arithmetic, created_at ordering).
 *
 * now() is the statement-start timestamp, so each poll - its own implicit
 * transaction - samples a fresh value, and it is the exact clock the shipped
 * SQL compares its deadlines against.
 *
 * @param {{ query: (text: string) => Promise<{ rows: any[] }> }} pgClient
 *   The suites' PgClient wrapper from createPgClient (anything with a
 *   node-postgres-shaped query()).
 * @param {number} ms - Backend-clock milliseconds to let elapse.
 * @param {number} [timeoutMs] - Host-side safety bound; defaults to
 *   max(ms * 10, 10s) and only guards against a dead backend.
 * @returns {Promise<void>}
 */
export async function waitPgMs(pgClient, ms, timeoutMs = Math.max(ms * 10, MIN_TIMEOUT_MS)) {
	async function pgNowMs() {
		const res = await pgClient.query('SELECT extract(epoch from now()) * 1000 AS ms');
		return Number(res.rows[0].ms);
	}
	const start = await pgNowMs();
	const hostDeadline = Date.now() + timeoutMs;
	for (;;) {
		const elapsed = (await pgNowMs()) - start;
		if (elapsed >= ms) return;
		if (Date.now() >= hostDeadline) {
			throw new Error(
				`waitPgMs: Postgres clock advanced only ${Math.round(elapsed)}ms of the ` +
				`requested ${ms}ms within the ${timeoutMs}ms host-side bound`
			);
		}
		await hostWait(POLL_MS);
	}
}
