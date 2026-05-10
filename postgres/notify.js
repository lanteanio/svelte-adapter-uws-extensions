/**
 * Postgres LISTEN/NOTIFY bridge for svelte-adapter-uws.
 *
 * Listens on a Postgres channel for notifications and forwards them to
 * platform.publish(). The user is responsible for setting up the trigger
 * that calls pg_notify() - this module only handles the listening side.
 *
 * Uses a dedicated connection (not from the pool) since LISTEN requires
 * a persistent connection that cannot be shared.
 *
 * @module svelte-adapter-uws-extensions/postgres/notify
 */

import { createBusValidator } from '../shared/bus-validate.js';

/**
 * @typedef {Object} NotifyBridgeOptions
 * @property {string} channel - Postgres LISTEN channel name (required)
 * @property {(payload: string, channel: string) => { topic: string, event: string, data?: unknown } | null} [parse] -
 *   Parse the notification payload into a publish call. Return null to skip.
 *   Defaults to JSON.parse expecting { topic, event, data }.
 * @property {boolean} [autoReconnect=true] - Reconnect on connection loss
 * @property {number} [reconnectInterval=3000] - ms between reconnect attempts
 * @property {'all' | 'advisory'} [multiListener='all'] -
 *   `'all'`: every instance opens its own LISTEN connection. `'advisory'`:
 *   one instance per cluster wins `pg_try_advisory_lock(lockId)` and holds
 *   the LISTEN connection; others poll for the lock. Requires `lockId` and
 *   that the platform is wrapped by a cross-instance pub/sub bus so the
 *   leader's publishes reach non-leader replicas.
 * @property {number} [lockId] - Advisory lock id. Required when `multiListener: 'advisory'`.
 * @property {number} [pollInterval=5000] - ms between leader-election polls.
 * @property {number} [maxEnvelopeBytes=1048576] - Reject NOTIFY payloads larger than this many bytes (after the parser hands back a topic/event tuple). Defends against an actor with `pg_notify` privilege fanning out a giant envelope. Postgres caps NOTIFY at ~8000 bytes, so the default 1 MB is well above any legitimate use.
 * @property {boolean} [allowSystemTopics=false] - Default false: drop notifications whose parsed `topic` starts with `__`. Defense against a foreign publisher (or hostile DBA) injecting forged `__signal:*` / `__rpc` / plugin-internal frames via `pg_notify`. Apps that legitimately bridge user-defined `__`-prefixed topics (rare) can opt back in via `allowSystemTopics: true`.
 */

/**
 * @typedef {Object} NotifyBridge
 * @property {(platform: import('svelte-adapter-uws').Platform) => Promise<void>} activate -
 *   Start listening. Forwards notifications to platform.publish().
 * @property {() => Promise<void>} deactivate -
 *   Stop listening and release the connection.
 */

/**
 * Create a Postgres LISTEN/NOTIFY bridge.
 *
 * @param {import('./index.js').PgClient} client
 * @param {NotifyBridgeOptions} options
 * @returns {NotifyBridge}
 *
 * @example
 * ```js
 * import { pg } from './pg.js';
 * import { createNotifyBridge } from 'svelte-adapter-uws-extensions/postgres/notify';
 *
 * const bridge = createNotifyBridge(pg, {
 *   channel: 'table_changes',
 *   parse: (payload) => {
 *     const row = JSON.parse(payload);
 *     return { topic: row.table, event: row.op, data: row.data };
 *   }
 * });
 *
 * // In your open hook (once):
 * bridge.activate(platform);
 * ```
 *
 * Then create a trigger on your table:
 * ```sql
 * CREATE OR REPLACE FUNCTION notify_table_change() RETURNS trigger AS $$
 * BEGIN
 *   PERFORM pg_notify('table_changes', json_build_object(
 *     'table', TG_TABLE_NAME,
 *     'op', lower(TG_OP),
 *     'data', CASE TG_OP
 *       WHEN 'DELETE' THEN row_to_json(OLD)
 *       ELSE row_to_json(NEW)
 *     END
 *   )::text);
 *   RETURN COALESCE(NEW, OLD);
 * END;
 * $$ LANGUAGE plpgsql;
 *
 * CREATE TRIGGER messages_notify
 *   AFTER INSERT OR UPDATE OR DELETE ON messages
 *   FOR EACH ROW EXECUTE FUNCTION notify_table_change();
 * ```
 */
export function createNotifyBridge(client, options) {
	if (!options || typeof options.channel !== 'string' || options.channel.length === 0) {
		throw new Error('notify bridge: channel must be a non-empty string');
	}

	const channel = options.channel;
	const quotedChannel = '"' + channel.replace(/"/g, '""') + '"';
	const autoReconnect = options.autoReconnect !== false;
	const validator = createBusValidator({
		maxBytes: options.maxEnvelopeBytes,
		allowSystemTopics: options.allowSystemTopics === true,
		allowedSystemTopics: []
	});
	const reconnectInterval = options.reconnectInterval ?? 3000;
	if (typeof reconnectInterval !== 'number' || !Number.isFinite(reconnectInterval) || reconnectInterval < 0) {
		throw new Error('notify bridge: reconnectInterval must be a non-negative number');
	}
	if (options.parse != null && typeof options.parse !== 'function') {
		throw new Error('notify bridge: parse must be a function');
	}
	const isDefaultParser = !options.parse;
	const parse = options.parse || defaultParse;

	const multiListener = options.multiListener ?? 'all';
	if (multiListener !== 'all' && multiListener !== 'advisory') {
		throw new Error("notify bridge: multiListener must be 'all' or 'advisory'");
	}
	const advisory = multiListener === 'advisory';
	let lockId;
	if (advisory) {
		if (typeof options.lockId !== 'number' || !Number.isInteger(options.lockId)) {
			throw new Error("notify bridge: lockId is required and must be an integer when multiListener is 'advisory'");
		}
		lockId = options.lockId;
	}
	const pollInterval = options.pollInterval ?? 5000;
	if (typeof pollInterval !== 'number' || !Number.isFinite(pollInterval) || pollInterval <= 0) {
		throw new Error('notify bridge: pollInterval must be a positive number');
	}

	const b = options.breaker;
	const m = options.metrics;
	const mReceived = m?.counter('notify_received_total', 'Notifications received', ['channel']);
	const mParseErrors = m?.counter('notify_parse_errors_total', 'Notification parse failures', ['channel']);
	const mReconnects = m?.counter('notify_reconnects_total', 'Connection reconnect attempts');

	/** @type {import('pg').Client | null} */
	let conn = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	let active = false;
	/** @type {ReturnType<typeof setTimeout> | null} */
	let reconnectTimer = null;
	/** @type {ReturnType<typeof setInterval> | null} */
	let pollTimer = null;
	let isLeader = false;

	function defaultParse(payload) {
		try {
			const obj = JSON.parse(payload);
			if (!obj.topic || !obj.event) return null;
			return { topic: obj.topic, event: obj.event, data: obj.data };
		} catch {
			return null;
		}
	}

	function onNotification(msg) {
		if (msg.channel !== channel) return;
		mReceived?.inc({ channel });
		if (msg.payload && msg.payload.length > 7500) {
			console.warn(
				`[postgres/notify] payload on "${channel}" is ${msg.payload.length} bytes - ` +
				'approaching the ~8000 byte Postgres NOTIFY limit'
			);
		}
		// Pre-parse size guard. Postgres caps NOTIFY at ~8000 bytes by
		// default, but the user's parser may unwrap into a larger object
		// graph. We bound the input the parser ever sees as defense
		// against an actor with `pg_notify` privilege injecting a giant
		// envelope across the cluster.
		const rawBytes = msg.payload ? Buffer.byteLength(msg.payload) : 0;
		if (!validator.acceptSize(rawBytes)) {
			mParseErrors?.inc({ channel });
			return;
		}
		try {
			const result = parse(msg.payload, msg.channel);
			if (result && activePlatform) {
				if (!validator.acceptEnvelope(result.topic, result.event)) {
					mParseErrors?.inc({ channel });
					return;
				}
				// In advisory mode, only the leader receives notifications;
				// it must publish *with* relay so the bus fans out to
				// non-leader replicas. In 'all' mode every instance has
				// its own LISTEN, so suppress relay to avoid duplicate
				// fan-out via the bus.
				const publishOpts = advisory ? undefined : { relay: false };
				activePlatform.publish(result.topic, result.event, result.data, publishOpts);
			} else if (!result && isDefaultParser) {
				mParseErrors?.inc({ channel });
			}
		} catch (err) {
			mParseErrors?.inc({ channel });
			if (!isDefaultParser) {
				console.warn(`[postgres/notify] parse error on "${channel}":`, err.message || err);
			}
		}
	}

	async function connect() {
		b?.guard();
		try {
			// Use a standalone Client instead of pool.connect() to avoid
			// permanently holding a pool connection. LISTEN needs a persistent
			// connection that stays open for the lifetime of the bridge --
			// borrowing from the pool would reduce available connections for queries.
			conn = client.createClient();
			conn.on('notification', onNotification);
			conn.on('error', handleError);
			await conn.connect();

			await conn.query(`LISTEN ${quotedChannel}`);
			b?.success();
		} catch (err) {
			b?.failure(err);
			if (conn) {
				try { await conn.end(); } catch { /* ignore */ }
			}
			conn = null;
			if (active && autoReconnect) {
				scheduleReconnect();
			}
			throw err;
		}
	}

	function handleError(err) {
		b?.failure(err);
		cleanup();
		isLeader = false;
		if (advisory) {
			// Polling re-establishes the connection on the next tick.
			return;
		}
		if (active && autoReconnect) {
			scheduleReconnect();
		}
	}

	async function ensureClient() {
		if (conn) return;
		conn = client.createClient();
		conn.on('error', handleError);
		await conn.connect();
	}

	async function advisoryTick() {
		if (!active) return;
		if (!conn) {
			try {
				await ensureClient();
			} catch (err) {
				b?.failure(err);
				return;
			}
		}
		try {
			b?.guard();
			const res = await conn.query('SELECT pg_try_advisory_lock($1) AS acquired', [lockId]);
			const acquired = res.rows[0]?.acquired === true;
			if (acquired && !isLeader) {
				conn.on('notification', onNotification);
				await conn.query(`LISTEN ${quotedChannel}`);
				isLeader = true;
			} else if (!acquired && isLeader) {
				try { await conn.query(`UNLISTEN ${quotedChannel}`); } catch { /* ignore */ }
				conn.removeListener('notification', onNotification);
				isLeader = false;
			}
			b?.success();
		} catch (err) {
			b?.failure(err);
			cleanup();
			isLeader = false;
		}
	}

	function startPolling() {
		if (pollTimer) return;
		pollTimer = setInterval(() => { advisoryTick().catch(() => {}); }, pollInterval);
		if (pollTimer.unref) pollTimer.unref();
	}

	function stopPolling() {
		if (pollTimer) {
			clearInterval(pollTimer);
			pollTimer = null;
		}
	}

	function scheduleReconnect() {
		if (reconnectTimer) return;
		mReconnects?.inc();
		reconnectTimer = setTimeout(async () => {
			reconnectTimer = null;
			if (!active) return;
			try {
				await connect();
			} catch {
				// connect() already schedules another retry on failure
			}
		}, reconnectInterval);
		if (reconnectTimer.unref) reconnectTimer.unref();
	}

	function cleanup() {
		if (conn) {
			conn.removeListener('notification', onNotification);
			conn.removeListener('error', handleError);
			conn.end().catch(() => { /* already closed */ });
			conn = null;
		}
	}

	return {
		async activate(platform) {
			// Always update the platform reference so notifications
			// are forwarded through the latest platform, even if a
			// previous activate() already started the listener.
			activePlatform = platform;
			if (active) return;
			active = true;
			if (advisory) {
				// Initial poll attempt; polling continues regardless of
				// outcome so this never throws - a follower replica is
				// a valid steady state.
				await advisoryTick();
				startPolling();
				return;
			}
			try {
				await connect();
			} catch (err) {
				if (!autoReconnect) {
					// Without autoReconnect, reset so activate() can be retried
					active = false;
					activePlatform = null;
				}
				// With autoReconnect, connect() already scheduled a retry
				throw err;
			}
		},

		async deactivate() {
			if (!active) return;
			active = false;
			activePlatform = null;
			if (reconnectTimer) {
				clearTimeout(reconnectTimer);
				reconnectTimer = null;
			}
			stopPolling();
			if (conn) {
				// 'all' mode always has LISTEN active; advisory mode only
				// when this replica is the leader.
				if (!advisory || isLeader) {
					try {
						await conn.query(`UNLISTEN ${quotedChannel}`);
					} catch {
						// Connection may already be dead
					}
				}
				cleanup();
			}
			isLeader = false;
		}
	};
}
