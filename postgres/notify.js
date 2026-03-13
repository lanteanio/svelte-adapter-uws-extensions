/**
 * Postgres LISTEN/NOTIFY bridge for svelte-adapter-uws.
 *
 * Listens on a Postgres channel for notifications and forwards them to
 * platform.publish(). The user is responsible for setting up the trigger
 * that calls pg_notify() -- this module only handles the listening side.
 *
 * Uses a dedicated connection (not from the pool) since LISTEN requires
 * a persistent connection that cannot be shared.
 *
 * @module svelte-adapter-uws-extensions/postgres/notify
 */

/**
 * @typedef {Object} NotifyBridgeOptions
 * @property {string} channel - Postgres LISTEN channel name (required)
 * @property {(payload: string, channel: string) => { topic: string, event: string, data?: unknown } | null} [parse] -
 *   Parse the notification payload into a publish call. Return null to skip.
 *   Defaults to JSON.parse expecting { topic, event, data }.
 * @property {boolean} [autoReconnect=true] - Reconnect on connection loss
 * @property {number} [reconnectInterval=3000] - ms between reconnect attempts
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
	const autoReconnect = options.autoReconnect !== false;
	const reconnectInterval = options.reconnectInterval || 3000;
	const parse = options.parse || defaultParse;

	/** @type {import('pg').Client | null} */
	let conn = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	let active = false;
	/** @type {ReturnType<typeof setTimeout> | null} */
	let reconnectTimer = null;

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
		try {
			const result = parse(msg.payload, msg.channel);
			if (result && activePlatform) {
				// relay: false -- in clustered mode each worker has its own
				// LISTEN connection, so relaying would duplicate delivery.
				activePlatform.publish(result.topic, result.event, result.data, { relay: false });
			}
		} catch {
			// Parse errors are non-fatal -- skip the notification
		}
	}

	async function connect() {
		try {
			// Use a standalone Client instead of pool.connect() to avoid
			// permanently holding a pool connection. LISTEN needs a persistent
			// connection that stays open for the lifetime of the bridge --
			// borrowing from the pool would reduce available connections for queries.
			conn = client.createClient();
			conn.on('notification', onNotification);
			conn.on('error', handleError);
			await conn.connect();

			// pg requires channel name to be a valid identifier or quoted
			// Use double-quoting to handle any channel name safely
			await conn.query(`LISTEN "${channel.replace(/"/g, '""')}"`);
		} catch (err) {
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

	function handleError() {
		cleanup();
		if (active && autoReconnect) {
			scheduleReconnect();
		}
	}

	function scheduleReconnect() {
		if (reconnectTimer) return;
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
			if (conn) {
				try {
					await conn.query(`UNLISTEN "${channel.replace(/"/g, '""')}"`);
				} catch {
					// Connection may already be dead
				}
				cleanup();
			}
		}
	};
}
