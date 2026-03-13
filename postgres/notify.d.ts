import type { Platform } from 'svelte-adapter-uws';
import type { PgClient } from './index.js';

export interface NotifyBridgeOptions {
	/** Postgres LISTEN channel name. Required. */
	channel: string;

	/**
	 * Parse the notification payload into a publish call.
	 * Return null to skip the notification.
	 * Defaults to JSON.parse expecting `{ topic, event, data }`.
	 */
	parse?: (payload: string, channel: string) => { topic: string; event: string; data?: unknown } | null;

	/** Reconnect on connection loss. @default true */
	autoReconnect?: boolean;

	/** ms between reconnect attempts. @default 3000 */
	reconnectInterval?: number;
}

export interface NotifyBridge {
	/** Start listening. Forwards notifications to platform.publish(). Idempotent. */
	activate(platform: Platform): Promise<void>;

	/** Stop listening and release the connection. */
	deactivate(): Promise<void>;
}

/**
 * Create a Postgres LISTEN/NOTIFY bridge.
 */
export function createNotifyBridge(client: PgClient, options: NotifyBridgeOptions): NotifyBridge;
