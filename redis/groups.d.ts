import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';

export type GroupRole = 'member' | 'admin' | 'viewer';

export interface RedisGroupOptions {
	/** Maximum members allowed. @default Infinity */
	maxMembers?: number;
	/** Initial group metadata. */
	meta?: Record<string, any>;
	/** Called after a member joins. */
	onJoin?: (ws: any, role: GroupRole) => void;
	/** Called after a member leaves. */
	onLeave?: (ws: any, role: GroupRole) => void;
	/** Called when a join is rejected because the group is full. */
	onFull?: (ws: any, role: GroupRole) => void;
	/** Called when the group is closed. */
	onClose?: () => void;
}

export interface RedisGroup {
	/** The group name. */
	readonly name: string;

	/** Get group metadata from Redis. */
	getMeta(): Promise<Record<string, any>>;

	/** Set group metadata in Redis. */
	setMeta(meta: Record<string, any>): Promise<void>;

	/** Add a member. Returns true on success, false if full or closed. */
	join(ws: any, platform: Platform, role?: GroupRole): Promise<boolean>;

	/** Remove a member. */
	leave(ws: any, platform: Platform): Promise<void>;

	/** Broadcast to all members, or filter by role. */
	publish(platform: Platform, event: string, data?: any, role?: GroupRole): Promise<void>;

	/** Send to a single member (validates membership). */
	send(platform: Platform, ws: any, event: string, data?: any): void;

	/** List members on this instance. */
	localMembers(): Array<{ ws: any; role: GroupRole }>;

	/** Total member count across all instances. */
	count(): Promise<number>;

	/** Check if a ws is a member on this instance. */
	has(ws: any): boolean;

	/** Dissolve the group, notify all members, clean up. */
	close(platform: Platform): Promise<void>;

	/** Stop the Redis subscriber. */
	destroy(): void;
}

/**
 * Create a Redis-backed broadcast group.
 */
export function createGroup(client: RedisClient, name: string, options?: RedisGroupOptions): RedisGroup;
