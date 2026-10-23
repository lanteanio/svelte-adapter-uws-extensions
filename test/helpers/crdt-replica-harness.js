// One server instance's document layer over the CRDT cluster coordinator:
// the adapter's real document authority, wired to the coordinator exactly as
// svelte-realtime wires `platform.crdt` (inbound handlers that apply and
// answer only for held topics, a relay per client update, a sync request per
// cold join, and a persist hook gated by acquirePersist). Instances share one
// `backend` object standing in for the app's persistence store.

import * as Y from 'yjs';
import { createCrdtAuthority } from 'svelte-adapter-uws/plugins/crdt/replica';
import { createCrdtCluster } from '../../src/redis/crdt.js';

export const DECL = 'topic:doc';

/** The persistence store the instances share: the last snapshot and every write. */
export function createBackend() {
	return { blob: null, writes: [] };
}

/** The `items` map a snapshot or replica state holds, as a plain object. */
export function itemsOf(bytes) {
	const doc = new Y.Doc();
	if (bytes && bytes.length > 0) Y.applyUpdate(doc, bytes instanceof Uint8Array ? bytes : Uint8Array.from(bytes));
	const out = doc.getMap('items').toJSON();
	doc.destroy();
	return out;
}

/**
 * @param {any} client - createRedisClient-shaped client
 * @param {ReturnType<typeof createBackend>} backend
 * @param {object} [options] - createCrdtCluster options
 * @param {{ debounceWait?: number, debounceMaxWait?: number }} [schedule] - the authority's store schedule
 */
export function createDocInstance(client, backend, options = {}, schedule = {}) {
	/** @type {any[]} */
	const subscribers = [];
	const duplicate = client.duplicate;
	// Record the relay subscriber the coordinator opens, so a test can cut it.
	const recording = Object.create(client);
	recording.duplicate = (...args) => {
		const sub = duplicate.apply(client, args);
		subscribers.push(sub);
		return sub;
	};
	const crdt = createCrdtCluster(recording, options);
	const authority = createCrdtAuthority({
		persist: {
			load: () => (backend.blob ? Uint8Array.from(backend.blob) : null),
			store: async (topic, bytes) => {
				const mayWrite = await crdt.acquirePersist(topic);
				if (!mayWrite) return false;
				backend.blob = Uint8Array.from(bytes);
				backend.writes.push({ by: crdt.instanceId, items: itemsOf(bytes) });
			}
		},
		debounceWait: schedule.debounceWait ?? 20,
		debounceMaxWait: schedule.debounceMaxWait ?? 40,
		onError: () => {}
	});
	crdt.onMessage({
		onUpdate: (declKey, topic, bytes) => {
			if (declKey !== DECL || !authority.has(topic)) return;
			authority.applyUpdate(topic, bytes);
		},
		onSyncRequest: (declKey, topic, sv, fromInstance) => {
			if (declKey !== DECL || !authority.has(topic)) return;
			const diff = authority.diff(topic, Array.isArray(sv) ? sv : null);
			if (diff && diff.length > 0) crdt.sendSyncReply(declKey, topic, Array.from(diff), fromInstance);
		},
		onSyncReply: (declKey, topic, bytes) => {
			if (declKey !== DECL || !authority.has(topic)) return;
			authority.applyUpdate(topic, bytes);
		}
	});
	let clientId = 1;
	return {
		crdt,
		authority,
		subscribers,
		/** Cold-join a topic: load the snapshot and ask the peers for anything newer. */
		async join(topic) {
			await authority.acquire(topic);
			const sv = authority.stateVector(topic);
			crdt.requestSync(DECL, topic, sv === null ? [] : Array.from(sv));
		},
		/** One client edit applied here and relayed to the peers. */
		edit(topic, key, value) {
			const doc = new Y.Doc();
			doc.clientID = (Number.parseInt(crdt.instanceId.slice(0, 6), 16) * 1000) + clientId++;
			doc.getMap('items').set(key, value);
			const out = authority.applyUpdate(topic, Y.encodeStateAsUpdate(doc));
			doc.destroy();
			if (out) crdt.relayUpdate(DECL, topic, Array.from(out));
		},
		/** The replica's current `items`. */
		items(topic) {
			return itemsOf(authority.diff(topic, null));
		},
		destroy() {
			authority.destroy();
			crdt.destroy();
		}
	};
}
