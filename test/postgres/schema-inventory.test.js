import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { postgresMigrations } from '../../src/postgres/migrations.js';
import { schemaFor } from '../../src/postgres/_schema.js';

const manifest = JSON.parse(readFileSync(new URL('../../docs/postgres-schema.v1.json', import.meta.url), 'utf8'));
const stores = ['replay', 'idempotency', 'jobs', 'tasks', 'dead-letter', 'alarm-store'];

describe('PostgreSQL schema inventory', () => {
	it('derives every store and version-store list from the migration declarations', () => {
		expect(manifest.stores.map((entry) => entry.subpath)).toEqual(stores.map((store) => `postgres/${store}`));
		const allVersions = new Set();
		const versionStores = new Map();
		for (const store of stores) {
			for (const { version } of postgresMigrations({ store })) {
				allVersions.add(version);
				if (version < 3) continue;
				const subpath = `postgres/${store}`;
				versionStores.set(version, [...(versionStores.get(version) ?? []), subpath]);
			}
		}
		const laterVersions = [...allVersions].filter((version) => version >= 3).sort((a, b) => a - b);
		expect(manifest.ledger.versions).toEqual([...allVersions].sort((a, b) => a - b));
		expect(Object.keys(manifest.ledger).filter((key) => /^version\d+Stores$/.test(key))).toEqual(laterVersions.map((version) => `version${version}Stores`));
		for (const version of laterVersions) {
			expect(manifest.ledger[`version${version}Stores`]).toEqual(versionStores.get(version));
		}
	});

	it('lists every declared store relation, including companions', () => {
		for (const store of stores) {
			const entry = manifest.stores.find((candidate) => candidate.subpath === `postgres/${store}`);
			expect(schemaFor({ store }).tables.map((relation) => relation.name), entry.subpath).toEqual(entry.tables);
		}
	});
});
