import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { verifyPostgresVersion } from '../scripts/check-postgres-support.js';

describe('Postgres server preflight', () => {
	it.each(['90525', '90624', '150019'])('refuses unsupported server version %s before initialization', async version => {
		const queries = [];
		const client = { query: async sql => { queries.push(sql); return { rows: [{ server_version_num: version }] }; } };
		await expect(verifyPostgresVersion(client)).rejects.toThrow('PostgreSQL 16 or newer');
		expect(queries).toEqual(['SHOW server_version_num']);
	});
	it.each([undefined, null, 160015, '16.15', '160015 trailing', ''])('fails closed on malformed server identity %j', async version => {
		await expect(verifyPostgresVersion({ query: async () => ({ rows: [{ server_version_num: version }] }) })).rejects.toThrow('missing or malformed');
	});
	it('reads a supported server identity without schema mutation', async () => {
		const queries = [];
		const result = await verifyPostgresVersion({ query: async sql => { queries.push(sql); return { rows: [{ server_version_num: '160015' }] }; } });
		expect(result).toEqual({ serverVersionNum: 160015, major: 16, verifiedMajor: 16 });
		expect(queries).toEqual(['SHOW server_version_num']);
	});
	it('does not represent newer majors as the verified integration baseline', async () => {
		const result = await verifyPostgresVersion({ query: async () => ({ rows: [{ server_version_num: '180006' }] }) });
		expect(result.major).toBe(18);
		expect(result.verifiedMajor).toBe(16);
	});
	it('propagates database refusal instead of assuming compatibility', async () => {
		const refusal = new Error('connection refused');
		await expect(verifyPostgresVersion({ query: async () => { throw refusal; } })).rejects.toBe(refusal);
	});
	it('documents every exported PostgreSQL entry point', () => {
		const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
		const doc = readFileSync(new URL('../docs/postgres-support.md', import.meta.url), 'utf8');
		for (const entry of Object.keys(pkg.exports).filter(key => key === './postgres' || key.startsWith('./postgres/'))) {
			expect(doc).toContain('| `' + entry.slice(2) + '` | 16 |');
		}
	});
});
