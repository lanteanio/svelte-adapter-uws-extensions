import { afterEach, describe, expect, it, vi } from 'vitest';
import pg from 'pg';
import Redis from 'ioredis';
import { inspect } from 'node:util';
import { redactConnectionUrl } from '../../src/shared/sensitive.js';
import { createPgClient } from '../../src/postgres/index.js';
import { createRedisClient } from '../../src/redis/index.js';
import { ReplayStorageError } from '../../src/shared/replay-helpers.js';
import { setOperationalEventSink } from '../../src/shared/diagnostic.js';

// Keep only values the actual installed driver reads as the full password.
function acceptedPasswords(protocol, query) {
	const accepted = [];
	for (const code of [9, 10, 11, 12, 13, ...Array.from({ length: 95 }, (_, index) => index + 32)]) {
		const password = 'credential' + String.fromCharCode(code) + 'suffix';
		const dsn = query
			? `${protocol}://host/app?password=${password}&application_name=check`
			: `${protocol}://user:${password}@host/app`;
		let parsed;
		try {
			if (protocol === 'postgres') parsed = new pg.Client(dsn).connectionParameters.password;
			else {
				const driver = new Redis(dsn, { lazyConnect: true });
				parsed = driver.options.password;
				driver.disconnect();
			}
		} catch { continue; }
		if (parsed === password.replace(/[\t\n\r]/g, '')) accepted.push({ password, dsn });
	}
	return accepted;
}

const corpus = ['postgres', 'redis'].flatMap(protocol =>
	[false, true].flatMap(query => acceptedPasswords(protocol, query)));

afterEach(() => {
	setOperationalEventSink(null);
	vi.restoreAllMocks();
});

describe('connection password delimiters', () => {
	it('removes every printable delimiter accepted by each driver', () => {
		for (const protocol of ['postgres', 'redis']) {
			for (const query of [false, true]) {
				const accepted = acceptedPasswords(protocol, query);
				expect(accepted.length).toBeGreaterThan(70);
				for (const punctuation of ['[', ']', String.fromCharCode(92), '"', '{', '}', '|', '^', '<', '>', '`']) {
					expect(accepted.some(({ password }) => password === 'credential' + punctuation + 'suffix')).toBe(true);
				}
				for (const { dsn } of accepted) {
					expect(redactConnectionUrl(dsn), dsn).toBe(query
						? `${protocol}://host/app?password=***&application_name=check`
						: `${protocol}://user:***@host/app`);
				}
			}
		}
	});

	it('uses decoded parameter names and preserves valid JSON fields and escapes', () => {
		for (const key of ['password', 'pass', 'pwd', '%70assword', 'pass%77ord', 'PASSWORD']) {
			const dsn = `postgres://host/app?${key}=credential[tail&sslmode=require`;
			expect(redactConnectionUrl(dsn)).toBe(`postgres://host/app?${key}=***&sslmode=require`);
		}
		for (const { dsn } of corpus) {
			const input = JSON.stringify({ dsn, user: 'unchanged' }, null, 2);
			const output = redactConnectionUrl(input);
			expect(JSON.parse(output)).toEqual({ dsn: redactConnectionUrl(dsn), user: 'unchanged' });
			expect(output).toContain('\n  "user": "unchanged"\n');
		}
		const dsn = 'postgres://user:credential[tail@host/app';
		for (const slash of [String.fromCharCode(92) + '/', String.fromCharCode(92) + 'u002f']) {
			const input = JSON.stringify({ dsn, ok: true }).replaceAll('/', slash);
			expect(JSON.parse(redactConnectionUrl(input))).toEqual({ dsn: 'postgres://user:***@host/app', ok: true });
		}
	});

	it('keeps an apparent URL inside a query credential in the redacted region', () => {
		for (const suffix of ['redis://suffix', 'postgres://user:extra@host/db']) {
			const dsn = `postgres://host/app?password=credential[${suffix}&x=1`;
			expect(new pg.Client(dsn).connectionParameters.password).toBe('credential[' + suffix);
			expect(redactConnectionUrl(dsn)).toBe('postgres://host/app?password=***&x=1');
		}
		const input = 'postgres://host/app?password=credential[redis://suffix&replica=redis://user:second@host';
		expect(redactConnectionUrl(input)).toBe('postgres://host/app?password=***&replica=redis://user:***@host');
		const adjacent = 'postgres://h/db?password=first,redis://u:credential&suffix@host/db';
		const safe = redactConnectionUrl(adjacent);
		expect(safe).not.toContain('credential');
		expect(safe).not.toContain('suffix');
	});

	it('finds every query password before any earlier credential removes its scheme', () => {
		for (const protocol of ['postgres', 'redis']) {
			for (const separator of [',', ' ', '\n']) {
				const second = `${protocol}://host/app?x=1&password=credential suffix&z=2`;
				const input = 'postgres://host/app?password=first' + separator + second;
				const output = redactConnectionUrl(input);
				expect(output).toContain('password=***');
				expect(output).not.toContain('credential');
				expect(output).not.toContain('suffix');
			}
			for (const inner of ['redis://suffix', 'redis://suffix/path', 'redis://user:earlier@host']) {
				for (const key of ['application_name', 'password']) {
					const dsn = `${protocol}://host/app?${key}=${inner}&password=credential suffix&z=2`;
					const output = redactConnectionUrl(dsn);
					expect(output).not.toContain('credential');
					expect(output).not.toContain('suffix&z');
					if (protocol === 'postgres') expect(new pg.Client(dsn).connectionParameters.password).toBe('credential suffix');
					else {
						const driver = new Redis(dsn, { lazyConnect: true });
						try { expect(driver.options.password).toBe('credential suffix'); }
						finally { driver.disconnect(); }
					}
				}
			}
		}
	});

	it('matches query keys and schemes normalized by the Redis parser', () => {
		for (const control of ['\t', '\n', '\r']) {
			for (let position = 0; position <= 'password'.length; position++) {
				const key = 'password'.slice(0, position) + control + 'password'.slice(position);
				const dsn = `redis://host/app?${key}=credential suffix&x=1`;
				const driver = new Redis(dsn, { lazyConnect: true });
				try {
					expect(driver.options.password).toBe('credential suffix');
					expect(redactConnectionUrl(dsn)).toBe(`redis://host/app?${key}=***&x=1`);
				} finally { driver.disconnect(); }
			}
			const dsn = `postgres:/${control}/user:credential@host/app`;
			expect(new pg.Client(dsn).connectionParameters.password).toBe('credential');
			expect(redactConnectionUrl(dsn)).toBe(`postgres:/${control}/user:***@host/app`);
		}
	});

	it('cleans registered Postgres and Redis listeners before sink projection or console output', async () => {
		const postgres = createPgClient({ connectionString: 'postgres://host/app', autoShutdown: false });
		const redis = createRedisClient({ options: { lazyConnect: true }, autoShutdown: false });
		const records = [];
		const stop = setOperationalEventSink(record => { records.push(record); });
		const error = vi.spyOn(console, 'error').mockImplementation(() => {});
		try {
			for (const { dsn } of corpus) {
				const failure = new Error('connection failed ' + dsn);
				postgres.pool.emit('error', failure);
				redis.redis.emit('error', failure);
			}
			expect(records).toHaveLength(corpus.length * 2);
			for (const record of records) {
				expect(record.attributes.error.message).toContain('***');
				expect(record.attributes.error.message).not.toContain('credential');
				expect(record.attributes.error.message).not.toContain('suffix');
				expect(record.attributes.error.message.length).toBeLessThanOrEqual(512);
			}
			stop();
			for (const { dsn } of corpus) postgres.pool.emit('error', new Error('connection failed ' + dsn));
			expect(error).toHaveBeenCalledTimes(corpus.length);
			expect(inspect(error.mock.calls)).not.toContain('credential');
			expect(inspect(error.mock.calls)).not.toContain('suffix');
		} finally {
			stop();
			await postgres.end();
			await redis.quit();
		}
	});

	it('cleans returned query and checkout errors and replay causes', async () => {
		for (const { dsn } of corpus) {
			const cause = Object.assign(new Error('connection failed ' + dsn), { code: 'ECONNREFUSED' });
			const client = createPgClient({ pool: {
				query: async () => { throw cause; },
				connect: async () => { throw cause; },
				end: async () => {},
				on: () => {}
			} });
			for (const operation of [() => client.query('SELECT 1'), () => client.connect()]) {
				const error = await operation().catch(failure => failure);
				expect(error.code).toBe('ECONNREFUSED');
				expect(error.message).toContain('***');
				expect(inspect(error)).not.toContain('credential');
				expect(inspect(error)).not.toContain('suffix');
			}
			const replay = new ReplayStorageError('read', cause);
			expect(inspect(replay)).not.toContain('credential');
			expect(inspect(replay)).not.toContain('suffix');
		}
	});
});
