import { describe, it, expect } from 'vitest';
import { inspect } from 'node:util';
import { runInNewContext } from 'node:vm';
import { createPgClient } from '../../src/postgres/index.js';
import { ReplayStorageError } from '../../src/shared/replay-helpers.js';

const DSN = 'postgres://svc:hunter2@db.internal:5432/app';

/** A pool stub whose failures carry the DSN, the way pg's do. */
function failingPool(makeError) {
	return {
		query: async () => { throw makeError(); },
		connect: async () => { throw makeError(); },
		end: async () => {},
		on: () => {}
	};
}

describe('postgres client error redaction', () => {
	it('redacts the DSN out of a query failure', async () => {
		const client = createPgClient({ pool: failingPool(() => new Error(`password authentication failed for "${DSN}"`)) });
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(err.message).not.toContain('hunter2');
		expect(err.message).toContain('***');
	});

	it('redacts the stack too', async () => {
		const client = createPgClient({
			pool: failingPool(() => {
				const e = new Error(`connection to ${DSN} refused`);
				// Materialize the stack first: assigning .message afterwards
				// does NOT rewrite an already-realized stack, so any logger or
				// pg internal that touched it pins the DSN in place.
				void e.stack;
				return e;
			})
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(err.message).not.toContain('hunter2');
		expect(String(err.stack)).not.toContain('hunter2');
	});

	it('redacts connection acquisition, the path that carries the DSN most', async () => {
		const client = createPgClient({ pool: failingPool(() => new Error(`could not connect to ${DSN}`)) });
		const err = await client.connect().catch((e) => e);
		expect(err.message).not.toContain('hunter2');
	});

	it('preserves the pg diagnostic fields callers switch on', async () => {
		const client = createPgClient({
			pool: failingPool(() => {
				const e = new Error('duplicate key value violates unique constraint');
				e.code = '23505';
				e.constraint = 'svti_tasks_pkey';
				e.severity = 'ERROR';
				return e;
			})
		});
		const err = await client.query('INSERT ...').catch((e) => e);
		expect(err.code).toBe('23505');
		expect(err.constraint).toBe('svti_tasks_pkey');
		expect(err.severity).toBe('ERROR');
	});

	it('does not mutate a frozen error supplied by an external pool', async () => {
		const original = Object.freeze(Object.assign(new Error(`auth failed for ${DSN}`), {
			code: '28P01',
			severity: 'FATAL'
		}));
		const client = createPgClient({ pool: failingPool(() => original) });
		const err = await client.query('SELECT 1').catch((e) => e);

		// `expect(err.message).not.toContain('hunter2')` alone is decorative:
		// it passes on the TypeError this path used to throw ('Cannot redefine
		// property: message'), which is the very failure the test is named
		// for. Assert what survived, not only what did not leak.
		expect(err).toBeInstanceOf(Error);
		expect(err.constructor.name).not.toBe('TypeError');
		expect(err.message).toContain('auth failed');
		expect(err.message).not.toContain('hunter2');
		// Every diagnostic a caller retries on has to come through intact.
		expect(err.code).toBe('28P01');
		expect(err.severity).toBe('FATAL');
		// The frozen original is untouched.
		expect(Object.isFrozen(original)).toBe(true);
		expect(original.message).toContain('hunter2');
	});

	it('keeps message and stack non-enumerable so JSON.stringify cannot ship a stack trace', async () => {
		const client = createPgClient({
			pool: failingPool(() => new Error(`auth failed for ${DSN}`))
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		// A redactor that assigns instead of defining turns message/stack into
		// own enumerable props, and an app serializing the error into a
		// response body starts shipping the server stack trace with it.
		const serialized = JSON.stringify(err);
		expect(serialized).not.toContain('at ');
		expect(Object.keys(err)).not.toContain('stack');
		expect(Object.keys(err)).not.toContain('message');
	});

	it('redacts a repeated error reference instead of handing back the raw one', async () => {
		// pg-pool wraps the original inside a timeout error, so the same Error
		// object is reachable twice. A visited-SET returns the raw object on
		// the second visit, putting the unredacted DSN right back into
		// util.inspect / console.error output.
		const inner = new Error(`inner ${DSN}`);
		const outer = new Error(`outer timeout`);
		outer.cause = inner;
		outer.original = inner;
		const client = createPgClient({ pool: failingPool(() => outer) });
		const err = await client.query('SELECT 1').catch((e) => e);

		expect(err.cause.message).not.toContain('hunter2');
		expect(err.original.message).not.toContain('hunter2');
		expect(inspect(err, { depth: 10 })).not.toContain('hunter2');
	});

	it('redacts a cyclic cause chain without leaking or spinning', async () => {
		const outer = new Error(`outer ${DSN}`);
		const inner = new Error(`inner ${DSN}`);
		outer.cause = inner;
		inner.cause = outer;
		const client = createPgClient({ pool: failingPool(() => outer) });
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(inspect(err, { depth: 10 })).not.toContain('hunter2');
	});

	it('redacts a DSN nested inside a plain container on the error', async () => {
		const err0 = new Error('connection failed');
		err0.details = { attempted: [`primary ${DSN}`], note: `fallback ${DSN}` };
		const client = createPgClient({ pool: failingPool(() => err0) });
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(JSON.stringify(err.details)).not.toContain('hunter2');
	});

	it('survives a non-object rejection', async () => {
		const client = createPgClient({ pool: { query: async () => { throw 'plain string'; }, end: async () => {}, on: () => {} } });
		await expect(client.query('SELECT 1')).rejects.toBe('plain string');
	});
});

describe('ReplayStorageError', () => {
	it('redacts the cause text it embeds', () => {
		const err = new ReplayStorageError('publish', new Error(`connect ECONNREFUSED ${DSN}`));
		expect(err.message).not.toContain('hunter2');
		expect(err.message).toContain('***');
	});
});

describe('nested and structural redaction', () => {
	it('redacts a DSN carried on err.cause', async () => {
		// pg-pool wraps the original in a connection-timeout error, and
		// util.inspect prints `cause` - which is what console.error(err) and
		// every structured logger's error serializer do.
		const client = createPgClient({
			pool: failingPool(() => new Error('Connection terminated due to connection timeout', {
				cause: new Error(`password authentication failed for ${DSN}`)
			}))
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		const util = await import('node:util');
		expect(util.inspect(err)).not.toContain('hunter2');
		expect(err.cause.message).not.toContain('hunter2');
	});

	it('redacts nested errors in an AggregateError', async () => {
		const client = createPgClient({
			pool: failingPool(() => {
				const e = new Error('all attempts failed');
				e.errors = [new Error(`connect failed for ${DSN}`)];
				return e;
			})
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		const util = await import('node:util');
		expect(util.inspect(err)).not.toContain('hunter2');
	});

	it('does not turn message and stack into enumerable own properties', async () => {
		// Assigning them would make JSON.stringify(err) ship the whole server
		// stack trace, so an app serializing an error into a response would
		// start leaking it - an information disclosure created by a redactor.
		const client = createPgClient({
			pool: failingPool(() => {
				const e = new Error(`boom for ${DSN}`);
				Object.assign(e, { code: '08006' });
				return e;
			})
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(Object.keys(err)).not.toContain('stack');
		expect(Object.keys(err)).not.toContain('message');
		expect(JSON.stringify(err)).not.toContain('at ');
		expect(err.code).toBe('08006');
	});

	it('survives a property whose getter throws', async () => {
		const client = createPgClient({
			pool: failingPool(() => {
				const e = new Error(`auth failed for ${DSN}`);
				Object.defineProperty(e, 'detail', { get() { throw new Error('BOOM from getter'); }, enumerable: true, configurable: true });
				return e;
			})
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		// The caller must get the redacted DB error, not the getter's throw.
		expect(err.message).toContain('auth failed');
		expect(err.message).not.toContain('hunter2');
	});

	it('redacts a DSN behind an accessor-valued own property', async () => {
		const client = createPgClient({
			pool: failingPool(() => {
				const e = new Error('auth failed');
				// Copying this descriptor verbatim installs the ORIGINAL getter
				// on the redacted copy, so reading it re-invokes the getter and
				// returns the raw DSN the walk just took care to hide.
				Object.defineProperty(e, 'detail', {
					get: () => `connecting to ${DSN}`,
					enumerable: true,
					configurable: true
				});
				return e;
			})
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(err.detail).not.toContain('hunter2');
		expect(err.detail).toContain('***');
		expect(inspect(err)).not.toContain('hunter2');
	});

	it('does not emit a raw subtree once the depth cap is reached', async () => {
		const client = createPgClient({
			pool: failingPool(() => {
				const e = new Error('auth failed');
				// One level past REDACT_MAX_DEPTH. Handing the subtree back
				// raw is the redactor itself publishing the DSN.
				e.ctx = { a: { b: { c: { d: { e: { f: { dsn: DSN } } } } } } };
				return e;
			})
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(JSON.stringify(err.ctx)).not.toContain('hunter2');
		expect(inspect(err, { depth: null })).not.toContain('hunter2');
	});

	it('does not re-install a subtree whose walk threw', async () => {
		const client = createPgClient({
			pool: failingPool(() => {
				const e = new Error('auth failed');
				// A proxy trap that throws aborts the walk part-way. Keeping the
				// original descriptor then puts the RAW object - DSN and all -
				// onto the copy the walk exists to sanitize.
				e.payload = new Proxy({ dsn: DSN }, { getPrototypeOf() { throw new Error('trap'); } });
				return e;
			})
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(String(err.payload?.dsn)).not.toContain('hunter2');
		expect(inspect(err, { depth: null })).not.toContain('hunter2');
	});

	it('redacts a plain object from another realm', async () => {
		const foreign = runInNewContext(`({ dsn: ${JSON.stringify(DSN)} })`);
		const client = createPgClient({
			pool: failingPool(() => {
				const e = new Error('auth failed');
				// Its prototype is the OTHER realm's Object.prototype, so an
				// identity check against ours calls it a class instance and
				// passes it through untouched.
				e.ctx = foreign;
				return e;
			})
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(JSON.stringify(err.ctx)).not.toContain('hunter2');
	});

	it('survives a throwing message or stack getter without destroying the error', async () => {
		for (const key of ['message', 'stack']) {
			const client = createPgClient({
				pool: failingPool(() => {
					const e = new Error(`auth failed for ${DSN}`);
					e.code = '40001';
					// `stack` is an accessor on V8 errors, so a source-map or APM
					// hook installing a throwing one is ordinary. An unguarded
					// read makes the REDACTOR throw, and the caller then gets
					// that error instead of the DatabaseError - losing the `code`
					// a serialization-failure retry reads.
					Object.defineProperty(e, key, { get() { throw new Error(`${key} getter exploded`); } });
					return e;
				})
			});
			const err = await client.query('SELECT 1').catch((e) => e);
			expect(err.code).toBe('40001');
		}
	});

	it('keeps an enumerable message on a plain-object rejection', async () => {
		const client = createPgClient({
			pool: failingPool(() => ({ code: 'ECONNREFUSED', message: `cannot reach ${DSN}`, detail: 'x' }))
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		const json = JSON.stringify(err);
		// Forcing message non-enumerable is right for a real Error and wrong
		// here: a structured logger would drop the message entirely.
		expect(json).toContain('cannot reach');
		expect(json).not.toContain('hunter2');
	});

	it('still hides stack from JSON for a real Error', async () => {
		const client = createPgClient({ pool: failingPool(() => new Error(`auth failed for ${DSN}`)) });
		const err = await client.query('SELECT 1').catch((e) => e);
		// The other half of the rule above: an app serializing an error into a
		// response body must not start shipping the server stack trace.
		expect(JSON.stringify(err)).not.toContain('at ');
	});

	it('survives an error that refuses to be inspected', async () => {
		// A proxy trap that throws, or a revoked proxy, makes the REDACTOR
		// throw - and the caller then receives that instead of its own error.
		const cases = {
			'getPrototypeOf trap': () => new Proxy(new Error('orig'), { getPrototypeOf() { throw new Error('gpo'); } }),
			'ownKeys trap': () => new Proxy(new Error('orig'), { ownKeys() { throw new Error('ownKeys'); } }),
			'revoked proxy': () => { const r = Proxy.revocable(new Error('orig'), {}); r.revoke(); return r.proxy; }
		};
		for (const [label, mk] of Object.entries(cases)) {
			const client = createPgClient({ pool: failingPool(mk) });
			const err = await client.query('SELECT 1').catch((e) => e);
			expect(err, label).toBeInstanceOf(Error);
			expect(err.message, label).not.toMatch(/gpo|ownKeys|revoked/);
		}
	});

	it('redacts an Error that crossed a realm boundary', async () => {
		// `instanceof Error` is realm-bound, so a vm-context error fails it,
		// falls through to the plain-object test and is returned RAW - with
		// its message, the field most likely to carry the DSN.
		const foreign = runInNewContext(`new Error(${JSON.stringify('connect failed ' + DSN)})`);
		const client = createPgClient({
			pool: failingPool(() => { const e = new Error('outer'); e.inner = foreign; return e; })
		});
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(String(err.inner?.message)).not.toContain('hunter2');
		expect(String(err.inner?.message)).toContain('***');
	});

	it('releases the connection when the client cannot be wrapped', async () => {
		const released = [];
		const frozen = Object.freeze({ query: () => {}, release: (e) => released.push(e) });
		const pg = createPgClient({ pool: { connect: async () => frozen, query: async () => ({ rows: [] }), end: async () => {}, on: () => {} } });
		await expect(pg.connect()).rejects.toBeDefined();
		// Leaking the checkout would exhaust the pool (pg default max 10).
		expect(released).toHaveLength(1);
	});

	it('wraps a SEALED client, which accepts the swap but refuses the marker', async () => {
		// Sealed is not frozen: existing properties stay writable, so `query`
		// can be replaced and redaction works. Only the wrapped-marker needs
		// a new property. Failing the whole connect() over that bookkeeping
		// flag took down every checkout on such a pool.
		const sealed = Object.seal({
			query: async () => { throw new Error(`auth failed for ${DSN}`); },
			release: () => {}
		});
		const pg = createPgClient({ pool: { connect: async () => sealed, query: async () => ({ rows: [] }), end: async () => {}, on: () => {} } });
		const client = await pg.connect();
		const err = await client.query('SELECT 1').catch((e) => e);
		expect(err.message).toContain('auth failed');
		expect(err.message).not.toContain('hunter2');
	});

	it('does not stack a second wrapper on a sealed client', async () => {
		const sealed = Object.seal({ query: async () => ({ rows: [] }), release: () => {} });
		const pg = createPgClient({ pool: { connect: async () => sealed, query: async () => ({ rows: [] }), end: async () => {}, on: () => {} } });
		const first = await pg.connect();
		const wrappedOnce = first.query;
		const second = await pg.connect();
		// A marker that never sticks re-wraps on every checkout, and the
		// wrapper chain grows for the life of the pool.
		expect(second.query).toBe(wrappedOnce);
	});
});
