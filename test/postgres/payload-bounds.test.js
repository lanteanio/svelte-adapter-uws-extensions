import { describe, it, expect } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { createReplay, ReplaySerializationError } from '../../src/postgres/replay.js';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { createJobQueue } from '../../src/postgres/jobs.js';
import { MAX_STORE_PAYLOAD_BYTES } from '../../src/shared/caps.js';

describe('replay payload cap', () => {
	it('refuses data past the cap and accepts data under it', async () => {
		const client = mockPgClient();
		const store = createReplay(client, { cleanupInterval: 0 });
		await expect(store.publish(mockPlatform(), 't', 'e', { blob: 'x'.repeat(300 * 1024) })).rejects.toThrow('maxDataBytes');
		await expect(store.publish(mockPlatform(), 't', 'e', { blob: 'small' })).resolves.toBeDefined();
		store.destroy?.();
	});

	it('rejects a maxDataBytes that would silently disable the cap', () => {
		const client = mockPgClient();
		// NaN is a number and `NaN < 1` is false, so a typeof-only guard
		// accepts it - and then every `bytes > NaN` comparison is false and
		// the configured cap does nothing at all.
		expect(() => createReplay(client, { maxDataBytes: NaN, cleanupInterval: 0 })).toThrow('positive integer');
		expect(() => createReplay(client, { maxDataBytes: 0, cleanupInterval: 0 })).toThrow('positive integer');
		expect(() => createReplay(client, { maxDataBytes: 1.5, cleanupInterval: 0 })).toThrow('positive integer');
	});

	it('honors a configured cap', async () => {
		const client = mockPgClient();
		const store = createReplay(client, { maxDataBytes: 100, cleanupInterval: 0 });
		await expect(store.publish(mockPlatform(), 't', 'e', { blob: 'x'.repeat(500) })).rejects.toThrow('maxDataBytes');
		store.destroy?.();
	});
	it('wraps top-level values with no JSON representation in ReplaySerializationError', async () => {
		const store = createReplay(mockPgClient(), { cleanupInterval: 0 });
		const badValues = [() => {}, Symbol('payload'), { toJSON() { return undefined; } }];

		for (const data of badValues) {
			let caught;
			try { await store.publish(mockPlatform(), 't', 'e', data); } catch (err) { caught = err; }
			expect(caught).toBeInstanceOf(ReplaySerializationError);
			expect(caught.cause).toBeInstanceOf(TypeError);
			expect(caught.message).toContain('no JSON representation');
		}

		let batchError;
		try {
			await store.publishBatch(mockPlatform(), [
				{ topic: 't', event: 'e', data: { toJSON() { return undefined; } } }
			]);
		} catch (err) { batchError = err; }
		expect(batchError).toBeInstanceOf(ReplaySerializationError);
		expect(batchError.cause).toBeInstanceOf(TypeError);
		expect(batchError.message).toContain('no JSON representation');
		store.destroy?.();
	});
});

describe('task payload cap', () => {
	it('refuses oversized input at the caller boundary', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('noop', async () => 'ok');
		// Raised from inside the SQL helpers the same throw arrives wrapped
		// as a transient storage failure and callers retry a payload that
		// can never fit, so it has to fail before any storage interaction.
		await expect(runner.run('noop', { input: { blob: 'x'.repeat(300 * 1024) } })).rejects.toThrow('payload cap');
		await expect(runner.run('noop', { input: { blob: 'small' } })).resolves.toBeDefined();
		runner.destroy();
	});

	it('refuses oversized input on the enqueue path too', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('noop', async () => 'ok');
		await expect(runner.enqueue('noop', { input: { blob: 'x'.repeat(300 * 1024) } })).rejects.toThrow('payload cap');
		runner.destroy();
	});

	it('uses the same bound as the sibling stores', () => {
		expect(MAX_STORE_PAYLOAD_BYTES).toBe(256 * 1024);
	});

	it('rejects a maxPayloadBytes that would silently disable the cap', () => {
		const client = mockPgClient();
		// Same NaN trap parseReplayOptions guards: NaN is a number and
		// `NaN < 1` is false, so a typeof-only guard accepts it and then every
		// `bytes > NaN` comparison is false - removing the bound entirely.
		for (const bad of [NaN, 0, -1, 1.5, '65536', null, Infinity]) {
			expect(() => createTaskRunner(client, { maxPayloadBytes: bad, recoveryInterval: 0, cleanupInterval: 0 }))
				.toThrow('positive integer');
		}
	});

	it('rejects a cap too small to store the terminal error shape', () => {
		const client = mockPgClient();
		expect(() => createTaskRunner(client, { maxPayloadBytes: 79, recoveryInterval: 0, cleanupInterval: 0 }))
			.toThrow('at least 80 bytes');
		expect(() => createTaskRunner(client, { maxPayloadBytes: 80, recoveryInterval: 0, cleanupInterval: 0 }))
			.not.toThrow();
	});

	it('names the option in the refusal, so the lever is findable from the error', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('noop', async () => 'ok');
		await expect(runner.run('noop', { input: { blob: 'x'.repeat(300 * 1024) } }))
			.rejects.toThrow(/maxPayloadBytes/);
		runner.destroy();
	});

	it('honours a LOWERED bound, refusing input the default would accept', async () => {
		// The direction that proves the option is applied rather than merely
		// accepted: 4KB of input sails through the 256KB default.
		const client = mockPgClient();
		const runner = createTaskRunner(client, { maxPayloadBytes: 1024, recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('noop', async () => 'ok');
		await expect(runner.run('noop', { input: { blob: 'x'.repeat(4096) } })).rejects.toThrow('payload cap');
		await expect(runner.run('noop', { input: { blob: 'x'.repeat(64) } })).resolves.toBeDefined();
		runner.destroy();
	});

	it('throws BEFORE any storage interaction, so nothing records a transient failure', async () => {
		// The SQL layer enforces the same cap with the identical message, so
		// asserting the text alone cannot tell the two throws apart. The
		// caller-boundary throw must happen before withBreaker: with the
		// boundary check deleted, the same message arrives THROUGH the breaker
		// and records a storage failure for a payload that can never fit.
		const failures = [];
		const breaker = {
			guard() {},
			success() {},
			failure(err) { failures.push(err); }
		};
		const client = mockPgClient();
		const runner = createTaskRunner(client, { breaker, recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('noop', async () => 'ok');
		await expect(runner.run('noop', { input: { blob: 'x'.repeat(300 * 1024) } })).rejects.toThrow('payload cap');
		expect(failures).toHaveLength(0);
		runner.destroy();
	});
	it('serializes run and enqueue input once and stores the exact validated bytes', async () => {
		const failures = [];
		const breaker = {
			guard() {},
			success() {},
			failure(err) { failures.push(err); }
		};
		const runner = createTaskRunner(mockPgClient(), {
			breaker, recoveryInterval: 0, dispatchInterval: 0, cleanupInterval: 0
		});
		runner.register('noop', async () => 'ok');

		function statefulInput(label) {
			let calls = 0;
			return {
				value: {
					toJSON() {
						calls++;
						return calls === 1 ? { label } : { blob: 'x'.repeat(300 * 1024) };
					}
				},
				calls: () => calls
			};
		}

		const runInput = statefulInput('run');
		await expect(runner.run('noop', { input: runInput.value })).resolves.toBe('ok');
		const queuedInput = statefulInput('enqueue');
		await expect(runner.enqueue('noop', { input: queuedInput.value })).resolves.toEqual(expect.any(String));

		expect(runInput.calls()).toBe(1);
		expect(queuedInput.calls()).toBe(1);
		expect(failures).toHaveLength(0);
		const rows = await runner.list({ limit: 10 });
		expect(rows.map((row) => row.input)).toEqual(expect.arrayContaining([
			{ label: 'run' },
			{ label: 'enqueue' }
		]));
		runner.destroy();
	});
});

describe('oversized task result', () => {
	it('fails the row terminally instead of leaving it running forever', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		let calls = 0;
		runner.register('big', async () => { calls++; return { blob: 'x'.repeat(300 * 1024) }; });

		await expect(runner.run('big', { input: { a: 1 } })).rejects.toThrow('payload cap');

		// The encode happens inside commitRow's parameter list, so an
		// unguarded throw escapes with the row still `running` under a live
		// fence - and the recovery sweep then re-runs the handler, and its
		// side effects, once per fence expiry forever.
		const rows = await runner.list({ name: 'big' });
		expect(rows).toHaveLength(1);
		expect(rows[0].status).toBe('failed');
		expect(calls).toBe(1);
		runner.destroy?.();
	});

	it('does not retry an oversized result, because it is deterministic', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		let calls = 0;
		runner.register('big', async () => {
			calls++;
			return { blob: 'x'.repeat(300 * 1024) };
		}, { retry: { maxAttempts: 5 } });
		await expect(runner.run('big', { input: {} })).rejects.toThrow('payload cap');
		expect(calls).toBe(1);
		runner.destroy?.();
	});

	it('still records a terminal row when the error itself is past the cap', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('boom', async () => {
			// failRow must never refuse on size: refusing there is what leaves
			// the row running and hands the handler back to the sweep.
			throw new Error('y'.repeat(MAX_STORE_PAYLOAD_BYTES + 1024));
		});
		await expect(runner.run('boom', { input: {} })).rejects.toThrow();
		const rows = await runner.list({ name: 'boom' });
		expect(rows[0].status).toBe('failed');

		// The terminal row alone is satisfied by an encoder that simply wrote
		// the oversized error through, so pin the TRUNCATION as well: the
		// stored error is marked, shortened, and actually fits the bound.
		expect(rows[0].error.truncated).toBe(true);
		expect(rows[0].error.name).toBe('Error');
		expect(rows[0].error.message.length).toBeLessThanOrEqual(2048);
		expect(rows[0].error.stack).toBeUndefined();
		expect(Buffer.byteLength(JSON.stringify(rows[0].error))).toBeLessThanOrEqual(MAX_STORE_PAYLOAD_BYTES);
		runner.destroy?.();
	});

	it('keeps even the minimum-size terminal fallback inside the configured cap', async () => {
		const runner = createTaskRunner(mockPgClient(), {
			maxPayloadBytes: 80, recoveryInterval: 0, dispatchInterval: 0, cleanupInterval: 0
		});
		runner.register('boom', async () => { throw new Error('y'.repeat(1000)); });

		await expect(runner.run('boom')).rejects.toThrow();
		const rows = await runner.list({ name: 'boom' });
		expect(rows[0].status).toBe('failed');
		expect(rows[0].error).toMatchObject({ name: 'Error', truncated: true });
		expect(Buffer.byteLength(JSON.stringify(rows[0].error))).toBeLessThanOrEqual(80);
		runner.destroy();
	});

	it('truncates the persisted error at the CONFIGURED bound, not the shared default', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { maxPayloadBytes: 4096, recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('boom', async () => { throw new Error('y'.repeat(8192)); });

		await expect(runner.run('boom', { input: {} })).rejects.toThrow();
		const rows = await runner.list({ name: 'boom' });
		expect(rows[0].status).toBe('failed');
		expect(rows[0].error.truncated).toBe(true);
		// Well under the 256KB default, so a stored error that ignored the
		// option would sail past this.
		expect(Buffer.byteLength(JSON.stringify(rows[0].error))).toBeLessThanOrEqual(4096);
		runner.destroy?.();
	});

	it('commits a result the default would refuse, once the bound is raised', async () => {
		// The workload the default breaks: an export whose ordinary run fits
		// and whose large run does not. The handler has already done the work
		// by the time the cap is met, so the raise has to actually commit.
		const client = mockPgClient();
		const runner = createTaskRunner(client, { maxPayloadBytes: 1024 * 1024, recoveryInterval: 0, cleanupInterval: 0 });
		let calls = 0;
		runner.register('export', async () => { calls++; return { blob: 'x'.repeat(300 * 1024) }; });

		const result = await runner.run('export', { input: { rows: 400 } });
		expect(result.blob.length).toBe(300 * 1024);
		expect(calls).toBe(1);

		const rows = await runner.list({ name: 'export' });
		expect(rows[0].status).toBe('committed');
		expect(rows[0].result.blob.length).toBe(300 * 1024);
		runner.destroy?.();
	});
});

describe('jobs claim batch bound', () => {
	it('refuses a batchSize the id-taking calls could never complete', async () => {
		const jobs = createJobQueue(mockPgClient(), {});
		// Claiming 1200 succeeds while complete/fail/extend all refuse >1000,
		// so the batch can only expire and be redelivered - re-running every
		// job's side effects on each cycle.
		await expect(jobs.claim('q', { batchSize: 1200 })).rejects.toThrow('at most');
		jobs.destroy?.();
	});

	it('still accepts a batch at the shared limit', async () => {
		const jobs = createJobQueue(mockPgClient(), {});
		await expect(jobs.claim('q', { batchSize: 1000 })).resolves.toBeDefined();
		jobs.destroy?.();
	});
});

describe('replay batch payload cap', () => {
	it('applies the same cap the single publish path applies', async () => {
		const client = mockPgClient();
		const store = createReplay(client, { cleanupInterval: 0 });
		const big = { blob: 'x'.repeat(300 * 1024) };
		// A cap the batch path skips is not a cap: the identical payload is
		// refused one at a time and accepted in a batch of two.
		await expect(store.publish(mockPlatform(), 't', 'e', big)).rejects.toThrow('maxDataBytes');
		await expect(store.publishBatch(mockPlatform(), [
			{ topic: 't', event: 'e', data: { ok: 1 } },
			{ topic: 't', event: 'e', data: big }
		])).rejects.toThrow('maxDataBytes');
		store.destroy?.();
	});
});

describe('task error shape', () => {
	it('keeps name/message/stack/code and drops cause by default', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('charge', async () => {
			const err = new Error('charge failed');
			err.code = 'E_CHARGE';
			// Handlers routinely attach config-bearing causes, and the row is
			// re-served to dashboards through await()/list().
			err.cause = { stripeKey: 'sk_live_hunter2' };
			throw err;
		});
		await expect(runner.run('charge', {})).rejects.toThrow('charge failed');
		const rows = await runner.list({ name: 'charge' });
		expect(rows[0].error.message).toBe('charge failed');
		expect(typeof rows[0].error.stack).toBe('string');
		expect(rows[0].error.code).toBe('E_CHARGE');
		expect(rows[0].error.cause).toBeUndefined();
		expect(JSON.stringify(rows[0].error)).not.toContain('sk_live_hunter2');
		runner.destroy();
	});

	it('serializeErrorCause: true restores the old shape', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0, serializeErrorCause: true });
		runner.register('charge', async () => {
			const err = new Error('charge failed');
			err.cause = { detail: 'why' };
			throw err;
		});
		await expect(runner.run('charge', {})).rejects.toThrow('charge failed');
		const rows = await runner.list({ name: 'charge' });
		expect(rows[0].error.cause).toEqual({ detail: 'why' });
		runner.destroy();
	});

	it('applies the cause opt-in to the state-change event, not just the row', async () => {
		const events = [];
		const client = mockPgClient();
		const runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			serializeErrorCause: true,
			onStateChange: (e) => { events.push(e); }
		});
		runner.register('charge', async () => {
			const err = new Error('charge failed');
			err.cause = { detail: 'why' };
			throw err;
		});
		await expect(runner.run('charge', {})).rejects.toThrow('charge failed');
		// Opting in and then getting the cause on the row but not on the event
		// leaves a listener unable to see what the dashboard already shows.
		const failed = events.find((e) => e.newStatus === 'failed');
		expect(failed.error.cause).toEqual({ detail: 'why' });
		runner.destroy();
	});

	it('withholds the cause from the state-change event by default', async () => {
		const events = [];
		const client = mockPgClient();
		const runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			onStateChange: (e) => { events.push(e); }
		});
		runner.register('charge', async () => {
			const err = new Error('charge failed');
			err.cause = { stripeKey: 'sk_live_hunter2' };
			throw err;
		});
		await expect(runner.run('charge', {})).rejects.toThrow('charge failed');
		const failed = events.find((e) => e.newStatus === 'failed');
		expect(failed.error.cause).toBeUndefined();
		expect(JSON.stringify(failed.error)).not.toContain('sk_live_hunter2');
		runner.destroy();
	});
});

describe('unserialisable task error', () => {
	// Size was never the only way the terminal encode can fail. serialiseError
	// copies `code` verbatim - and, opted in, a non-Error `cause` too - so a
	// handler that attaches a BigInt or a circular object makes JSON.stringify
	// throw from inside failRow's parameter list. That throw escapes with the
	// row still `running` under a live fence, and the recovery sweep hands the
	// handler back once per fence expiry, forever.
	const hostile = [
		['a BigInt code', () => { const e = new Error('boom'); e.code = 10n; return e; }, false],
		['a circular code', () => { const e = new Error('boom'); const c = {}; c.me = c; e.code = c; return e; }, false],
		['a circular cause', () => { const e = new Error('boom'); const c = {}; c.me = c; e.cause = c; return e; }, true],
		['a BigInt in the cause', () => { const e = new Error('boom'); e.cause = { v: 1n }; return e; }, true],
		['a throwing toJSON on the cause', () => { const e = new Error('boom'); e.cause = { toJSON() { throw new Error('nope'); } }; return e; }, true]
	];

	for (const [label, make, needsOptIn] of hostile) {
		it(`still reaches a terminal row when the error carries ${label}`, async () => {
			const client = mockPgClient();
			const runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				serializeErrorCause: needsOptIn
			});
			let calls = 0;
			runner.register('boom', async () => { calls++; throw make(); });

			await expect(runner.run('boom', { input: {} })).rejects.toThrow();

			const rows = await runner.list({ name: 'boom' });
			expect(rows).toHaveLength(1);
			expect(rows[0].status).toBe('failed');
			expect(calls).toBe(1);
			runner.destroy?.();
		});
	}

	it('keeps the identifying fields in the stand-in', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('boom', async () => {
			const err = new TypeError('charge failed');
			err.code = 42n;
			throw err;
		});
		await expect(runner.run('boom', { input: {} })).rejects.toThrow('charge failed');
		const rows = await runner.list({ name: 'boom' });
		// A terminal row that says only "could not be serialised" tells the
		// dashboard nothing about which task broke or why.
		expect(rows[0].error.name).toBe('TypeError');
		expect(rows[0].error.message).toBe('charge failed');
		expect(rows[0].error.code).toBe('42');
		expect(rows[0].error.unserialisable).toBe(true);
		runner.destroy?.();
	});

	it('survives an error whose own accessors throw', async () => {
		const events = [];
		const client = mockPgClient();
		const runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			onStateChange: (e) => { events.push(e); }
		});
		runner.register('boom', async () => {
			// A source-map or APM hook installing its own `stack` getter is the
			// ordinary way this shape arrives.
			const err = new Error('the real handler failure');
			Object.defineProperty(err, 'stack', { get() { throw new Error('hostile stack'); } });
			throw err;
		});

		// A bare rejects.toThrow() passes on the GETTER's error, which is the
		// bug: the caller must still receive the handler's failure, not
		// whatever the accessor threw on the way to reporting it.
		await expect(runner.run('boom', { input: {} })).rejects.toThrow('the real handler failure');

		const rows = await runner.list({ name: 'boom' });
		expect(rows[0].status).toBe('failed');
		// Serialising for the event is as able to throw as serialising for the
		// row, and a throw there skips the terminal transition entirely - the
		// row reads `failed` while an event-driven dashboard shows it running
		// forever.
		expect(events.map((e) => e.newStatus)).toContain('failed');
		runner.destroy?.();
	});

	it('keeps the row and the state-change event on the same terminal error', async () => {
		const events = [];
		const client = mockPgClient();
		const runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			onStateChange: (e) => { events.push(e); }
		});
		runner.register('boom', async () => {
			const err = new Error('charge failed');
			err.code = 7n;
			throw err;
		});
		await expect(runner.run('boom', { input: {} })).rejects.toThrow('charge failed');
		const failed = events.find((e) => e.newStatus === 'failed');
		expect(failed).toBeDefined();
		expect(failed.error.message).toBe('charge failed');
		runner.destroy?.();
	});
});

describe('unserialisable task result', () => {
	it('reports what is wrong with the value instead of a bare arity error', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		// JSON.stringify returns undefined for these, and Buffer.byteLength
		// then raises ERR_INVALID_ARG_TYPE - terminally failing the task with
		// a message that names neither the task nor the real problem.
		runner.register('fn', async () => () => {});
		runner.register('sym', async () => Symbol('s'));
		runner.register('big', async () => 1n);

		await expect(runner.run('fn', {})).rejects.toThrow('not JSON-serialisable');
		await expect(runner.run('sym', {})).rejects.toThrow('not JSON-serialisable');
		await expect(runner.run('big', {})).rejects.toThrow('not JSON-serialisable');
		runner.destroy?.();
	});

	it('fails the row terminally rather than leaving it running', async () => {
		const client = mockPgClient();
		const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
		let calls = 0;
		runner.register('circ', async () => { calls++; const o = {}; o.me = o; return o; });
		await expect(runner.run('circ', {})).rejects.toThrow('not JSON-serialisable');
		const rows = await runner.list({ name: 'circ' });
		expect(rows[0].status).toBe('failed');
		expect(calls).toBe(1);
		runner.destroy?.();
	});
});

describe('jobs id validation', () => {
	it('bounds the array length', async () => {
		const jobs = createJobQueue(mockPgClient(), {});
		await expect(jobs.complete(Array.from({ length: 1001 }, (_, i) => i + 1))).rejects.toThrow('at most');
		jobs.destroy?.();
	});

	it('rejects values Postgres cannot parse as bigint', async () => {
		const jobs = createJobQueue(mockPgClient(), {});
		// Each of these coerces to a finite safe integer via Number(), so a
		// Number-based guard passes them through and pg still aborts the
		// whole statement with a raw 22P02.
		for (const bad of ['0x10', '0b101', '1e3', '12.0', true, [5], 'abc', -1, 0, null]) {
			await expect(jobs.complete([1, bad])).rejects.toThrow('positive integers');
		}
		jobs.destroy?.();
	});

	it('rejects a 19-digit id above 2^63-1, which pg aborts with a raw 22003', async () => {
		const jobs = createJobQueue(mockPgClient(), {});
		// Digits-only and non-zero, so the shape guard passes it; only the
		// MAX_BIGINT comparison keeps it out of the statement. 2^63 itself
		// is one past the largest bigint Postgres accepts.
		await expect(jobs.complete(['9223372036854775808'])).rejects.toThrow('positive integers');
		await expect(jobs.complete(['9999999999999999999'])).rejects.toThrow('positive integers');
		// The boundary value itself is legal and must keep working.
		await expect(jobs.complete(['9223372036854775807'])).resolves.toBeUndefined();
		jobs.destroy?.();
	});

	it('accepts a bigserial id beyond 2^53, which pg returns as a string', async () => {
		const jobs = createJobQueue(mockPgClient(), {});
		// Number.isSafeInteger caps at 2^53 while the column runs to 2^63,
		// so a safe-integer guard rejects real ids straight out of pending().
		await expect(jobs.complete(['9007199254740993'])).resolves.toBeUndefined();
		await expect(jobs.complete([1, 2, '3'])).resolves.toBeUndefined();
		jobs.destroy?.();
	});

	it('rejects unsafe numeric ids before precision loss can target another row', async () => {
		const jobs = createJobQueue(mockPgClient(), {});
		const unsafe = Number.MAX_SAFE_INTEGER + 1;
		expect(Number.isSafeInteger(unsafe)).toBe(false);
		await expect(jobs.complete(unsafe)).rejects.toThrow('safe integers');
		await expect(jobs.complete(String(unsafe))).resolves.toBeUndefined();
		await expect(jobs.complete(BigInt(unsafe))).resolves.toBeUndefined();
		jobs.destroy?.();
	});

	it('keeps invalid bigint diagnostics inside the job-id error contract', async () => {
		const jobs = createJobQueue(mockPgClient(), {});
		for (const bad of [0n, -1n, 9223372036854775808n]) {
			await expect(jobs.complete(bad)).rejects.toThrow('positive integers');
		}
		jobs.destroy?.();
	});
});
