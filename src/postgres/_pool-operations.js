import { ConnectionError } from '../shared/errors.js';
import { monotonicNow } from '../shared/runtime.js';

// Registry facades may return the same gauges. Each gauge owns its collector;
// scrape each pool once without retaining otherwise unreachable pools.
const poolCollectors = new WeakMap();
const durationBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];

export function createPoolOperations(pool, maxInFlight, metrics, redactError) {
	let inFlight = 0;
	const active = metrics?.gauge('postgres_operations_in_flight', 'Unsettled PostgreSQL wrapper operations', ['operation']);
	const duration = metrics?.histogram('postgres_operation_duration_seconds', 'PostgreSQL wrapper operation duration in seconds', ['operation', 'result'], durationBuckets);
	const timeouts = metrics?.counter('postgres_operation_timeouts_total', 'PostgreSQL driver timeout failures', ['operation']);
	const rejected = metrics?.counter('postgres_admission_rejected_total', 'PostgreSQL operations rejected before admission or cancelled by the caller', ['operation', 'reason']);
	if (metrics) registerPoolMetrics(metrics, pool);

	function capacityError(operation) {
		rejected?.inc({ operation, reason: 'capacity' });
		const error = new ConnectionError('postgres', 'maximum outstanding pool operations reached');
		error.code = 'PG_CAPACITY_EXCEEDED';
		return error;
	}
	// Shared settlement callbacks keep the normal bounded path allocation-free
	// apart from the promise reaction already needed for error redaction.
	function settled(value) { inFlight--; return value; }
	function failed(error) { inFlight--; throw redactError(error); }
	function rethrow(error) { throw redactError(error); }

	function run(operation, text, values, signal) {
		if (signal !== undefined && (!signal || typeof signal.addEventListener !== 'function' || typeof signal.removeEventListener !== 'function' || typeof signal.aborted !== 'boolean')) {
			return Promise.reject(new TypeError('signal must be an AbortSignal'));
		}
		if (signal?.aborted) {
			rejected?.inc({ operation, reason: 'aborted' });
			return Promise.reject(signal.reason === undefined ? new DOMException('PostgreSQL operation aborted', 'AbortError') : signal.reason);
		}
		if (maxInFlight && inFlight >= maxInFlight) return Promise.reject(capacityError(operation));
		inFlight++;
		if (!signal && !metrics) {
			try {
				return (operation === 'query' ? pool.query(text, values) : pool.connect()).then(settled, failed);
			} catch (error) {
				inFlight--;
				return Promise.reject(redactError(error));
			}
		}
		const started = duration ? monotonicNow() : 0;
		active?.inc({ operation });
		let abandoned = false;
		let finished = false;
		let onAbort;
		function finish(error, rejectedOperation = false) {
			finished = true;
			if (onAbort) { try { signal.removeEventListener('abort', onAbort); } catch { /* foreign signal cleanup */ } }
			inFlight--;
			active?.dec({ operation });
			const timeout = isTimeout(error);
			if (timeout) timeouts?.inc({ operation });
			duration?.observe({ operation, result: timeout ? 'timeout' : rejectedOperation ? 'error' : 'ok' }, Math.max(0, monotonicNow() - started) / 1000);
		}
		return new Promise((resolve, reject) => {
			onAbort = signal ? () => {
				if (finished || abandoned) return;
				abandoned = true;
				try { signal.removeEventListener('abort', onAbort); } catch { /* foreign signal cleanup */ }
				rejected?.inc({ operation, reason: 'aborted' });
				reject(signal.reason === undefined ? new DOMException('PostgreSQL operation aborted', 'AbortError') : signal.reason);
			} : undefined;
			try { if (onAbort) signal.addEventListener('abort', onAbort, { once: true }); }
			catch (error) { finish(error, true); reject(redactError(error)); return; }
			let pending;
			try { pending = operation === 'query' ? pool.query(text, values) : pool.connect(); }
			catch (error) { finish(error, true); reject(redactError(error)); return; }
			Promise.resolve(pending).then((value) => {
				finish();
				if (abandoned && operation === 'connect') {
					// The caller never received this checkout. Return it once, only
					// after pg actually hands it to us; abort never frees capacity early.
					try { value.release(); } catch { /* caller-owned release failed */ }
				} else if (!abandoned) resolve(value);
			}, (error) => {
				finish(error, true);
				if (!abandoned) reject(redactError(error));
			});
		});
	}

	return {
		query(text, values, options) {
			if (!metrics && options?.signal === undefined) {
				if (!maxInFlight) return pool.query(text, values).catch(rethrow);
				if (inFlight >= maxInFlight) return Promise.reject(capacityError('query'));
				inFlight++;
				try { return pool.query(text, values).then(settled, failed); }
				catch (error) { inFlight--; return Promise.reject(redactError(error)); }
			}
			return run('query', text, values, options?.signal);
		},
		connect(options) {
			if (!maxInFlight && !metrics && options?.signal === undefined) return pool.connect().catch(rethrow);
			return run('connect', undefined, undefined, options?.signal);
		}
	};
}

function isTimeout(error) {
	try {
		return error?.message === 'Query read timeout'
			|| error?.message === 'timeout exceeded when trying to connect'
			|| error?.message === 'Connection terminated due to connection timeout'
			|| (error?.code === '57014' && /statement timeout/.test(error.message));
	} catch { return false; }
}

// Keep the scrape closure outside the operation factory: its lexical context
// must never contain that factory's strong pool reference.
function registerPoolMetrics(metrics, pool) {
	registerPoolGauge(metrics.gauge('postgres_pool_waiters', 'PostgreSQL driver pool checkout waiters'), pool, 'waiters');
	registerPoolGauge(metrics.gauge('postgres_pool_connections', 'PostgreSQL driver connections by state', ['state']), pool, 'connections');
	registerPoolGauge(metrics.gauge('postgres_pool_capacity', 'Configured PostgreSQL pool connection capacity'), pool, 'capacity');
}

function registerPoolGauge(gauge, pool, kind) {
	let registration = poolCollectors.get(gauge);
	if (!registration) {
		registration = createPoolCollector(gauge, kind);
		poolCollectors.set(gauge, registration);
	}
	const { pools, finalizer } = registration;
	let registered = false;
	for (const ref of pools) {
		const current = ref.deref();
		if (!current || current.ended) { pools.delete(ref); finalizer.unregister(ref); }
		else if (current === pool) registered = true;
	}
	if (!registered && !pool.ended) {
		const ref = new WeakRef(pool);
		pools.add(ref);
		finalizer.register(pool, ref, ref);
	}
}

function createPoolCollector(gauge, kind) {
	const pools = new Set();
	// Owned by this gauge's registration, so discarded registries do not leave
	// finalizer records in a global collector for a still-live shared pool.
	const finalizer = new FinalizationRegistry(ref => pools.delete(ref));
	gauge.collect(() => {
		let count = 0, idle = 0;
		for (const ref of pools) {
			const pool = ref.deref();
			if (!pool || pool.ended) { pools.delete(ref); finalizer.unregister(ref); continue; }
			if (kind === 'waiters') {
				if (Number.isFinite(pool.waitingCount)) count += pool.waitingCount;
			} else if (kind === 'capacity') {
				if (Number.isFinite(pool.options?.max)) count += Math.max(0, pool.options.max);
			} else if (Number.isFinite(pool.totalCount) && Number.isFinite(pool.idleCount)) {
				idle += Math.max(0, pool.idleCount);
				count += Math.max(0, pool.totalCount - pool.idleCount);
			}
		}
		if (kind === 'connections') {
			gauge.set({ state: 'idle' }, idle);
			gauge.set({ state: 'active' }, count);
		} else gauge.set(count);
	});
	return { pools, finalizer };
}
