import { breakerStates } from '../shared/breaker-observation.js';

const states = ['healthy', 'broken', 'probing'];
const registrations = new WeakMap();

/** Aggregate breaker state without exposing partition identifiers. */
export function wireBreakerMetrics(breaker, metrics) {
	if (!breaker || typeof breaker.subscribe !== 'function' || !states.includes(breaker.state)) throw new TypeError('wireBreakerMetrics: circuit breaker is required');
	if (!metrics || typeof metrics.gauge !== 'function' || typeof metrics.counter !== 'function') throw new TypeError('wireBreakerMetrics: metrics registry is required');
	let registry = registrations.get(metrics);
	if (!registry) {
		const gauge = metrics.gauge('circuit_breaker_states', 'Registered circuit breaker partitions by current state', ['state']);
		const transitions = metrics.counter('circuit_breaker_transitions_total', 'Registered circuit breaker state transitions', ['from', 'to']);
		const refs = new Set();
		registry = { refs, transitions, bindings: new WeakMap() };
		registrations.set(metrics, registry);
		installCollector(gauge, refs);
		for (const from of states) for (const to of states) if (from !== to) transitions.inc({ from, to }, 0);
	}
	let binding = registry.bindings.get(breaker);
	if (!binding) {
		const ref = new WeakRef(breaker);
		const transitions = registry.transitions;
		const unsubscribe = breaker.subscribe((from, to) => {
			if (from !== to && states.includes(from) && states.includes(to)) transitions.inc({ from, to });
		});
		binding = { ref, unsubscribe, count: 0 };
		registry.bindings.set(breaker, binding);
		registry.refs.add(ref);
	}
	binding.count++;
	let removed = false;
	return () => {
		if (removed) return;
		removed = true;
		if (--binding.count) return;
		binding.unsubscribe();
		registry.refs.delete(binding.ref);
		registry.bindings.delete(breaker);
	};
}

// This closure's context contains no strong breaker or subscription reference.
function installCollector(gauge, refs) {
	gauge.collect(() => {
		const counts = { healthy: 0, broken: 0, probing: 0 };
		for (const ref of refs) {
			const breaker = ref.deref();
			if (!breaker) { refs.delete(ref); continue; }
			const partitions = breakerStates.get(breaker);
			if (partitions) {
				if (!partitions.has('')) counts.healthy++;
				for (const entry of partitions.values()) counts[entry.state]++;
			} else if (states.includes(breaker.state)) counts[breaker.state]++;
		}
		for (const state of states) gauge.set({ state }, counts[state]);
	});
}
