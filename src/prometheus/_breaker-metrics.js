import { breakerStates } from '../shared/breaker-observation.js';

const states = ['healthy', 'broken', 'probing'];
const stateRegistrations = new WeakMap();
const transitionRegistrations = new WeakMap();

/** Aggregate breaker state without exposing partition identifiers. */
export function wireBreakerMetrics(breaker, metrics) {
	if (!breaker || typeof breaker.subscribe !== 'function' || !states.includes(breaker.state)) throw new TypeError('wireBreakerMetrics: circuit breaker is required');
	if (!metrics || typeof metrics.gauge !== 'function' || typeof metrics.counter !== 'function') throw new TypeError('wireBreakerMetrics: metrics registry is required');
	const gauge = metrics.gauge('circuit_breaker_states', 'Registered circuit breaker partitions by current state', ['state']);
	const transitions = metrics.counter('circuit_breaker_transitions_total', 'Registered circuit breaker state transitions', ['from', 'to']);
	let registry = stateRegistrations.get(gauge);
	if (!registry) {
		registry = createStateCollector(gauge);
		stateRegistrations.set(gauge, registry);
	}
	let transitionBindings = transitionRegistrations.get(transitions);
	if (!transitionBindings) {
		transitionBindings = new WeakMap();
		transitionRegistrations.set(transitions, transitionBindings);
		for (const from of states) for (const to of states) if (from !== to) transitions.inc({ from, to }, 0);
	}
	let transitionBinding = transitionBindings.get(breaker);
	if (!transitionBinding) {
		const unsubscribe = breaker.subscribe((from, to) => {
			if (from !== to && states.includes(from) && states.includes(to)) transitions.inc({ from, to });
		});
		transitionBinding = { unsubscribe, count: 0 };
		transitionBindings.set(breaker, transitionBinding);
	}
	let binding = registry.bindings.get(breaker);
	if (!binding) {
		const ref = new WeakRef(breaker);
		binding = { ref, count: 0 };
		registry.bindings.set(breaker, binding);
		registry.refs.add(ref);
		registry.finalizer.register(breaker, ref, ref);
	}
	binding.count++;
	transitionBinding.count++;
	let removed = false;
	return () => {
		if (removed) return;
		removed = true;
		if (!--binding.count) {
			registry.finalizer.unregister(binding.ref);
			registry.refs.delete(binding.ref);
			registry.bindings.delete(breaker);
		}
		if (!--transitionBinding.count) {
			transitionBinding.unsubscribe();
			transitionBindings.delete(breaker);
		}
	};
}

// This closure's context contains no strong breaker or subscription reference.
function createStateCollector(gauge) {
	const refs = new Set();
	const finalizer = new FinalizationRegistry(ref => refs.delete(ref));
	gauge.collect(() => {
		const counts = { healthy: 0, broken: 0, probing: 0 };
		for (const ref of refs) {
			const breaker = ref.deref();
			if (!breaker) { refs.delete(ref); finalizer.unregister(ref); continue; }
			const partitions = breakerStates.get(breaker);
			if (partitions) {
				if (!partitions.has('')) counts.healthy++;
				for (const entry of partitions.values()) counts[entry.state]++;
			} else if (states.includes(breaker.state)) counts[breaker.state]++;
		}
		for (const state of states) gauge.set({ state }, counts[state]);
	});
	return { refs, finalizer, bindings: new WeakMap() };
}
