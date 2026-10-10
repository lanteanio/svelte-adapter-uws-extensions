// Keep the display-oriented rateOf contract separate from admission bounds.
// Weak ownership does not retain an aggregator after its consumers are gone.
const readers = new WeakMap();

export function registerAdmissionRate(aggregator, reader) {
	readers.set(aggregator, reader);
	return aggregator;
}

export function admissionRateReader(aggregator) {
	return readers.get(aggregator) ?? (topic => aggregator.rateOf(topic));
}
