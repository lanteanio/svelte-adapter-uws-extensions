export function invalidHealthEvents() {
	return [
		['unknown event', 'unknown', { at: 1 }],
		['unicast stale notice', 'protocol-stale', { at: 1 }],
		['unicast mismatch notice', 'protocol-mismatch', { at: 1 }],
		['null data', 'degraded', null],
		['array data', 'degraded', []],
		['missing timestamp', 'degraded', { reason: 'invalid' }],
		['string timestamp', 'recovered', { at: '1' }],
		['negative timestamp', 'degraded', { at: -1 }],
		['unknown data field', 'recovered', { at: 1, custom: true }],
		['wrong detail field', 'degraded', { at: 1, recovery: {} }],
		['primitive mitigation', 'degraded', { at: 1, mitigation: 'invalid' }],
		['primitive recovery', 'recovered', { at: 1, recovery: 0 }],
		['wrong streams', 'degraded', { at: 1, mitigation: { streams: [1] } }],
		['negative retry', 'degraded', { at: 1, mitigation: { retryAfterMs: -1 } }],
		['oversized notice', 'degraded', { at: 1, mitigation: { bannerCopy: 'x'.repeat(2049) } }],
		['oversized catalog key', 'degraded', { at: 1, mitigation: { messageKey: 'x'.repeat(129) } }],
		['oversized parameter', 'recovered', { at: 1, recovery: { messageParams: { x: 'x'.repeat(1025) } } }],
		['wrong recovery flag', 'recovered', { at: 1, recovery: { refetch: 'yes' } }],
		['malformed refetch request', 'recovered', { recovery: { refetch: true } }]
	];
}
