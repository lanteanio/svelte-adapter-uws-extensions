/**
 * Client-address classification for the per-IP rate-limit proxy-collapse warning.
 *
 * A per-IP limiter keys on `userData.remoteAddress`, which the adapter resolves
 * from ADDRESS_HEADER / XFF_DEPTH. When the server sits behind an address-
 * rewriting proxy (a docker userland-proxy, an L4 load balancer, a non-XFF
 * proxy) and ADDRESS_HEADER is unset, every client arrives as the same gateway
 * address - so a per-IP bucket silently degrades into one shared global bucket.
 *
 * The keyed address being a loopback / private / link-local one while no proxy
 * header is configured is the signature of that collapse: a real internet
 * client never presents one. These helpers let a limiter emit a one-shot
 * diagnostic when it first rejects on such a key, the per-message counterpart to
 * the adapter's own upgrade-path warning. They are pure and run off the hot path
 * (once per limiter, on the first denial), so clarity wins over micro-tuning.
 *
 * @module svelte-adapter-uws-extensions/shared/client-ip
 */

/**
 * Classify a dotted-quad IPv4 string as loopback / private / link-local /
 * unspecified. Returns false for any routable (public) address or malformed
 * input.
 * @param {string} s
 * @returns {boolean}
 */
function isPrivateOrLoopbackV4(s) {
	const parts = s.split('.');
	if (parts.length !== 4) return false;
	const o = [];
	for (let i = 0; i < 4; i++) {
		// Reject non-numeric or out-of-range octets so a malformed string never
		// classifies as private by coincidence.
		if (!/^\d{1,3}$/.test(parts[i])) return false;
		const n = Number(parts[i]);
		if (n > 255) return false;
		o.push(n);
	}
	const [a, b] = o;
	if (a === 0) return true; // 0.0.0.0/8 - "this network" / unspecified
	if (a === 127) return true; // 127.0.0.0/8 - loopback
	if (a === 10) return true; // 10.0.0.0/8 - private
	if (a === 172 && b >= 16 && b <= 31) return true; // 172.16.0.0/12 - private
	if (a === 192 && b === 168) return true; // 192.168.0.0/16 - private
	if (a === 169 && b === 254) return true; // 169.254.0.0/16 - link-local
	return false;
}

/**
 * Classify a lowercased IPv6 string as loopback / unique-local / link-local /
 * unspecified. Returns false for global-unicast (public) addresses.
 * @param {string} s
 * @returns {boolean}
 */
function isPrivateOrLoopbackV6(s) {
	if (s === '::' || s === '::1') return true; // unspecified, loopback
	if (s.startsWith('fc') || s.startsWith('fd')) return true; // fc00::/7 - unique local
	// fe80::/10 - link-local (first hextet 0xfe80-0xfebf)
	if (s.startsWith('fe8') || s.startsWith('fe9') || s.startsWith('fea') || s.startsWith('feb')) return true;
	return false;
}

/**
 * True when `ip` is a loopback, private, link-local, or unspecified address -
 * or an unusable sentinel ('', 'unknown', a non-string). A routable public
 * address returns false. Used as the collapse signature for the proxy-collapse
 * warning, never to make a rate-limit decision.
 *
 * @param {unknown} ip
 * @returns {boolean}
 */
export function isPrivateOrLoopbackAddress(ip) {
	if (typeof ip !== 'string') return true; // no usable address -> collapse signature
	let addr = ip.trim().toLowerCase();
	if (addr === '' || addr === 'unknown') return true;

	// Strip an IPv6 zone id (fe80::1%eth0) and surrounding brackets ([::1]).
	const zone = addr.indexOf('%');
	if (zone !== -1) addr = addr.slice(0, zone);
	if (addr.startsWith('[') && addr.endsWith(']')) addr = addr.slice(1, -1);

	// IPv4-mapped / -compatible IPv6 (::ffff:127.0.0.1, ::127.0.0.1): classify by
	// the embedded dotted IPv4 tail.
	if (addr.startsWith('::ffff:') || addr.startsWith('::')) {
		const tail = addr.slice(addr.lastIndexOf(':') + 1);
		if (tail.includes('.')) return isPrivateOrLoopbackV4(tail);
	}

	if (addr.includes(':')) return isPrivateOrLoopbackV6(addr);
	return isPrivateOrLoopbackV4(addr);
}

/**
 * True when an ADDRESS_HEADER is configured in the environment - i.e. the
 * adapter is told which proxy header carries the real client IP. Matches the
 * unprefixed `ADDRESS_HEADER` and any adapter-envPrefix form (`*_ADDRESS_HEADER`)
 * so a custom `envPrefix` deploy is detected too. A blank value counts as unset.
 *
 * Read once at limiter creation (process env is fixed before the server starts),
 * never per request.
 *
 * @param {Record<string, string | undefined>} [env]
 * @returns {boolean}
 */
export function isAddressHeaderConfigured(env = process.env) {
	for (const name in env) {
		if (name === 'ADDRESS_HEADER' || name.endsWith('_ADDRESS_HEADER')) {
			const v = env[name];
			if (typeof v === 'string' && v.trim() !== '') return true;
		}
	}
	return false;
}
