/**
 * The one rule for deciding whether a WIRE topic belongs to a tenant.
 *
 * Tenancy is carried in the topic itself - a tenant-scoped wire topic is
 * `@t/<tenantId>/<topic>` - not in a per-table column. That shape is
 * wire-frozen with the realtime layer, so every surface that scopes an
 * operation by tenant has to agree on it byte for byte. It lives here rather
 * than in any one store because a right-to-erasure purge fans out across
 * several of them at once, and a store that applies a different rule (or none)
 * silently widens or narrows the erasure the caller asked for.
 *
 * @module svelte-adapter-uws-extensions/shared/tenant-topic
 */

/** Namespace prefix of a tenant-scoped wire topic. */
export const TENANT_TOPIC_NS = '@t/';

/**
 * With a tenantId: true iff the topic carries that tenant's namespace prefix.
 * With null: true iff the topic carries NO tenant prefix. So an untenanted
 * erasure can never reach a tenant's data and vice versa - and in a
 * deployment that uses no tenants at all, every topic is untenanted and the
 * null scope still matches all of them.
 *
 * @param {string | null} tenantId
 * @param {string} topic
 * @returns {boolean}
 */
export function topicInTenant(tenantId, topic) {
	if (typeof topic !== 'string') return false;
	if (tenantId) return topic.startsWith(TENANT_TOPIC_NS + tenantId + '/');
	return !topic.startsWith(TENANT_TOPIC_NS);
}

/**
 * SQL fragment applying the same rule to a `topic` column, plus the parameter
 * to bind. `$<n>` is the placeholder index the caller has available.
 *
 * `left(topic, char_length($n)) = $n` rather than `LIKE $n || '%'`: a
 * validated tenant id may contain `_`, which is a LIKE wildcard matching any
 * single character, so `LIKE` would let tenant `a_c` match topics under
 * `abc`. `left(...)` has no pattern semantics at all.
 *
 * @param {string | null} tenantId
 * @param {number} paramIndex - 1-based placeholder number for the tenant value.
 * @returns {{ sql: string, value: string }}
 */
export function tenantTopicSql(tenantId, paramIndex) {
	const p = '$' + paramIndex;
	if (tenantId) {
		return {
			sql: `left(topic, char_length(${p})) = ${p}`,
			value: TENANT_TOPIC_NS + tenantId + '/'
		};
	}
	return {
		sql: `left(topic, char_length(${p})) <> ${p}`,
		value: TENANT_TOPIC_NS
	};
}
