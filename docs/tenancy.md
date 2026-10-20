# Tenant namespaces and shared rate budgets

[Package overview](../README.md#rate-limiting) | [Authorization](../README.md#authorize-every-extension-call)

A tenant namespace prevents two tenants from sharing state. A tenant budget also bounds how much all of one tenant's principals can consume together. Ten separate per-principal allowances of three points still permit thirty actions; a shared tenant allowance of three permits three across every connection and server instance using the same Redis namespace.

## Ownership

| Surface | Responsibility |
| --- | --- |
| Adapter connection attribution | Resolves and freezes the tenant identity at connection open. The supported `svelte-adapter-uws/connection` accessor reads that identity. |
| Adapter in-memory rate limiter | Enforces a worker-local principal or tenant allowance. |
| Realtime tenant namespace and config | Namespaces application resources and carries declared tenant metadata. Metadata alone does not enforce a budget. |
| Extensions Redis rate limiter | Enforces the explicitly configured shared allowance atomically in Redis. The optional local floor is per instance during storage failures. |
| Application admission | Chooses the unit, authenticates the tenant, installs the limiter, and rejects work when its verdict denies. |

## One allowance per tenant

```js
import { createRateLimit } from 'svelte-adapter-uws-extensions/redis/ratelimit';

const limiter = createRateLimit(redis, {
  points: 3,
  interval: 60000,
  budget: 'tenant'
});

const verdict = await limiter.consume(ws);
if (!verdict.allowed) return;
```

`budget: 'principal'` remains the default and gives each `keyBy` result its own allowance inside the tenant namespace. Both modes use the adapter's frozen attribution when `tenant` is omitted and cache that immutable answer per connection. An explicit `tenant: ws => id` resolver overrides that source, including a null answer. Without attribution or a resolver, principal mode retains its unscoped keys; tenant mode rejects `consume` and `peek` because an unidentified connection cannot name a tenant allowance. Tenant ids must be non-empty strings without NUL bytes in tenant mode.

Tenant mode never calls `keyBy`. `window`, `sliding`, and `gcra` keep their existing refill semantics, and both `consume` and read-only `peek` see the same shared state. All limiter instances sharing a namespace must agree on the budget and refill configuration. The emergency scale applies as before. A local storage-failure floor shares its allowance across that instance's tenant principals, but N instances can still allow N times the configured floor while Redis is unavailable.

Admin calls select the aggregate with the existing tenant argument: `reset('', tenantId)`, `ban('', duration, tenantId)`, and `unban('', tenantId)`. Their principal-key argument is ignored. `clear(tenantId)` deletes that aggregate; `clear()` clears this budget mode and refill mode. Tenant-total keys use a separate namespace from principal keys, so neither can reset the other's allowance or collide with an unscoped principal named like a tenant. `purgeUser` returns zero in tenant mode: the aggregate contains no user-owned record, and erasing one user must not restore everyone else's allowance.

## Principal and tenant limits in one atomic check

For both limits together, use the existing composite limiter with a constant key for the tenant-total dimension:

```js
import { attribution } from 'svelte-adapter-uws/connection';
import { createCompositeRateLimit } from 'svelte-adapter-uws-extensions/redis/ratelimit';

const limiter = createCompositeRateLimit(redis, {
  tenant: (ws) => {
    const id = attribution(ws)?.tenantId;
    if (!id) throw new Error('Authenticated tenant required');
    return id;
  },
  dimensions: {
    principal: { points: 3, interval: 60000, keyBy: (ws) => ws.getUserData().userId },
    tenant: { points: 10, interval: 60000, keyBy: () => 'all' }
  }
});
```

The tenant resolver scopes both dimensions to one Redis Cluster hash slot. Every dimension is checked and consumed atomically; a refusal spends nothing in the other dimension. The composite limiter requires its own explicit tenant resolver and does not automatically read attribution or append dimensions.

## Weighted costs and configured caps

The existing `consume(ws, cost)` and `peek(ws, cost)` accept a positive integer cost. Choose one unit per limiter: `Buffer.byteLength(payload, 'utf8')` for a text payload, `payload.byteLength` for binary bytes, or the number of intended recipients for delivery work. Skip a zero-cost operation rather than passing zero. Use the same unit for `points`; composite dimensions charge the same cost, so do not mix bytes and actions in one composite check. Charge before performing the work, and do not describe the count as a client acknowledgement.

Where an application has the realtime tenant config carrier, it can read `live.tenant(id).config?.metadata?.caps` and deliberately configure the owning limiter using a positive `actions`, `bytes`, or `recipientDeliveries` ceiling and `windowMs` as its interval. Keep the chosen config consistent on every server. An omitted ceiling declares no limit; zero declares no allowance and must be rejected by application admission without creating a positive-points limiter. The application must configure enforcement explicitly from this metadata.

A retained `storedBytes` cap is not a periodically refilled rate limit: it needs storage accounting that releases capacity when records are deleted. Passing stored bytes as a rate cost meters writes per interval only, and must not be advertised as an enforced retained-storage ceiling.

## Deployment

Principal buckets retain their existing Redis layout. Connections with adapter attribution now use the attributed namespace even without an explicit resolver, matching the adapter limiter; these connections begin with a fresh allowance if an older deployment wrote them unscoped. An explicit `tenant: () => null` preserves unscoped principal behavior when that is intentional.

Tenant budgets use `v1:ratelimit-tenant:`, `v1:ratelimits-tenant:`, or `v1:ratelimitg-tenant:` under the client prefix for window, sliding, or GCRA respectively. Include those patterns in restricted Redis key ACLs before enabling the mode. Switching budget or refill mode starts a fresh allowance; old keys expire by their existing TTL. Deploy the admission configuration consistently across the fleet so older instances cannot keep admitting through per-principal budgets.
