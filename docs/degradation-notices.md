# Localized and accessible degradation notices

[Package overview](../README.md) | [Proactive degradation policy](../README.md#proactive-degradation-policy) | [Release history](../CHANGELOG.md)

A degradation policy emits one shared machine event for every display language. The server supplies stable localization metadata and a source-English fallback; the application selects and renders the client's language. Extensions does not negotiate locales or ship a user-interface catalog.

## Notice fields and bounds

Both `mitigation` and `recovery` accept these optional fields:

| Field | Contract |
| --- | --- |
| `messageKey` | An application catalog key containing only ASCII letters, digits, `.`, `_`, or `-`, at most 128 UTF-8 bytes. It does not name a built-in catalog. |
| `messageParams` | A plain interpolation object with at most 64 entries. Keys follow `messageKey` syntax and bounds. Values are strings, finite numbers, booleans, or null; each string is at most 1024 UTF-8 bytes and the serialized object is at most 4096 UTF-8 bytes. |
| `bannerCopy` | The source-English fallback: well-formed plain text containing a non-ignorable letter or number, at most 2048 UTF-8 bytes, without control/format characters or line breaks. ZWNJ/ZWJ joiners may accompany text. |

On the server, one invalid field rejects the whole envelope: a static envelope makes `createDegradationPolicy` throw, and an invalid envelope returned by a dynamic policy makes that transition throw, so the bus emits the bare `degraded` or `recovered` event without a `mitigation` or `recovery` envelope. On receipt, the realtime client validates notice metadata and drops each field it rejects, keeping the envelope's other fields. Render resolved notices as text, never as HTML.

## Resolve the display language

Resolve a known key through the selected display-language catalog. For an unknown key, use `bannerCopy`, then the application's generic localized fallback for that transition. A transition without an envelope, such as a recovery with no `recovery` field, resolves straight to that fallback. Applications own locale selection, fallback chains, interpolation, plural and number formatting, `lang`, `dir`, and bidirectional isolation. Keep machine identifiers outside translated prose and isolate untrusted identifiers at the rendering boundary.

```js
const catalogs = {
  en: {
    generic: 'Real-time updates are temporarily unavailable.',
    restored: 'Real-time updates are available again.',
    'orders.unavailable': 'Saving orders is unavailable while {service} recovers.',
    'orders.restored': 'Saving orders is available again.'
  },
  de: {
    generic: 'Echtzeit-Updates sind vorübergehend nicht verfügbar.',
    restored: 'Echtzeit-Updates sind wieder verfügbar.',
    'orders.unavailable': 'Bestellungen können während der Wiederherstellung von {service} nicht gespeichert werden.',
    'orders.restored': 'Bestellungen können wieder gespeichert werden.'
  }
};

export function resolveNotice(notice, locale = 'en', fallbackKey = 'generic') {
  const catalog = Object.hasOwn(catalogs, locale) ? catalogs[locale] : catalogs.en;
  if (!notice) return catalog[fallbackKey];
  const template = notice.messageKey && Object.hasOwn(catalog, notice.messageKey)
    ? catalog[notice.messageKey]
    : null;
  if (typeof template === 'string') {
    return template.replace(/\{([A-Za-z0-9_.-]+)\}/g, (match, key) =>
      Object.hasOwn(notice.messageParams || {}, key) ? String(notice.messageParams[key]) : match
    );
  }
  return notice.bannerCopy || catalog[fallbackKey];
}
```

A locale switch updates visible copy through the same resolver. It does not emit a new degradation or recovery transition and must not cause another transition announcement.

## Announce and recover accessibly

Keep one persistent polite status region mounted before the first event with `role="status"`, `aria-live="polite"`, and `aria-atomic="true"`. Update its announcement only on a meaningful degradation or recovery transition, not on every render, locale switch, duplicate event, or timer tick. The visible reason and announcement use the same resolved copy.

Map each unavailable control to its visible reason with `aria-describedby`. Mark a control that may hold focus with `aria-disabled="true"` and make its action handler refuse activation, including form submission; the control stays focusable, so focus stays where the user left it when the transition arrives. Native `disabled` drops focus from a focused control, so reserve it for controls that cannot hold focus at that moment. Do not disable unaffected controls. Recovery restores availability and removes the stale reason association without moving focus. Announce recovery once through the same persistent region.

Derive a retry deadline once from receipt of the degraded transition plus `retryAfterMs`. Keep it stable across rerenders and duplicate events. It is retry guidance, not a recovery promise. Without `retryAfterMs`, show no countdown or estimated recovery time. Put countdowns and elapsed-time updates outside the live region so timer ticks are not announced.

This Svelte reference uses the resolver above with realtime's `degradation` store:

```svelte
<script>
  import { degradation } from 'svelte-realtime/client';
  import { resolveNotice } from '$lib/notice.js';

  let locale = 'en';
  let announced = '';
  let wasActive = false;
  let retryAt = null;

  $: visibleCopy = $degradation.active
    ? resolveNotice($degradation.mitigation, locale)
    : resolveNotice($degradation.recovery, locale, 'restored');
  $: savingUnavailable = $degradation.active &&
    Boolean($degradation.mitigation?.rpcs?.includes('orders/create'));
  $: if ($degradation.active !== wasActive) {
    wasActive = $degradation.active;
    announced = visibleCopy;
    retryAt = wasActive && Number.isFinite($degradation.mitigation?.retryAfterMs)
      ? Date.now() + $degradation.mitigation.retryAfterMs
      : null;
  }

  function saveOrder() {
    if (savingUnavailable) return;
    // Submit the order.
  }
</script>

<div role="status" aria-live="polite" aria-atomic="true">{announced}</div>
{#if $degradation.active}
  <p id="degradation-reason">{visibleCopy}</p>
{/if}
<button
  type="button"
  aria-disabled={savingUnavailable}
  aria-describedby={savingUnavailable ? 'degradation-reason' : undefined}
  on:click={saveOrder}
>Save order</button>
{#if retryAt !== null}
  <p>Retry at <time datetime={new Date(retryAt).toISOString()}>{new Date(retryAt).toISOString()}</time></p>
{/if}
```

The realtime fixture exercises transition-only announcements, catalog switching and unknown-key fallback, control gating, focus-preserving recovery, stable retry deadlines, and automated accessibility checks. See [Notifying clients of degradation](../README.md#notifying-clients-of-degradation) for the backend-capability versus WebSocket-transport distinction and timestamp semantics.
