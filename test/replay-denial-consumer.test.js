import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';
import { parseFragment } from 'parse5';
import { describe, expect, it } from 'vitest';
import { checkReplayAccess } from '../src/shared/replay-gate.js';

const migration = readFileSync(new URL('../MIGRATION.md', import.meta.url), 'utf8');
function snippet(marker, language) {
	const source = migration.split('<!-- ' + marker + ' -->')[1];
	return source.split('```' + language + '\n')[1].split('```')[0];
}
function consumer(reqId = 'resume-1') {
	const nodes = { '#replay-notice': { textContent: '' }, '#replay-action': { hidden: true }, '#replay-content': { dataset: {} } };
	const context = { document: { querySelector: selector => nodes[selector] } };
	runInNewContext(snippet('replay-denial-consumer', 'js') + '\nglobalThis.consumer = { replayState, onReplayFrame };', context);
	context.consumer.replayState.reqId = reqId;
	return { ...context.consumer, nodes };
}

describe('documented replay-denial consumer', () => {
	it.each(['resume-1', 0])('turns an actual authorization denial into stale, non-writable, actionable state (%s)', async reqId => {
		const { replayState, onReplayFrame, nodes } = consumer(reqId);
		const platform = { checkSubscribe: () => 'FORBIDDEN', send: (_ws, _topic, event, data) => onReplayFrame(event, data) };
		expect(await checkReplayAccess({}, 'private-room', platform, reqId)).toBe(false);
		expect(replayState).toEqual({ status: 'denied', code: 'FORBIDDEN', reqId, stale: true, canWrite: false });
		expect(nodes['#replay-content'].dataset.stale).toBe('true');
		expect(nodes['#replay-notice'].textContent).toMatch(/access was denied.*out of date.*sign-in/i);
		expect(nodes['#replay-action'].hidden).toBe(false);
		onReplayFrame('end', { reqId });
		expect(replayState.status).toBe('denied');
	});

	it('ignores stale request denials and never renders an untrusted denial code as markup', () => {
		const { replayState, onReplayFrame, nodes } = consumer('new');
		onReplayFrame('denied', { reqId: 'old', code: 'FORBIDDEN' });
		expect(replayState.status).toBe('loading');
		expect(nodes['#replay-action'].hidden).toBe(true);
		onReplayFrame('denied', { reqId: 'new', code: '<img src=x onerror=alert(1)>' });
		expect(replayState.code).toContain('<img');
		expect(nodes['#replay-notice'].textContent).not.toContain('<img');
	});

	it('mounts a polite status, associated stale content, and a native recovery link', () => {
		const fragment = parseFragment(snippet('replay-denial-markup', 'html'));
		const elements = [];
		const visit = node => { if (node.tagName) elements.push(node); for (const child of node.childNodes || []) visit(child); };
		visit(fragment);
		const attrs = node => Object.fromEntries(node.attrs.map(({ name, value }) => [name, value]));
		const notice = elements.find(node => attrs(node).id === 'replay-notice');
		expect(attrs(notice)).toMatchObject({ role: 'status', 'aria-live': 'polite', 'aria-atomic': 'true' });
		const content = elements.find(node => attrs(node).id === 'replay-content');
		expect(attrs(content)['aria-describedby']).toBe('replay-notice');
		const action = elements.find(node => node.tagName === 'a');
		expect(attrs(action)).toEqual({ href: '/account' });
		expect(action.childNodes[0].value).toMatch(/sign-in and access/i);
	});
});
