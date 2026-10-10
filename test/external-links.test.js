import { readFileSync } from 'node:fs';
import { describe, it, expect } from 'vitest';
import { externalLinksOf, collectExternalUrls, crawlUrl, crawlExternalLinks, looksLikeSoft404, publicUrlProblem } from '../scripts/check-external-links.js';
describe('scheduled external documentation checks', () => {
	it('collects visible targets with deduplicated URLs', () => {
		expect(externalLinksOf('`[not](https://example.com/no)`')).toEqual([]);
		const result = collectExternalUrls([{ relative: 'README.md', text: '[one](https://example.com/x#a) [two](https://example.com/x#b)' }]);
		expect([...result.urls.keys()]).toEqual(['https://example.com/x']);
		expect(result.urls.get('https://example.com/x')).toHaveLength(2);
	});
	it('refuses private and credentialed targets', async () => {
		for (const url of ['http://127.0.0.1/', 'http://localhost./', 'https://user:secret@example.com']) expect(publicUrlProblem(url)).toBeTruthy();
		let fetched = false;
		expect((await crawlUrl('http://localhost./', { fetchImpl: () => { fetched = true; } })).ok).toBe(false);
		expect(fetched).toBe(false);
	});
	it('distinguishes successful pages, redirects and missing pages', async () => {
		const fetchImpl = async (url) => url.endsWith('/old') ? new Response('', { status: 301, headers: { location: '/new' } }) : new Response('<h1>Guide</h1>', { status: 200 });
		expect((await crawlUrl('https://example.com/new', { fetchImpl })).ok).toBe(true);
		expect((await crawlUrl('https://example.com/old', { fetchImpl })).kind).toBe('redirect');
		expect((await crawlUrl('https://example.com/gone', { fetchImpl: async () => new Response('<title>404 - Page not found</title>', { headers: { 'content-type': 'text/html' } }) })).ok).toBe(false);
		expect(looksLikeSoft404('<h1>How to handle 404 responses</h1>')).toBe(false);
	});
	it('bounds concurrent requests and retains input order', async () => {
		let active = 0, peak = 0;
		const urls = Array.from({ length: 7 }, (_, i) => 'https://example.com/' + i);
		const result = await crawlExternalLinks(urls, { concurrency: 2, fetchImpl: async () => { active++; peak = Math.max(peak, active); await new Promise((resolve) => setTimeout(resolve, 2)); active--; return new Response('ok'); } });
		expect(peak).toBeLessThanOrEqual(2);
		expect(result.map((entry) => entry.url)).toEqual(urls);
	});
	it('keeps network checks in a scheduled/manual lane after dependency installation', () => {
		const workflow = readFileSync(new URL('../.github/workflows/docs-links.yml', import.meta.url), 'utf8');
		expect(workflow).toMatch(/schedule:/);
		expect(workflow).toMatch(/workflow_dispatch:/);
		expect(workflow).not.toMatch(/^  (?:push|pull_request):/m);
		expect(workflow.indexOf('npm ci')).toBeLessThan(workflow.indexOf('npm run check:links:external'));
		expect(workflow).toContain('contents: read');
	});
});
