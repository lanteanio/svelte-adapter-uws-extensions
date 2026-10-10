import { describe, expect, it } from 'vitest';
import { collectPostgresPlans, normalizePlan } from '../scripts/collect-postgres-plans.js';
describe('PostgreSQL plan evidence', () => {
	it('keeps structural, row and I/O facts without timing noise or bound values', () => {
		const plan = normalizePlan({ 'Node Type': 'Index Scan', 'Relation Name': 'plan_replay', 'Index Name': 'idx_plan_replay_topic_seq', 'Actual Rows': 25, 'Actual Loops': 1, 'Shared Hit Blocks': 3, 'Actual Total Time': 123, 'Index Cond': "topic='private'", Plans: [{ 'Node Type': 'Result', 'Plan Rows': 1 }] });
		expect(plan).toEqual({ node: 'Index Scan', relation: 'plan_replay', index: 'idx_plan_replay_topic_seq', actualRows: 25, loops: 1, sharedHits: 3, children: [{ node: 'Result', estimatedRows: 1 }] });
		expect(JSON.stringify(plan)).not.toContain('private');
		expect(JSON.stringify(plan)).not.toContain('123');
	});
	it('rejects malformed plan roots and out-of-budget fixtures before opening a connection', async () => {
		expect(() => normalizePlan({})).toThrow('Missing');
		for (const rows of [0, 999, 250001, Infinity, 1000.5])
			await expect(collectPostgresPlans({ connectionString: 'unused', rows })).rejects.toThrow('rows must');
		await expect(collectPostgresPlans()).rejects.toThrow('isolated');
	});
});
