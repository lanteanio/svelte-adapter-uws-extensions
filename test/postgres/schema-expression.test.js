import { expect, it } from 'vitest';
import { schemaExpression } from '../../src/postgres/_schema-expression.js';

it('preserves literal case and boolean grouping', () => {
	expect(schemaExpression("status = 'running'")).not.toBe(schemaExpression("status = 'RUNNING'"));
	expect(schemaExpression('a IS NULL AND (b IS NULL OR c IS NULL)')).not.toBe(schemaExpression('(a IS NULL AND b IS NULL) OR c IS NULL'));
	expect(schemaExpression("status = '= ANY(ARRAY[running])'")).not.toBe(schemaExpression("status = ' IN (running)'"));
});
it('accepts PostgreSQL formatting and constant IN rendering', () => {
	expect(schemaExpression("((status = 'pending'::text) OR (status = 'committed'::text))")).toBe(schemaExpression("status = 'pending' OR status = 'committed'"));
	expect(schemaExpression("(status = ANY (ARRAY['committed'::text, 'failed'::text]))")).toBe(schemaExpression("status IN ('committed', 'failed')"));
});
it('rejects unrecognized operators and trailing expressions', () => {
	expect(schemaExpression('a + b')).toBe(null);
	expect(schemaExpression("status = 'running'; SELECT 1")).toBe(null);
});
