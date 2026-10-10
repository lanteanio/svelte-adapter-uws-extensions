/** Parse the small expression grammar used by the schema, preserving literal case and boolean precedence. */
export function schemaExpression(source) {
	if (source == null) return 'none';
	const input = String(source);
	const tokens = [];
	const lexer = /\s+|::(?:text|bigint|integer|character varying)(?:\[\])?|>=|<=|<>|!=|=|>|<|\(|\)|\[|\]|,|'(?:''|[^'])*'|[a-zA-Z_][a-zA-Z0-9_]*|\d+/gy;
	let offset = 0, match;
	while (offset < input.length) {
		lexer.lastIndex = offset; match = lexer.exec(input);
		if (!match) return null;
		offset = lexer.lastIndex;
		if (/^\s|^::/.test(match[0])) continue;
		tokens.push(match[0].startsWith("'") ? match[0] : match[0].toLowerCase());
	}
	let cursor = 0;
	const take = token => tokens[cursor] === token && (++cursor > 0);
	function primary() {
		if (take('(')) { const value = expression(); if (!take(')')) throw new Error('unclosed expression'); return value; }
		const token = tokens[cursor++];
		if (!token || ['and', 'or', 'is', 'in', ')', '[', ']', ',', '=', '>=', '<=', '!=', '<>', '>', '<'].includes(token)) throw new Error('invalid operand');
		return ['value', token];
	}
	function comparison() {
		const left = primary();
		if (take('is')) {
			const negate = take('not');
			if (!take('null')) throw new Error('invalid null comparison');
			return [negate ? 'is not null' : 'is null', left];
		}
		if (take('in')) {
			if (!take('(')) throw new Error('invalid list');
			const entries = [primary()]; while (take(',')) entries.push(primary());
			if (!take(')')) throw new Error('unclosed list');
			return ['in', left, entries];
		}
		const operator = tokens[cursor];
		if (operator === '=' && tokens[cursor + 1] === 'any') {
			cursor += 2;
			if (!take('(') || !take('array') || !take('[')) throw new Error('invalid array comparison');
			const entries = [primary()]; while (take(',')) entries.push(primary());
			if (!take(']') || !take(')')) throw new Error('unclosed array comparison');
			return ['in', left, entries];
		}
		if (['=', '>=', '<=', '!=', '<>', '>', '<'].includes(operator)) { cursor++; return [operator, left, primary()]; }
		return left;
	}
	function conjunction() {
		let value = comparison(); while (take('and')) value = ['and', value, comparison()]; return value;
	}
	function expression() {
		let value = conjunction(); while (take('or')) value = ['or', value, conjunction()]; return value;
	}
	try { const value = expression(); return cursor === tokens.length ? JSON.stringify(value) : null; }
	catch { return null; }
}
