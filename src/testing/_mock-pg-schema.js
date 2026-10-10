function splitColumns(body) {
	const parts = []; let depth = 0, start = 0;
	for (let i = 0; i < body.length; i++) {
		if (body[i] === '(') depth++;
		else if (body[i] === ')') depth--;
		else if (body[i] === ',' && depth === 0) { parts.push(body.slice(start, i).trim()); start = i + 1; }
	}
	parts.push(body.slice(start).trim()); return parts;
}

/** Catalog and migration-ledger state for the SQL double. Native tests enforce database constraints. */
export function mockPgSchema() {
	const tables = new Map(), indexes = new Map(), checks = new Map(), ledger = new Map();
	let lockOwner = null;
	const waiters = [];
	const result = rows => ({ rows, rowCount: rows.length });
	function column(definition) {
		const match = /^(\w+)\s+(TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ|BIGSERIAL|BIGINT|INTEGER|INT|TEXT|UUID|JSONB)/i.exec(definition);
		if (!match) return null;
		let type = match[2].toLowerCase();
		if (type === 'bigserial') type = 'bigint';
		if (type === 'int') type = 'integer';
		if (type === 'timestamptz') type = 'timestamp with time zone';
		return { column_name: match[1], data_type: type, not_null: /NOT NULL|PRIMARY KEY|BIGSERIAL/i.test(definition), column_default: /BIGSERIAL/i.test(definition) ? "nextval('sequence')" : /DEFAULT\s+(.+)$/i.exec(definition)?.[1] ?? null };
	}
	function query(sql, values, connection) {
		if (sql === 'SHOW server_version_num') return result([{ server_version_num: '160015' }]);
		if (sql === 'SELECT to_regclass($1) IS NOT NULL AS present') return result([{ present: tables.has(values[0]) }]);
		if (sql.startsWith('SELECT privilege, has_table_privilege')) return result(values[1].map(privilege => ({ privilege, granted: tables.has(values[0]) })));
		if (sql.startsWith('SELECT has_sequence_privilege')) return result([{ allowed: tables.has(values[0]), default_matches: tables.get(values[0])?.find(column => column.column_name === values[1])?.column_default === "nextval('sequence')" }]);
		if (sql.includes('pg_try_advisory_lock(hashtextextended')) {
			const acquired = lockOwner === null || lockOwner === connection;
			if (acquired) lockOwner = connection;
			return result([{ acquired }]);
		}
		if (sql.includes('pg_advisory_lock(hashtextextended')) {
			if (lockOwner === null || lockOwner === connection) { lockOwner = connection; return result([]); }
			return new Promise(resolve => waiters.push({ connection, resolve }));
		}
		if (sql.startsWith('SELECT store, version, checksum FROM svti_schema_migrations')) return result([...(ledger.get(values[0]) ?? [])]);
		if (sql.startsWith('INSERT INTO svti_schema_migrations')) {
			const rows = ledger.get(values[0]) ?? [];
			rows.push({ store: values[1], version: values[2], checksum: values[3] }); ledger.set(values[0], rows); return result([]);
		}
		if (sql.startsWith('CREATE TABLE')) {
			const match = /^CREATE TABLE(?: IF NOT EXISTS)? (\w+) \((.*)\)$/.exec(sql);
			if (match && !tables.has(match[1])) {
				const columns = splitColumns(match[2]), mapped = columns.map(column).filter(Boolean);
				tables.set(match[1], mapped);
				const key = columns.find(item => item.startsWith('PRIMARY KEY'));
				const names = key ? /\((.*)\)/.exec(key)[1].split(',').map(item => item.trim()) : columns.filter(item => /PRIMARY KEY/.test(item)).map(item => /^\w+/.exec(item)[0]);
				for (const entry of mapped) if (names.includes(entry.column_name)) entry.not_null = true;
				if (names.length) indexes.set(`${match[1]}_pkey`, { table: match[1], name: `${match[1]}_pkey`, valid: true, unique: true, primary: true, columns: names, predicate: null });
			}
		}
		if (sql.startsWith('ALTER TABLE')) {
			const match = /^ALTER TABLE (\w+) ADD COLUMN(?: IF NOT EXISTS)? (.*)$/.exec(sql);
			if (match) { const entry = column(match[2]), columns = tables.get(match[1]); if (entry && columns && !columns.some(item => item.column_name === entry.column_name)) columns.push(entry); }
			const validate = /^ALTER TABLE (\w+) VALIDATE CONSTRAINT (\w+)$/.exec(sql);
			if (validate) { const entry = checks.get(validate[2]); if (entry) entry.valid = true; }
		}
		if (sql.startsWith('CREATE') && sql.includes('INDEX')) {
			const match = /^CREATE (UNIQUE )?INDEX(?: CONCURRENTLY)?(?: IF NOT EXISTS)? (\w+) ON (\w+) \((.*?)\)(?: WHERE (.*))?$/.exec(sql);
			if (match && !indexes.has(match[2])) indexes.set(match[2], { table: match[3], name: match[2], valid: true, unique: Boolean(match[1]), primary: false, columns: match[4].split(',').map(item => item.trim()), predicate: match[5] ?? null });
			return result([]);
		}
		if (sql.startsWith('DROP INDEX')) { indexes.delete(sql.split(' ').at(-1)); return result([]); }
		if (sql.startsWith('DO $migration$')) {
			const match = /ALTER TABLE (\w+) ADD CONSTRAINT (\w+) CHECK \((.*)\) NOT VALID;/.exec(sql);
			if (match && !checks.has(match[2])) checks.set(match[2], { table: match[1], name: match[2], expression: match[3], valid: false });
			return result([]);
		}
		if (sql.includes('FROM pg_attribute a')) return result(tables.get(values[0]) ?? []);
		if (sql.startsWith('SELECT i.indisvalid')) { const entry = indexes.get(values[0]); return result(entry ? [{ indisvalid: entry.valid, owned: entry.table === values[1] }] : []); }
		if (sql.includes('FROM pg_index i JOIN pg_class c')) return result([...indexes.values()].filter(entry => entry.table === values[0]).map(entry => ({ ...entry, method: 'btree' })));
		if (sql.includes("contype IN ('f', 'x')")) return result([]);
		if (sql.includes('FROM pg_constraint WHERE')) return result([...checks.values()].filter(entry => entry.table === values[0]));
	}
	query.release = connection => {
		if (lockOwner !== connection) return;
		const next = waiters.shift(); lockOwner = next?.connection ?? null;
		next?.resolve(result([]));
	};
	return query;
}
