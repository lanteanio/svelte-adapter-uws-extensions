# README code examples

Every fenced block in the [README](../README.md) has a fingerprint, source location, classification, and validation path in [the code manifest](./code-blocks.v1.json).

| Class | Validation |
| --- | --- |
| `executed` | The complete module runs in a fresh Node process against the extracted `npm pack` tarball. Exported handlers are defined, not automatically invoked. |
| `syntax` | JavaScript parses with Acorn; TypeScript is type-checked; Svelte compiles; JSON and YAML parse. Application-provided identifiers are recorded. |
| `tutorial` | Code with application imports is parsed, package imports and immediate imported members are checked, and TypeScript is type-checked with application import shims. It needs the surrounding application to run. |
| `fragment` | An explicitly marked handler-body excerpt cannot parse as a standalone module. The marker explains its context, and the manifest caps the number of fragments. |
| `command` | Shell and SQL commands are reviewed instructions requiring their stated environment; the documentation check does not execute them. |
| `config` | Dockerfile, INI, and nginx configuration excerpts are reviewed, not executed. JSON and YAML use the syntax path instead. |
| `output` | Console output and directory listings are reviewed reference material. |

Package imports and immediate member accesses in compilable code are checked against the public TypeScript declarations, including re-exports. JavaScript argument types and application behavior are not inferred by the syntax check. The packed runner uses the lockfile-resolved dependency tree and imports this package from its tarball rather than the source checkout.

Run `npm run check:docs-code` for classification and compiler checks, or `npm run verify:docs` to include packed execution. The normal test suite includes both. When editing a README fence or moving its source location, run `node scripts/check-doc-code.js --write`, then run `npm run verify:docs` and review the generated manifest diff.

Put an exception marker immediately above its fence:

- `<!-- doc-code: fragment reason="Handler body; return exits the enclosing hook" -->` for an intentional excerpt that cannot parse on its own. Prefer a complete example when possible. Growing the pinned fragment count requires an explicit `--accept-fragment-count` update and review.
- `<!-- doc-code: no-run reason="external-service: Redis must be running" -->` for code that parses but needs a service. The other reason prefixes are `requires-browser` and `app-context`; describe the missing environment after the prefix.
- `<!-- doc-code: ambient names="event,locals" -->` for identifiers supplied by the surrounding application. TypeScript examples should otherwise import or declare their context explicitly.

An unknown language, broken syntax without an intentional fragment marker, missing package export, stale fingerprint, or incorrect classification fails the gate. The checker reports counts by class and validation path so execution coverage stays visible.
