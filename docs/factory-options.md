# Factory option checks

[Package overview](../README.md) | [Migration guide](../MIGRATION.md#factory-options-refuse-values-they-cannot-honor) | [Release history](../CHANGELOG.md)

Every factory checks its options when it is called, on the same terms as the adapter checks its own:

- **An unrecognized key warns and is ignored.** One `console.warn` names every unknown key and, when one is close enough, the documented key it was probably meant to be: `[extensions] unknown createPresence option(s): hearbeat (did you mean 'heartbeat'?) - not recognized by createPresence and ignored. ...`. It never throws, so an options object written for a newer version still starts an older one. A factory constructed many times with the same options (a group per room) warns once for the same set of unknown keys, not once per call.
- **A recognized key with a value the factory cannot honor throws** before any timer starts or any connection opens, naming the option, what it accepts and what it got: `redis presence: binary must be true or false - got "false" (string).` A misshaped value is never read as "use the default", because most of them silently disabled what they configured: `autoMigrate: 'false'` ran migrations, `breaker: 'on'` failed on first use, `persistLeaseMs: '6000'` fell back to 6000 without a word.
- **`undefined` and `null` both mean "not set"** and take the default. Breakers and metrics registries accept any falsy value as "none", as they always did.
- **Timer intervals stay within Node's timer range.** Node stores a timer delay in a signed 32-bit integer and turns anything above 2147483647 ms into a 1 ms loop, so every option that paces a timer refuses a larger value (for a seconds option, its millisecond equivalent; for a value whose fraction paces a timer, such as a lease renewed at a third of its length, that derived delay).
- **Lifetimes Redis receives as an expiry are whole milliseconds** (whole seconds where Redis takes seconds). Redis refuses a fractional PX or EXPIRE argument, so a value such as `ttlMs: 1500.5` used to fail every write that set it.

TypeScript declarations list each factory's accepted keys; the checks give a JavaScript caller the same answer at startup.
