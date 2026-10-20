// Child process for close-on-shutdown.test.js. Registers one connection with
// presence and the connection registry, then runs both close hooks the way a
// graceful shutdown does: started, not awaited, and the process exits on the
// next turn of the event loop, before any Redis reply can arrive.
import { createBackendClient } from '../helpers/backend.js';
import { createPresence } from '../../../src/redis/presence.js';
import { createConnectionRegistry } from '../../../src/redis/registry.js';
import { mockPlatform } from '../../../src/testing/mock-platform.js';
import { mockWs } from '../../../src/testing/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';

const client = createBackendClient({ keyPrefix: process.argv[2] });
const platform = mockPlatform();
const presence = createPresence(client, { key: 'id', consistencyAuditIntervalMs: 0 });
const registry = createConnectionRegistry(client, {
	identify: (ws) => ws.getUserData().userId,
	attributes: (ws) => ({ tenantId: ws.getUserData().tenantId }),
	sessionIdentify: (ws) => ws.getUserData().sid
});

const member = mockWs({ id: 'alice' });
await presence.join(member, 'room', platform);
const conn = mockWs({ userId: 'u1', tenantId: 't', sid: 'app-1' });
conn.getUserData()[WS_SESSION_ID] = 's1';
await registry.hooks.open(conn, { platform });
await new Promise((resolve) => process.stdout.write('ready\n', resolve));
await new Promise((resolve) => process.stdin.once('data', resolve));

presence.hooks.close(member, { platform });
registry.hooks.close(conn, { platform });
setImmediate(() => process.exit(0));
