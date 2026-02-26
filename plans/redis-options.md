# Plan: Client constructs its own Redis connection

## Goal

Change the `Client` constructor so that instead of accepting a pre-built node-redis
connection as its first argument, it accepts an options object and constructs the
connection itself (calling either `createClient` or `createCluster` from the `redis`
package). This gives Queasy full control over connection lifecycle.

---

## 1. Options object design

The first argument changes from `RedisClient` to a `RedisOptions` object:

```ts
import type { RedisClientOptions, RedisClusterOptions } from 'redis';

type SingleNodeOptions = Pick<RedisClientOptions, 'url' | 'socket' | 'username' | 'password' | 'database'>;

type RedisOptions =
  | SingleNodeOptions
  | {
      rootNodes: SingleNodeOptions[];
      defaults?: Partial<SingleNodeOptions>;
      nodeAddressMap?: RedisClusterOptions['nodeAddressMap'];
    }
```

`RedisClientOptions` and `RedisClusterOptions` are both exported from the top-level
`redis` package and can be imported directly. `RedisClusterClientOptions` (the type
node-redis uses for `rootNodes` elements and `defaults`) is **not** exported at the
top level, so we define the cluster form inline, reusing `SingleNodeOptions` to
constrain both `rootNodes` elements and `defaults` to the same permitted fields as
the single-node case. `nodeAddressMap` is taken via an index type from
`RedisClusterOptions` directly to avoid depending on the unexported `NodeAddressMap`
type.

The dispatch rule: **if `options.rootNodes` exists, use `createCluster`; otherwise
use `createClient`.**

- If `options.rootNodes` is present: call `createCluster(options)`, passing the
  object through directly. The caller may also provide `defaults` (shared auth/TLS
  for all nodes) and `nodeAddressMap` — all standard `createCluster` options,
  named consistently with the node-redis API.
- Otherwise: call `createClient(options)`, passing the object through directly.
- The constructed connection is stored as `this.redis`.
- `connect()` is called internally in the constructor.
- `close()` calls `this.redis.destroy()` to disconnect.

### Constructor signature

All three arguments default so that `new Client()` works (connects to `localhost:6379`):

```js
constructor(options = {}, workerCount = os.cpus().length, callback = undefined)
```

---

## 2. Changes to `src/client.js`

1. Add imports:
   ```js
   import { createClient, createCluster } from 'redis';
   ```

2. Add a helper at module level:
   ```js
   function buildRedisConnection(options) {
     if (options.rootNodes) {
       return createCluster(options);
     }
     return createClient(options);
   }
   ```

3. Update constructor:
   - Parameter `redis` → `options` (with JSDoc type `RedisOptions`)
   - Replace `this.redis = redis;` with:
     ```js
     this.redis = buildRedisConnection(options);
     ```
   - Replace the bare `installLuaFunctions(this.redis).then(...)` call with:
     ```js
     this.redis.connect()
       .then(() => installLuaFunctions(this.redis))
       .then((disconnect) => { ... })
       .catch((err) => {
         this.disconnected = true;
         this.emit('disconnected', err.message);
       });
     ```
   - Apply defaults to all parameters:
     ```js
     constructor(options = {}, workerCount = os.cpus().length, callback)
     ```
     Add `import os from 'node:os';` (check if already imported; if so, reuse).

4. Update `close()`:
   - After `this.disconnected = true;`, add:
     ```js
     await this.redis.destroy().catch(() => {});
     ```
     `destroy()` is the documented disconnect method for both `RedisClientType`
     and `RedisClusterType` in node-redis v5.

5. Update JSDoc typedef:
   ```js
   /** @typedef {import('./types').RedisOptions} RedisOptions */
   ```
   Point to `types.ts` rather than duplicating the type inline in JS, since the
   full `Pick<>` construction is cleaner to express in TypeScript. Remove the old
   `RedisClient` typedef.

---

## 3. Changes to `src/types.ts`

Import the relevant node-redis types and define `RedisOptions` using `Pick<>` and
an inline cluster shape:

```ts
import type { RedisClientOptions, RedisClusterOptions } from 'redis';

type SingleNodeOptions = Pick<RedisClientOptions, 'url' | 'socket' | 'username' | 'password' | 'database'>;

export type RedisOptions =
  | SingleNodeOptions
  | {
      rootNodes: SingleNodeOptions[];
      defaults?: Partial<SingleNodeOptions>;
      nodeAddressMap?: RedisClusterOptions['nodeAddressMap'];
    }
```

`SingleNodeOptions` is not exported — it's an internal building block. Only
`RedisOptions` is exported. Update the `Client` constructor signature to accept
`RedisOptions` instead of `RedisClientType`.

---

## 4. Changes to `package.json`

- Move `redis` from `peerDependencies` + `devDependencies` to **`dependencies`**
  (queasy now owns the connection; callers no longer need to install it themselves).
- Remove the `peerDependencies` section entirely.

Before:
```json
"peerDependencies": { "redis": "^5.10.0" },
"devDependencies": { "redis": "^5.10.0", ... }
```

After:
```json
"dependencies": { "redis": "^5.10.0" },
"devDependencies": { ... }
```

---

## 5. Changes to `test/queue.test.js`

Currently each `beforeEach` creates and connects a Redis client, passes it to
`Client`, and each `afterEach` calls `redis.quit()`.

Changes:
- Remove the `createClient` import and the `redis` variable.
- Change `new Client(redis, 1)` → `new Client({}, 1)`.
- Remove `await redis.quit()` — `client.close()` now handles disconnection.
- Any direct Redis inspection calls (`redis.zScore`, `redis.hGetAll`, `redis.keys`,
  `redis.del`) still need a Redis connection. Add a dedicated `redisInspect` client
  used only for test-side assertions:
  ```js
  // beforeEach
  redisInspect = createClient();
  await redisInspect.connect();
  // afterEach
  await redisInspect.quit();
  ```

---

## 6. Changes to `test/client.test.js`

Same pattern as `queue.test.js`:
- Remove `createClient` import and `redis` variable.
- `new Client(redis, 1)` → `new Client({}, 1)`.
- Direct Redis calls (`redis.zScore`, `redis.keys`, `redis.del`) move to a
  separate `redisInspect` client.
- Remove `await redis.quit()` from `afterEach`; `client.close()` handles it.

---

## 7. Changes to `test/redis-functions.test.js`

This test file does **not** use the `Client` class — it builds its own `redis`
connection to test Lua functions directly. **No changes needed.**

---

## 8. Changes to `fuzztest/process.js`

Currently:
```js
import { createClient } from 'redis';
const redis = createClient();
await redis.connect();
const client = await new Promise((resolve) => new Client(redis, WORKER_THREADS, resolve));
```

Change to:
```js
const client = await new Promise((resolve) => new Client({}, WORKER_THREADS, resolve));
```

Remove the `import { createClient } from 'redis'` line and the three lines that
create and connect `redis`.

---

## 9. Changes to `Readme.md`

Update the `client()` API section:

**Before:**
```
### `client(redisConnection, workerCount)`
Returns a Queasy client.
- `redisConnection`: a node-redis connection object.
- `workerCount`: number; Size of the worker pool. ...
```

**After:**
```
### `new Client(options, workerCount)`
Returns a Queasy client. Queasy creates and manages its own Redis connection internally.
- `options`: connection options. Two forms are accepted:
  - **Single node** (plain object): passed to node-redis `createClient`. Accepts
    `url`, `socket`, `username`, `password`, and `database`. Defaults to `{}`
    (connects to `localhost:6379`).
  - **Cluster** (object with `rootNodes`): passed to node-redis `createCluster`.
    Accepts `rootNodes` (required — array of per-node connection options, at least
    three recommended), `defaults` (options shared across all nodes, e.g. auth and
    TLS), and `nodeAddressMap` (for address translation in NAT environments).
- `workerCount`: number; size of the worker pool. Defaults to number of CPUs.
```

Also update the terminology section to reflect that the client manages its own
connection rather than accepting one from the caller.

---

## 10. Changes to `CLAUDE.md`

In the Architecture section, update the JS layer description:
- "On construction, it calls `createClient` or `createCluster` (based on whether
  `options.rootNodes` is present), connects, then uploads the Lua script via
  `FUNCTION LOAD REPLACE`."
- Remove the mention of `WeakSet (initializedClients)` tracking which Redis clients
  have had functions loaded (no longer relevant since the connection is internal and
  only created once per `Client` instance).

---

## Summary of file changes

| File | Change |
|---|---|
| `src/client.js` | Accept `RedisOptions`; call `createClient`/`createCluster`; connect in constructor; `destroy()` in `close()`; emit `'disconnected'` on connect error; default all args |
| `src/types.ts` | Add `RedisOptions` type using `Pick<RedisClientOptions, ...> \| Pick<RedisClusterOptions, ...>`; remove `RedisClientType` reference |
| `package.json` | Move `redis` from `peerDependencies`+`devDependencies` to `dependencies` |
| `test/queue.test.js` | Remove external `redis` client; pass `{}` to `Client`; add `redisInspect` for assertions |
| `test/client.test.js` | Same as above |
| `test/redis-functions.test.js` | No changes |
| `fuzztest/process.js` | Remove `createClient`/`redis`; pass `{}` to `Client` |
| `Readme.md` | Update `client()` API docs |
| `CLAUDE.md` | Update architecture description |
