import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { get } from "node:http";
import { test } from "node:test";

async function startMock() {
  const child = spawn(
    process.execPath,
    [new URL("./server.mjs", import.meta.url).pathname, "--accounts", "0", "--quiet"],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  let output = "";
  const ready = new Promise((resolve, reject) => {
    child.on("error", reject);
    child.stderr.on("data", (chunk) => {
      output += chunk;
    });
    child.on("exit", (code) => reject(new Error(`mock exited ${code}: ${output}`)));
    child.stdout.on("data", (chunk) => {
      output += chunk;
      const match = /listening (http:\/\/[^ ]+)/.exec(output);
      if (match) resolve(match[1]);
    });
  });
  try {
    return { child, base: await ready };
  } catch (error) {
    child.kill();
    throw error;
  }
}

async function assertIdentities(read, apply, original) {
  const expectedProviders = structuredClone(original["api-keys"].claude);
  expectedProviders[0].keys[0].auth_index = "a stale live index";
  expectedProviders[0].keys[0]["auth-index"] = "also ignored";
  const providers = structuredClone(expectedProviders);
  providers[0].name = "renamed";
  providers.push({ "@id": "second-provider", name: "second", keys: [{ "api-key": "second-key" }] });
  await apply([
    { op: "set", path: "api-keys/claude", value: providers, expect: expectedProviders },
  ]);
  const current = (await read())["api-keys"].claude;
  assert.equal(current[0]["@id"], original["api-keys"].claude[0]["@id"]);
  assert.match(
    current[0].keys[0]["@id"],
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-8[0-9a-f]{3}-[0-9a-f]{12}$/,
  );
  assert.equal(current[0].keys[0]["@id"], original["api-keys"].claude[0].keys[0]["@id"]);
  assert.equal(current[0].keys[0]["api-key"], "$secret");
  assert.equal(current[0].keys[0].auth_index, original["api-keys"].claude[0].keys[0].auth_index);
  assert.equal(Object.hasOwn(current[0].keys[0], "auth-index"), false);
  await apply([
    { op: "set", path: "api-keys/claude", value: [...current].reverse(), expect: current },
  ]);
  assert.deepEqual((await read())["api-keys"].claude, [...current].reverse());
  await apply([{ op: "set", path: "model-routers", value: [{ name: "first", models: [] }] }]);
  const routers = (await read())["model-routers"];
  await apply([
    {
      op: "set",
      path: "model-routers",
      value: [{ ...routers[0], name: "renamed" }],
      expect: routers,
    },
  ]);
  assert.equal((await read())["model-routers"][0]["@id"], routers[0]["@id"]);
}

async function assertHiddenSecrets(read, apply) {
  await apply([
    { op: "set", path: "management/secret-key", value: "not-a-real-key", expect: "$secret" },
  ]);
  const management = (await read()).management;
  assert.equal(management["secret-key"], "$secret");
  assert.equal(
    (await apply([{ op: "set", path: "management", value: management, expect: management }]))
      .written,
    false,
  );
  await apply([
    {
      op: "set",
      path: "oauth/providers/codex/live-media-relay/ice-servers",
      value: [
        {
          urls: ["turn:example.invalid"],
          username: "private-user",
          credential: "private-password",
        },
      ],
    },
  ]);
  const oauth = (await read()).oauth;
  const server = oauth.providers.codex["live-media-relay"]["ice-servers"][0];
  assert.equal(server.username, "$secret");
  assert.equal(server.credential, "$secret");
  assert.equal(
    (await apply([{ op: "set", path: "oauth", value: oauth, expect: oauth }])).written,
    false,
  );
}

async function assertResourceRevisions(apply, get) {
  const client = (await get("/admin/v1/r/client-key")).items[0];
  await apply([
    {
      op: "patch",
      kind: "client-key",
      id: client.id,
      patch: { key: "rotated-secret" },
      if_rev: client.rev,
    },
  ]);
  const changed = await get(`/admin/v1/r/client-key/${client.id}`);
  assert.ok(changed.rev > client.rev);
  assert.equal(changed.created_rev, client.created_rev);
  assert.equal(changed.spec.key, "$secret");
  await apply(
    [
      {
        op: "patch",
        kind: "client-key",
        id: client.id,
        patch: { key: "stale" },
        if_rev: client.rev,
      },
    ],
    412,
  );
  await apply(
    [{ op: "put", kind: "client-key", id: client.id, spec: { key: "stale" }, if_rev: client.rev }],
    412,
  );
  const exported = async () =>
    (await get("/admin/v1/export?include_secrets=true")).resources.find(
      ({ kind, id }) => kind === "client-key" && id === client.id,
    );
  assert.equal((await exported()).spec.key, "rotated-secret");
  await apply([
    {
      op: "put",
      kind: "client-key",
      id: client.id,
      spec: { key: "replaced-secret" },
      if_rev: changed.rev,
    },
  ]);
  assert.deepEqual((await exported()).spec, { key: "replaced-secret" });
}

async function assertClientResources(read, apply, get) {
  const clients = (await get("/admin/v1/r/client-key")).items;
  assert.equal(clients.length, 2);
  assert.ok(clients.every((resource) => resource.spec.key === "$secret" && resource.rev > 0));
  assert.deepEqual(
    (await read()).access["api-keys"],
    clients.map(({ id }) => ({ "@id": id, key: "$secret" })),
  );
  assert.deepEqual(await get(`/admin/v1/r/client-key/${clients[0].id}`), clients[0]);
  await get("/admin/v1/r/client-key/missing", 404);
  const publicExport = await get("/admin/v1/export");
  const explicitPublic = await get("/admin/v1/export?include_secrets=false");
  assert.deepEqual(publicExport, explicitPublic);
  assert.equal(publicExport.format, "cooker-config");
  const clientExport = publicExport.resources.find(
    ({ kind, id }) => kind === "client-key" && id === clients[0].id,
  );
  assert.equal(clientExport.spec.key, "$secret");
  const plaintext = await get("/admin/v1/export?include_secrets=true");
  const originalKey = plaintext.resources.find(
    ({ kind, id }) => kind === "client-key" && id === clients[0].id,
  ).spec.key;
  assert.notEqual(originalKey, "$secret");
  assert.equal(
    publicExport.resources.find(({ kind }) => kind === "provider").spec.keys[0]["api-key"],
    "$secret",
  );
  await get("/admin/v1/export?include_secrets=1", 400);
  assert.equal(
    (await get("/v1/models", 200, { Authorization: `Bearer ${originalKey}` })).object,
    "list",
  );
  await get("/v1/models", 401, { Authorization: "Bearer $secret" });

  const reversed = [...(await read()).access["api-keys"]].reverse();
  await apply([{ op: "set", path: "access/api-keys", value: reversed }]);
  const reordered = (await get("/admin/v1/r/client-key")).items;
  assert.deepEqual(
    reordered.map(({ id }) => id),
    [...clients].reverse().map(({ id }) => id),
  );
  const revealed = await get("/admin/v1/export?include_secrets=true");
  assert.equal(
    revealed.resources.find(({ kind, id }) => kind === "client-key" && id === clients[0].id).spec
      .key,
    originalKey,
  );
  return clients;
}

async function assertClientResourceMutations(read, apply, get, clients) {
  const create = {
    op: "put",
    kind: "client-key",
    id: "new-client",
    spec: { key: "new-client-secret" },
    if_absent: true,
  };
  await apply([create], 200, { dry_run: true });
  await get("/admin/v1/r/client-key/new-client", 404);
  await apply([create]);
  await apply([create], 412);
  const added = await get("/admin/v1/r/client-key/new-client");
  assert.equal(added.spec.key, "$secret");
  assert.deepEqual((await read()).access["api-keys"].at(-1), { "@id": added.id, key: "$secret" });
  await apply([{ op: "delete", kind: "client-key", id: added.id, if_rev: added.rev - 1 }], 412);
  await apply(
    [
      { op: "delete", kind: "client-key", id: added.id, if_rev: added.rev },
      { op: "delete", kind: "client-key", id: clients[0].id, if_rev: 0 },
    ],
    412,
  );
  assert.deepEqual(await get(`/admin/v1/r/client-key/${added.id}`), added);
  await apply([{ op: "delete", kind: "client-key", id: added.id, if_rev: added.rev }]);
  await get(`/admin/v1/r/client-key/${added.id}`, 404);
}

async function assertProviderResources(read, apply, get) {
  const group = (await read())["api-keys"].claude[0];
  await apply([
    {
      op: "set",
      path: "api-keys/claude",
      value: [
        {
          ...group,
          keys: [...group.keys, { "api-key": "second-child-secret" }],
          headers: { "X-Custom": "private-header" },
          "base-url": "https://user:pass@example.invalid/v1?token=private",
        },
      ],
    },
  ]);
  const maskedGroup = (await read())["api-keys"].claude[0];
  assert.equal(maskedGroup.headers["X-Custom"], "$secret");
  assert.equal(maskedGroup["base-url"], "$secret");
  assert.ok(maskedGroup.keys.every((key) => key["api-key"] === "$secret"));
  const children = [...maskedGroup.keys].reverse();
  await apply([
    {
      op: "set",
      path: "api-keys/claude",
      value: [{ ...maskedGroup, keys: children }],
      expect: [maskedGroup],
    },
  ]);
  assert.deepEqual((await read())["api-keys"].claude[0].keys, children);
  const ambiguous = children.map((key) => ({ "api-key": key["api-key"] }));
  await apply(
    [{ op: "set", path: "api-keys/claude", value: [{ ...maskedGroup, keys: ambiguous }] }],
    400,
  );
  assert.deepEqual((await read())["api-keys"].claude[0].keys, children);
  const storedGroup = (await get("/admin/v1/export?include_secrets=true")).resources.find(
    ({ kind, id }) => kind === "provider" && id === group["@id"],
  ).spec;
  assert.equal(storedGroup.keys[0]["api-key"], "second-child-secret");
  assert.equal(storedGroup.headers["X-Custom"], "private-header");
  assert.equal(storedGroup["base-url"], "https://user:pass@example.invalid/v1?token=private");
  const usage = await get("/v8/management/observability/usage/api-keys");
  assert.deepEqual(
    Object.keys(usage.claude).sort(),
    children.map(({ auth_index }) => auth_index).sort(),
  );
  const providerResource = await get(`/admin/v1/r/provider/${group["@id"]}`);
  assert.equal(providerResource.spec.keys[0]["api-key"], "$secret");
  assert.equal(providerResource.spec.headers["X-Custom"], "$secret");
}

async function assertResources(read, apply, get) {
  const clients = await assertClientResources(read, apply, get);
  await assertClientResourceMutations(read, apply, get, clients);
  await assertResourceRevisions(apply, get);
  await assertProviderResources(read, apply, get);
}

async function assertAtomicApply(read, apply) {
  const original = await read();
  const failed = await apply(
    [
      {
        op: "set",
        path: "routing/strategy",
        value: "fill-first",
        expect: original.routing.strategy,
      },
      { op: "unset", path: "routing/session-affinity", expect: false },
    ],
    412,
  );
  assert.equal(failed.error, "precondition_failed");
  assert.deepEqual(await read(), original);
  const changed = await apply([
    { op: "set", path: "routing/strategy", value: "fill-first", expect: original.routing.strategy },
    { op: "set", path: "routing/strategy", value: "round-robin", expect: "fill-first" },
    { op: "unset", path: "routing/session-affinity", expect: true },
  ]);
  assert.equal(changed.revision, 2);
  assert.equal((await read()).routing.strategy, "round-robin");
  assert.equal(Object.hasOwn((await read()).routing, "session-affinity"), false);
  for (const expect of [null, [], {}]) {
    await apply([{ op: "unset", path: "routing/session-affinity", expect }]);
  }
  assert.equal((await apply([])).revision, 2);
  return original;
}

test(
  "admin mock applies atomic preconditions and keeps resource identities",
  { timeout: 15_000 },
  async () => {
    const { child, base } = await startMock();
    const headers = { Authorization: "Bearer mock", "Content-Type": "application/json" };
    const read = async () => {
      const response = await fetch(`${base}/admin/v1/effective/config`, { headers });
      assert.equal(response.status, 200);
      return response.json();
    };
    const apply = async (ops, expected = 200, extra = {}) => {
      const response = await fetch(`${base}/admin/v1/apply`, {
        method: "POST",
        headers,
        body: JSON.stringify({ ops, ...extra }),
      });
      assert.equal(response.status, expected, await response.clone().text());
      const body = await response.json();
      if (expected === 200)
        assert.equal(response.headers.get("x-ck-revision"), String(body.revision));
      return body;
    };
    const get = async (path, expected = 200, extraHeaders = {}) => {
      const response = await fetch(`${base}${path}`, { headers: { ...headers, ...extraHeaders } });
      assert.equal(response.status, expected, await response.clone().text());
      return response.json();
    };
    try {
      const original = await assertAtomicApply(read, apply);

      await assertIdentities(read, apply, original);
      await assertHiddenSecrets(read, apply);
      await assertResources(read, apply, get);

      const current = await read();
      await apply([{ op: "set", path: "routing/strategy", value: "fill-first" }], 200, {
        dry_run: true,
      });
      assert.deepEqual(await read(), current);
      await apply(
        [
          { op: "set", path: "routing/strategy", value: "fill-first" },
          { op: "set", path: "credentials/concurrency/lifecycle-config-revision", value: 99 },
        ],
        400,
      );
      assert.deepEqual(await read(), current);
      await apply([{ op: "unset", path: "" }], 400);
      assert.deepEqual(await read(), current);

      for (const path of ["config", "config/routing/strategy", "config.yaml"]) {
        for (const method of ["GET", "PUT", "PATCH", "DELETE"]) {
          assert.equal(
            (await fetch(`${base}/v8/management/${path}`, { method, headers })).status,
            404,
          );
        }
      }
      assert.equal(
        (await fetch(`${base}/v8/management/effective/config`, { headers })).status,
        404,
      );
      assert.equal((await fetch(`${base}/admin/v1/credentials`, { headers })).status, 404);
      assert.equal((await fetch(`${base}/v8/management/credentials`, { headers })).status, 200);
    } finally {
      const exited = once(child, "exit");
      child.kill();
      await exited;
    }
  },
);

test("mock configuration and CORS match management responses", { timeout: 15_000 }, async (t) => {
  const { child, base } = await startMock();
  try {
    await t.test("effective config omits the removed OAuth auth directory", async () => {
      const response = await fetch(`${base}/admin/v1/effective/config`, {
        headers: { Authorization: "Bearer mock" },
      });
      assert.equal(response.status, 200);
      const config = await response.json();
      assert.equal(Object.hasOwn(config.oauth, "auth-dir"), false);
    });
    await t.test("preflight and regular responses expose management headers", async () => {
      const exposed = [
        "X-Ck-Trace-Id",
        "X-Ck-Version",
        "X-Ck-Commit",
        "X-Ck-Build-Date",
        "X-SERVER-VERSION",
        "X-SERVER-BUILD-DATE",
        "Location",
        "Retry-After",
        "ETag",
        "X-Request-Id",
        "OpenAI-Request-Id",
      ];
      for (const [path, extra] of [
        ["/v8/management/credentials", []],
        ["/admin/v1/effective/config", ["X-Ck-Revision", "X-Ck-Published-Revision"]],
      ]) {
        for (const method of ["OPTIONS", "GET"]) {
          const response = await new Promise((resolve, reject) => {
            get(
              `${base}${path}`,
              {
                method,
                headers: {
                  Authorization: "Bearer mock",
                  Origin: "http://dashboard.example.invalid",
                },
              },
              resolve,
            ).on("error", reject);
          });
          response.resume();
          assert.equal(response.statusCode, method === "OPTIONS" ? 204 : 200);
          assert.equal(response.headers["access-control-allow-origin"], "*");
          assert.deepEqual(
            response.headersDistinct["access-control-expose-headers"],
            method === "OPTIONS" || extra.length === 0
              ? [exposed.join(", ")]
              : [exposed.join(", "), extra.join(", ")],
          );
        }
      }
    });
  } finally {
    const exited = once(child, "exit");
    child.kill();
    await exited;
  }
});
