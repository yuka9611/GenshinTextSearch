import assert from "node:assert/strict";
import test from "node:test";

import worker from "../src/index.js";

function createEnv(handler) {
  return {
    PRIVATE_API: {
      fetch: handler,
    },
  };
}

test("proxies API requests through the VPC binding", async () => {
  let upstreamRequest;
  const env = createEnv(async (request) => {
    upstreamRequest = request;
    return Response.json({ status: "ok" });
  });
  const request = new Request(
    "https://api.example.workers.dev/api/startupStatus?probe=1",
    {
      headers: {
        Origin: "https://genshin-text-search.pages.dev",
        "CF-Connecting-IP": "192.0.2.8",
      },
    },
  );

  const response = await worker.fetch(request, env);

  assert.equal(response.status, 200);
  assert.equal(
    response.headers.get("Access-Control-Allow-Origin"),
    "https://genshin-text-search.pages.dev",
  );
  assert.equal(upstreamRequest.url, "http://127.0.0.1:5055/api/startupStatus?probe=1");
  assert.equal(upstreamRequest.headers.get("X-Forwarded-For"), "192.0.2.8");
  assert.equal(upstreamRequest.headers.get("X-Forwarded-Proto"), "https");
  assert.equal(upstreamRequest.headers.get("CF-Connecting-IP"), null);
});

test("handles allowed CORS preflight without reaching the VM", async () => {
  const env = createEnv(async () => {
    throw new Error("unexpected upstream call");
  });
  const request = new Request(
    "https://api.example.workers.dev/api/search",
    {
      method: "OPTIONS",
      headers: { Origin: "https://genshin-text-search.pages.dev" },
    },
  );

  const response = await worker.fetch(request, env);

  assert.equal(response.status, 204);
  assert.equal(response.headers.get("Access-Control-Allow-Methods"), "GET, HEAD, POST, OPTIONS");
});

test("rejects browser origins outside the Pages allowlist", async () => {
  let called = false;
  const env = createEnv(async () => {
    called = true;
    return new Response();
  });
  const response = await worker.fetch(
    new Request("https://api.example.workers.dev/api/startupStatus", {
      headers: { Origin: "https://attacker.example" },
    }),
    env,
  );

  assert.equal(response.status, 403);
  assert.equal(called, false);
});

test("does not proxy paths outside the API surface", async () => {
  let called = false;
  const env = createEnv(async () => {
    called = true;
    return new Response();
  });
  const response = await worker.fetch(
    new Request("https://api.example.workers.dev/admin"),
    env,
  );

  assert.equal(response.status, 404);
  assert.equal(called, false);
});
