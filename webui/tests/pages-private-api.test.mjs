import assert from "node:assert/strict"
import test from "node:test"

import { forwardToPrivateApi } from "../cloudflare/pagesPrivateApi.js"

test("forwards the original request through the private service binding", async () => {
  const request = new Request("https://genshin-text-search.pages.dev/api/startupStatus")
  let forwardedRequest

  const response = await forwardToPrivateApi({
    request,
    env: {
      database: {
        async fetch(nextRequest) {
          forwardedRequest = nextRequest
          return Response.json({ status: "ok" })
        },
      },
    },
  })

  assert.equal(forwardedRequest, request)
  assert.equal(response.status, 200)
  assert.deepEqual(await response.json(), { status: "ok" })
})

test("returns 503 when the production service binding is missing", async () => {
  const response = await forwardToPrivateApi({
    request: new Request("https://genshin-text-search.pages.dev/healthz"),
    env: {},
  })

  assert.equal(response.status, 503)
  assert.equal(response.headers.get("Cache-Control"), "no-store")
  assert.deepEqual(await response.json(), {
    status: "private_api_binding_unavailable",
  })
})

test("returns 502 without leaking binding errors", async () => {
  const originalConsoleError = console.error
  console.error = () => {}

  try {
    const response = await forwardToPrivateApi({
      request: new Request("https://genshin-text-search.pages.dev/api/startupStatus"),
      env: {
        database: {
          async fetch() {
            throw new Error("internal service details")
          },
        },
      },
    })

    assert.equal(response.status, 502)
    assert.deepEqual(await response.json(), {
      status: "private_api_unavailable",
    })
  } finally {
    console.error = originalConsoleError
  }
})
