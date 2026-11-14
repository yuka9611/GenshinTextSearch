import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

// Replace the browser preference adapter; execute the real cache implementation.
const source = readFileSync(new URL('../src/utils/requestCache.js', import.meta.url), 'utf8')
  .replace(/import \{ getDisplayPreferencesFingerprint \}[^\n]+/, 'const getDisplayPreferencesFingerprint = () => globalThis.cacheTestPreferences || "default"')
const { withCache } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`)

test('anonymous endpoints cannot return another endpoint cached response', async () => {
  const texts = withCache(async () => ({ json: { 1: 'Chinese' } }))
  const voices = withCache(async () => ({ json: {} }))
  assert.deepEqual((await texts()).json, { 1: 'Chinese' })
  assert.deepEqual((await voices()).json, {})
})

test('simultaneous metadata loads share a request and rejected loads can retry', async () => {
  let calls = 0
  let complete
  const load = withCache(() => {
    calls++
    return new Promise((resolve, reject) => { complete = { resolve, reject } })
  })
  const first = load()
  const second = load()
  await Promise.resolve()
  assert.equal(calls, 1)
  complete.reject(new Error('network unavailable'))
  const results = await Promise.allSettled([first, second])
  assert.deepEqual(results.map((item) => item.status), ['rejected', 'rejected'])
  const retry = load()
  await Promise.resolve()
  assert.equal(calls, 2)
  complete.resolve({ json: ['language'] })
  await retry
  assert.deepEqual(await load(), { json: ['language'] })
  assert.equal(calls, 2)
})

test('pending and completed responses remain isolated by display preferences', async () => {
  let calls = 0
  const load = withCache(async () => ++calls)
  try {
    globalThis.cacheTestPreferences = 'Japanese'
    const japanese = load()
    globalThis.cacheTestPreferences = 'English'
    assert.equal(await load(), 2)
    assert.equal(await japanese, 1)
    globalThis.cacheTestPreferences = 'Japanese'
    assert.equal(await load(), 1)
  } finally {
    delete globalThis.cacheTestPreferences
  }
})
