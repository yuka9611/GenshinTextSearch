import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import { ref } from 'vue'

const source = readFileSync(new URL('../src/composables/useVersion.js', import.meta.url), 'utf8')
  .replace(/^import .*$/gm, '')
  .replace('export default useVersion', 'return useVersion')
const loadVersion = new Function('ref', 'basicInfoApi', 'console', source)
const createVersionLoader = (responses) => {
  const calls = []
  const errors = []
  const useVersion = loadVersion(ref, {
    async getAvailableVersionFilters() {
      calls.push('filters')
      const next = responses.shift()
      if (next.error) throw next.error
      return next
    },
    async getAvailableVersions() { calls.push('legacy'); return { json: ['7.0'] } },
  }, { error: (...args) => errors.push(args) })
  return { versions: useVersion(), calls, errors }
}
const filters = (version) => ({ json: { created: [version], updated: [version] } })

test('only explicitly unsupported version endpoints use the legacy fallback', async () => {
  for (const error of [
    { error: { response: { status: 404 } } },
    { response: { status: 405 } },
    { errorCode: 404 },
  ]) {
    const { versions, calls } = createVersionLoader([{ error }])
    await versions.loadVersionOptions()
    assert.deepEqual(calls, ['filters', 'legacy'])
    assert.deepEqual(versions.versionOptions.value, ['7.0'])
  }
})

test('timeouts and server errors retain loaded options, skip fallback, and allow a later retry', async () => {
  for (const error of [
    { network: true, error: { code: 'ECONNABORTED', message: 'timeout of 30000ms exceeded' } },
    { network: true, error: { response: { status: 500 } } },
    { errorCode: 400 },
  ]) {
    const { versions, calls } = createVersionLoader([filters('7.1'), { error }, filters('7.2')])
    await versions.loadVersionOptions()
    await versions.loadVersionOptions()
    assert.deepEqual(calls, ['filters', 'filters'])
    assert.deepEqual(versions.versionOptions.value, ['7.1'])
    assert.deepEqual(versions.createdVersionOptions.value, ['7.1'])
    assert.deepEqual(versions.updatedVersionOptions.value, ['7.1'])
    await versions.loadVersionOptions()
    assert.deepEqual(calls, ['filters', 'filters', 'filters'])
    assert.deepEqual(versions.versionOptions.value, ['7.2'])
  }
})
