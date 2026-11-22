import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import { ref, computed } from 'vue'

const deferred = () => {
  let resolve, reject
  const promise = new Promise((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
const flush = () => new Promise((resolve) => setImmediate(resolve))
const source = readFileSync(new URL('../src/composables/useSearch.js', import.meta.url), 'utf8')
  .replace(/^import .*$/gm, '')
  .replace('export default useSearch', 'return useSearch')
const loadSearch = new Function('ref', 'computed', 'api', 'useLanguage', 'useVersion', 'matchVersionFilters', 'console', source)

const createSearch = async (ready = Promise.resolve(), versionsReady = Promise.resolve()) => {
  const requests = []
  const harness = {
    api: { queryByKeyword: (...args) => {
      const request = { ...deferred(), args }
      requests.push(request)
      return request.promise
    } },
    language: {
      selectedInputLanguage: ref('1'), supportedInputLanguage: ref({}),
      loadLanguages: () => ready,
    },
    versions: {
      versionOptions: ref([]), createdVersionOptions: ref([]), updatedVersionOptions: ref([]),
      loadVersionOptions: () => versionsReady,
    },
  }
  // Keep product diagnostics inside the harness. Bursts of console records can
  // corrupt Node 26's child-test IPC stream; errors remain observable assertions.
  const diagnostics = { errors: [] }
  const capturedConsole = {
    log() {},
    error: (...args) => diagnostics.errors.push(args),
  }
  const useSearch = loadSearch(ref, computed, harness.api, () => harness.language, () => harness.versions, () => true, capturedConsole)
  const search = useSearch()
  return { search, requests, diagnostics }
}
const response = (hash) => ({ json: { contents: [{ hash }], total: 1, time: 1 } })

for (const oldOutcome of ['success', 'failure']) {
  test(`old request ${oldOutcome} cannot replace newer results`, async () => {
    const { search, requests, diagnostics } = await createSearch()
    search.keyword.value = 'old'
    const old = search.onQueryButtonClicked()
    await flush()
    search.keyword.value = 'new'
    const latest = search.onQueryButtonClicked()
    await flush()
    assert.equal(requests[0].args.at(-1).signal.aborted, true)
    assert.equal(requests[1].args.at(-1).signal.aborted, false)
    requests[1].resolve(response('new'))
    await latest
    if (oldOutcome === 'success') requests[0].resolve(response('old'))
    else requests[0].reject(new Error('timeout of 30000ms exceeded'))
    await old
    assert.deepEqual(search.queryResult.value, [{ hash: 'new' }])
    assert.equal(search.totalCount.value, 1)
    assert.equal(search.keywordLast.value, 'new')
    assert.equal(search.isLoading.value, false)
    assert.deepEqual(diagnostics.errors, [], 'superseded requests do not report search errors')
  })
}

test('old completion cannot end the loading state of a newer request', async () => {
  const { search, requests } = await createSearch()
  const old = search.onQueryButtonClicked()
  await flush()
  const latest = search.onQueryButtonClicked()
  await flush()
  requests[0].resolve(response('old'))
  await old
  assert.equal(search.isLoading.value, true)
  assert.deepEqual(search.queryResult.value, [])
  requests[1].resolve(response('new'))
  await latest
  assert.equal(search.isLoading.value, false)
})

test('reentry while initialization waits sends only the latest search', async () => {
  const initialization = deferred()
  const { search, requests } = await createSearch(initialization.promise)
  search.keyword.value = 'old'
  const old = search.onQueryButtonClicked()
  search.keyword.value = 'new'
  const latest = search.onQueryButtonClicked()
  await flush()
  assert.equal(requests.length, 0)
  initialization.resolve()
  await flush()
  assert.equal(requests.length, 1)
  assert.equal(requests[0].args[0], 'new')
  assert.equal(search.isLoading.value, true)
  requests[0].resolve(response('new'))
  await Promise.all([old, latest])
  assert.equal(search.isLoading.value, false)
})

test('background version rejection is handled without failing a ready search', async () => {
  const versions = deferred()
  const { search, requests, diagnostics } = await createSearch(Promise.resolve(), versions.promise)
  const failure = new Error('version metadata unavailable')
  versions.reject(failure)
  await flush()
  assert.deepEqual(diagnostics.errors, [[failure]])
  const query = search.onQueryButtonClicked()
  await flush()
  requests[0].resolve(response('ready'))
  await query
  assert.deepEqual(search.queryResult.value, [{ hash: 'ready' }])
})
