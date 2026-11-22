import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('../src/composables/useSearch.js', import.meta.url), 'utf8')
  .replace("from 'vue'", `from '${import.meta.resolve('vue')}'`)
  .replace(/import api[^\n]+/, 'const api = globalThis.searchInitializationHarness.api')
  .replace(/import useLanguage[^\n]+/, 'const useLanguage = () => globalThis.searchInitializationHarness.language')
  .replace(/import useVersion[^\n]+/, 'const useVersion = () => globalThis.searchInitializationHarness.versions')
  .replace(/import \{ matchVersionFilters \}[^\n]+/, 'const matchVersionFilters = () => true')

test('first search waits for configured language without waiting for version options', async () => {
  const { ref } = await import('vue')
  let finishLanguages
  const ready = new Promise((resolve) => { finishLanguages = resolve })
  let finishVersions
  const versionsReady = new Promise((resolve) => { finishVersions = resolve })
  let versionsFinished = false
  const selected = ref('1')
  const requests = []
  globalThis.searchInitializationHarness = {
    api: { queryByKeyword: async (...args) => {
      requests.push(args)
      return { json: { contents: [], total: 0, time: 1 } }
    } },
    language: {
      selectedInputLanguage: selected, supportedInputLanguage: ref({}),
      loadLanguages: async () => { await ready; selected.value = '9' },
    },
    versions: {
      versionOptions: ref([]), createdVersionOptions: ref([]), updatedVersionOptions: ref([]),
      loadVersionOptions: async () => { await versionsReady; versionsFinished = true },
    },
  }
  try {
    const { default: useSearch } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`)
    const search = useSearch()
    search.keyword.value = 'アルハイゼン'
    const firstSearch = search.onQueryButtonClicked()
    await Promise.resolve()
    assert.equal(requests.length, 0)
    finishLanguages()
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(requests.length, 1, 'a pending version-options request does not block a ready search')
    await firstSearch
    assert.equal(requests.length, 1)
    assert.equal(requests[0][1], 9)
    assert.equal(requests[0][4], 20)
    assert.equal(search.isLoading.value, false)
    assert.equal(versionsFinished, false)
  } finally {
    finishVersions()
    delete globalThis.searchInitializationHarness
  }
})

test('version metadata requests retain their timeout without opening the blocking loading screen', async () => {
  const requests = []
  globalThis.versionMetadataHarness = { get: (...args) => requests.push(args) }
  const source = readFileSync(new URL('../src/api/basicInfo.js', import.meta.url), 'utf8')
    .replace(/import request[^\n]+/, 'const request = globalThis.versionMetadataHarness')
    .replace(/import \{ withCache \}[^\n]+/, 'const withCache = (fn) => fn')
  try {
    const { default: api } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`)
    api.getAvailableVersionFilters()
    api.getAvailableVersions()
    assert.deepEqual(requests, [
      ['/api/getAvailableVersionFilters', { timeout: 30000, doNotShowLoadingScreen: true }],
      ['/api/getAvailableVersions', { timeout: 30000, doNotShowLoadingScreen: true }],
    ])
  } finally {
    delete globalThis.versionMetadataHarness
  }
})
