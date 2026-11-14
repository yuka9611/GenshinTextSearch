import assert from 'node:assert/strict'
import test from 'node:test'
import { readFileSync } from 'node:fs'
import { compileScript, parse } from 'vue/compiler-sfc'
import { createRenderer, defineComponent, h, KeepAlive, nextTick, ref, watch } from 'vue'
import useSearchHistoryReplay from '../src/composables/useSearchHistoryReplay.js'
import {
  normalizeSearchHistoryReplay,
  queueSearchHistoryReplay,
  takeSearchHistoryReplay,
} from '../src/utils/searchHistoryReplay.js'

for (const searchType of ['text', 'name', 'npc_dialogue', 'voice', 'story', 'catalog']) {
  test(`${searchType} history preserves language and condition-only searches`, () => {
    const languageKey = ['voice', 'story'].includes(searchType) ? 'searchLang' : 'langCode'
    const entry = {
      search_type: searchType,
      keyword: '',
      filters: { [languageKey]: 9, createdVersion: '7.2', sourceTypeCode: 0 },
    }
    assert.equal(queueSearchHistoryReplay(entry), true)
    assert.deepEqual(takeSearchHistoryReplay(searchType), {
      searchType,
      keyword: '',
      language: '9',
      filters: { [languageKey]: '9', createdVersion: '7.2', sourceTypeCode: '0' },
    })
    assert.equal(takeSearchHistoryReplay(searchType), null, 'activation consumes a replay once')
  })
}

test('rejects unknown search types and ignores invalid language and structured filter values', () => {
  assert.equal(queueSearchHistoryReplay({ search_type: '__proto__' }), false)
  assert.equal(normalizeSearchHistoryReplay(null), null)
  assert.deepEqual(normalizeSearchHistoryReplay({
    search_type: 'text', keyword: '派蒙',
    filters: { langCode: 'invalid', speaker: ['unexpected'], sourceType: { name: 'quest' }, createdVersion: '7.2' },
  }), {
    searchType: 'text', keyword: '派蒙', language: null,
    filters: { langCode: 'invalid', createdVersion: '7.2' },
  })
})

test('latest queued record replaces an older selection without crossing search types', () => {
  queueSearchHistoryReplay({ search_type: 'text', keyword: 'old' })
  queueSearchHistoryReplay({ search_type: 'name', keyword: 'other type' })
  queueSearchHistoryReplay({ search_type: 'text', keyword: 'new' })
  assert.equal(takeSearchHistoryReplay('text').keyword, 'new')
  assert.equal(takeSearchHistoryReplay('name').keyword, 'other type')
})

// A minimal Vue renderer exercises real KeepAlive lifecycle and watcher timing
// without requiring a browser or duplicating Vue's scheduling logic in a mock.
const createHost = () => {
  const node = (type, text = '') => ({ type, text, children: [], parent: null })
  const remove = (child) => {
    if (!child.parent) return
    const siblings = child.parent.children
    const index = siblings.indexOf(child)
    if (index >= 0) siblings.splice(index, 1)
    child.parent = null
  }
  return { node, renderer: createRenderer({
    createElement: node, createText: (text) => node('text', text), createComment: (text) => node('comment', text),
    setText: (target, text) => { target.text = text },
    setElementText: (target, text) => { target.text = text; target.children = [] },
    patchProp() {}, remove,
    insert(child, parent, anchor = null) {
      remove(child)
      const index = anchor ? parent.children.indexOf(anchor) : -1
      parent.children.splice(index < 0 ? parent.children.length : index, 0, child)
      child.parent = parent
    },
    parentNode: (target) => target.parent,
    nextSibling: (target) => target.parent?.children[target.parent.children.indexOf(target) + 1] || null,
  }) }
}
const flush = async () => { await nextTick(); await new Promise((resolve) => setImmediate(resolve)) }

test('KeepAlive replay waits for defaults, suppresses filter watchers, and reruns on a later click only', async () => {
  const { renderer, node } = createHost()
  const visible = ref(true)
  const language = ref('1')
  const version = ref('')
  let finishInitialization
  const ready = new Promise((resolve) => { finishInitialization = () => { language.value = '1'; resolve() } })
  const requests = []
  const Search = defineComponent({
    setup() {
      const { restoringHistory } = useSearchHistoryReplay({
        searchType: 'text', ready: () => ready,
        restore(replay) { language.value = replay.language; version.value = replay.filters.createdVersion || '' },
        search() { requests.push({ language: language.value, version: version.value }) },
      })
      watch(version, () => {
        if (!restoringHistory.value) requests.push({ unexpectedWatcherSearch: true })
      })
      return () => h('div', 'search')
    },
  })
  const Other = defineComponent({ setup: () => () => h('div', 'history') })
  const app = renderer.createApp({ render: () => h(KeepAlive, null, { default: () => visible.value ? h(Search) : h(Other) }) })
  queueSearchHistoryReplay({ search_type: 'text', keyword: '', filters: { langCode: 9, createdVersion: '7.2' } })
  app.mount(node('root'))
  await flush()
  assert.deepEqual(requests, [], 'must not query before initialization finishes')
  finishInitialization()
  await flush()
  assert.deepEqual(requests, [{ language: '9', version: '7.2' }])
  visible.value = false
  await flush()
  visible.value = true
  await flush()
  assert.equal(requests.length, 1, 'ordinary tab return must not repeat a consumed record')
  visible.value = false
  await flush()
  queueSearchHistoryReplay({ search_type: 'text', keyword: '', filters: { langCode: 4, createdVersion: '7.1' } })
  visible.value = true
  await flush()
  assert.deepEqual(requests, [{ language: '9', version: '7.2' }, { language: '4', version: '7.1' }])
  app.unmount()
})

test('leaving before initialization completes cancels a stale history search', async () => {
  const { renderer, node } = createHost()
  const visible = ref(true)
  let finishInitialization
  const ready = new Promise((resolve) => { finishInitialization = resolve })
  let searches = 0
  const Search = defineComponent({
    setup() {
      useSearchHistoryReplay({
        searchType: 'text', ready: () => ready,
        restore() {}, search() { searches += 1 },
      })
      return () => h('div')
    },
  })
  const Other = defineComponent({ setup: () => () => h('div') })
  const app = renderer.createApp({ render: () => h(KeepAlive, null, { default: () => visible.value ? h(Search) : h(Other) }) })
  queueSearchHistoryReplay({ search_type: 'text', keyword: 'old' })
  app.mount(node('root'))
  await flush()
  visible.value = false
  await flush()
  finishInitialization()
  await flush()
  assert.equal(searches, 0)
  queueSearchHistoryReplay({ search_type: 'text', keyword: 'new' })
  visible.value = true
  await flush()
  assert.equal(searches, 1)
  app.unmount()
})

test('real VersionFilter prop watchers cannot trigger extra searches during history restoration', async () => {
  const source = readFileSync(new URL('../src/components/VersionFilter.vue', import.meta.url), 'utf8')
  const { descriptor } = parse(source)
  const compiled = compileScript(descriptor, { id: 'history-version-filter-regression' }).content
    .replace(/from ['"]vue['"]/g, `from ${JSON.stringify(import.meta.resolve('vue'))}`)
  const { default: VersionFilter } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)
  VersionFilter.render = () => h('div')

  for (const searchType of ['text', 'catalog']) {
    const { renderer, node } = createHost()
    const created = ref('')
    const updated = ref('')
    const requests = []
    let childSearchEmits = 0
    const Search = defineComponent({
      setup() {
        const handleSearch = () => {
          if (restoringHistory.value) return
          requests.push({ created: created.value, updated: updated.value })
        }
        const { restoringHistory } = useSearchHistoryReplay({
          searchType, ready: async () => {},
          restore(replay) {
            created.value = replay.filters.createdVersion || ''
            updated.value = replay.filters.updatedVersion || ''
          },
          search: handleSearch,
        })
        return () => h(VersionFilter, {
          createdVersion: created.value,
          updatedVersion: updated.value,
          onSearch() { childSearchEmits += 1; handleSearch() },
        })
      },
    })
    queueSearchHistoryReplay({
      search_type: searchType, keyword: 'test',
      filters: { createdVersion: '7.1', updatedVersion: '7.2' },
    })
    const app = renderer.createApp({ render: () => h(KeepAlive, null, { default: () => h(Search) }) })
    app.mount(node('root'))
    await flush()
    assert.equal(childSearchEmits, 2, `${searchType}: exercise both actual child prop-to-local watchers`)
    assert.deepEqual(requests, [{ created: '7.1', updated: '7.2' }], `${searchType}: query only once after restoring both filters`)
    app.unmount()
  }
})
