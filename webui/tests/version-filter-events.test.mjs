import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import { compileScript, parse } from 'vue/compiler-sfc'
import { createRenderer, defineComponent, h, nextTick, ref } from 'vue'

test('version prop synchronization stays silent and each user change searches once', async () => {
  const { descriptor } = parse(readFileSync(new URL('../src/components/VersionFilter.vue', import.meta.url), 'utf8'))
  const compiled = compileScript(descriptor, { id: 'version-filter-events', inlineTemplate: true }).content
    .replace(/from ['"]vue['"]/g, `from ${JSON.stringify(import.meta.resolve('vue'))}`)
  const { default: VersionFilter } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`)
  const node = (type) => ({ type, children: [], parent: null })
  const remove = (child) => {
    if (!child.parent) return
    const siblings = child.parent.children
    siblings.splice(siblings.indexOf(child), 1)
    child.parent = null
  }
  const renderer = createRenderer({
    createElement: node, createText: node, createComment: node,
    setText() {}, setElementText() {}, patchProp() {}, remove,
    insert(child, parent, anchor = null) {
      remove(child)
      const index = anchor ? parent.children.indexOf(anchor) : -1
      parent.children.splice(index < 0 ? parent.children.length : index, 0, child)
      child.parent = parent
    },
    parentNode: (target) => target.parent,
    nextSibling: (target) => target.parent?.children[target.parent.children.indexOf(target) + 1] || null,
  })
  const selects = []
  const Select = defineComponent({
    props: ['modelValue'], emits: ['update:modelValue', 'change'],
    setup(props, { emit }) {
      selects.push({ props, emit })
      return () => h('select')
    },
  })
  const created = ref('7.1')
  const updated = ref('7.2')
  const searches = []
  const app = renderer.createApp({
    render: () => h(VersionFilter, {
      createdVersion: created.value, updatedVersion: updated.value,
      'onUpdate:createdVersion': (value) => { created.value = value },
      'onUpdate:updatedVersion': (value) => { updated.value = value },
      onSearch: () => searches.push([created.value, updated.value]),
    }),
  })
  app.component('el-select', Select)
  app.component('el-option', { render: () => h('option') })
  app.mount(node('root'))
  created.value = ''
  updated.value = ''
  await nextTick()
  assert.deepEqual(selects.map(({ props }) => props.modelValue), ['', ''])
  assert.deepEqual(searches, [], 'clearing parent filters must not emit searches')
  for (const [index, value] of ['7.2', '7.3'].entries()) {
    selects[index].emit('update:modelValue', value)
    selects[index].emit('change', value)
    await nextTick()
    assert.equal(searches.length, index + 1)
  }
  assert.deepEqual(searches, [['7.2', ''], ['7.2', '7.3']])
  app.unmount()
})
