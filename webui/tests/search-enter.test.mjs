import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('../src/utils/searchEnter.js', import.meta.url), 'utf8')
const { onSearchEnter } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`)

for (const [name, confirmation] of [
  ['active composition', { isComposing: true, keyCode: 13 }],
  ['composition ending before keydown', { isComposing: false, keyCode: 229 }],
]) {
  test(`IME Enter during ${name} stays silent; the next Enter searches once`, () => {
    let searches = 0
    const search = () => { searches++ }
    onSearchEnter(confirmation, search)
    assert.equal(searches, 0)
    onSearchEnter({ isComposing: false, keyCode: 13 }, search)
    assert.equal(searches, 1)
  })
}
