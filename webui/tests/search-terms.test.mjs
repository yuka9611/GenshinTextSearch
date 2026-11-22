import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('../src/utils/searchTerms.js', import.meta.url), 'utf8')
const {
  normalizeSearchKeyword,
  splitSearchTerms,
  matchesSearchTerms,
  findHighlightRanges,
} = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`)

test('normalizeSearchKeyword strips balanced and unbalanced outer quotes', () => {
  assert.equal(normalizeSearchKeyword('"Ad abyssosque"'), 'Ad abyssosque')
  assert.equal(normalizeSearchKeyword('「天空 琴」'), '天空 琴')
  assert.equal(normalizeSearchKeyword('“星辰 深渊'), '星辰 深渊')
  assert.equal(normalizeSearchKeyword('   '), '')
})

test('splitSearchTerms handles ASCII spaces, fullwidth spaces, quotes, and dedupe', () => {
  assert.deepEqual(splitSearchTerms('天空  琴\t天空'), ['天空', '琴'])
  assert.deepEqual(splitSearchTerms('大型\u3000遺跡'), ['大型', '遺跡'])
  assert.deepEqual(splitSearchTerms('「Ad  abyssosque」'), ['Ad', 'abyssosque'])
  assert.deepEqual(splitSearchTerms('   '), [])
})

test('matchesSearchTerms requires every whitespace-separated term regardless of order', () => {
  assert.equal(matchesSearchTerms('向着星辰与深渊！', '深渊 星辰'), true)
  assert.equal(matchesSearchTerms("Welcome to the Adventurers' Guild", 'guild welcome'), true)
  assert.equal(matchesSearchTerms('こういう大型の遺跡', '大型\u3000遺跡'), true)
  assert.equal(matchesSearchTerms('向着星辰！', '深渊 星辰'), false)
  assert.equal(matchesSearchTerms('任意文本', '   '), true)
})

test('findHighlightRanges highlights multiple unordered terms and merges overlaps', () => {
  const line = 'Ad astra abyssosque! Welcome to the Guild.'
  const ranges = findHighlightRanges(line, 'guild ad')
  assert.deepEqual(
    ranges.map(([start, end]) => line.slice(start, end)),
    ['Ad', 'Guild'],
  )

  const lineZh = '向着星辰与深渊！'
  const rangesZh = findHighlightRanges(lineZh, '深渊 星辰')
  assert.deepEqual(
    rangesZh.map(([start, end]) => lineZh.slice(start, end)),
    ['星辰', '深渊'],
  )

  const exactLine = 'Ad astra ad astra'
  const merged = findHighlightRanges(exactLine, 'ad astra')
  assert.deepEqual(
    merged.map(([start, end]) => exactLine.slice(start, end)),
    ['Ad astra', 'ad astra'],
  )

  assert.deepEqual(findHighlightRanges('abc (def)', '(def) abc'), [[0, 3], [4, 9]])
  assert.deepEqual(findHighlightRanges('', 'abc'), [])
  assert.deepEqual(findHighlightRanges('abc', '   '), [])
})
