const QUOTE_PAIRS = [
  ['"', '"'],
  ["'", "'"],
  ['“', '”'],
  ['‘', '’'],
  ['「', '」'],
  ['『', '』'],
  ['《', '》'],
  ['〈', '〉'],
  ['«', '»'],
  ['‹', '›'],
]

function countOccurrences(text, needle) {
  let count = 0
  let index = 0
  while (index < text.length) {
    const found = text.indexOf(needle, index)
    if (found === -1) break
    count += 1
    index = found + needle.length
  }
  return count
}

function escapeRegExp(text) {
  return String(text || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

export function normalizeSearchKeyword(keyword) {
  let text = String(keyword || '').trim()
  if (!text) return ''

  while (true) {
    const previous = text
    let matchedOuterPair = false

    for (const [left, right] of QUOTE_PAIRS) {
      if (text.length <= left.length + right.length) continue
      if (text.startsWith(left) && text.endsWith(right)) {
        const inner = text.slice(left.length, text.length - right.length).trim()
        if (inner) {
          text = inner
          matchedOuterPair = true
          break
        }
      }
    }

    if (!matchedOuterPair) {
      let stripped = false
      for (const [left, right] of QUOTE_PAIRS) {
        if (text.startsWith(left)) {
          const inner = text.slice(left.length).trim()
          const isUnbalancedLeft =
            (left === right && countOccurrences(text, left) === 1) ||
            (left !== right && !text.slice(left.length).includes(right))
          if (inner && isUnbalancedLeft) {
            text = inner
            stripped = true
            break
          }
        }
        if (text.endsWith(right)) {
          const inner = text.slice(0, text.length - right.length).trim()
          const isUnbalancedRight =
            (left === right && countOccurrences(text, right) === 1) ||
            (left !== right && !text.slice(0, text.length - right.length).includes(left))
          if (inner && isUnbalancedRight) {
            text = inner
            stripped = true
            break
          }
        }
      }
      if (!stripped) return text
    }

    if (text === previous) return text
  }
}

export function splitSearchTerms(keyword) {
  const raw = String(keyword || '').trim()
  if (!raw) return []

  const normalized = normalizeSearchKeyword(raw) || raw
  const seen = new Set()
  const terms = []
  for (const part of normalized.split(/\s+/)) {
    const trimmed = part.trim()
    if (!trimmed || seen.has(trimmed)) continue
    seen.add(trimmed)
    terms.push(trimmed)
  }
  return terms
}

export function matchesSearchTerms(text, keyword) {
  const terms = splitSearchTerms(keyword)
  if (terms.length === 0) return true
  const haystack = String(text || '').toLowerCase()
  return terms.every((term) => haystack.includes(term.toLowerCase()))
}

export function findHighlightRanges(line, keyword) {
  const rawLine = String(line || '')
  if (!rawLine || !keyword) return []

  const rawKeyword = String(keyword || '').trim()
  if (!rawKeyword) return []

  const normalized = normalizeSearchKeyword(rawKeyword) || rawKeyword
  const terms = splitSearchTerms(rawKeyword)
  if (terms.length === 0) return []

  const needles = []
  const seenNeedles = new Set()
  for (const candidate of [normalized, ...terms]) {
    const lower = candidate.toLowerCase()
    if (!lower || seenNeedles.has(lower)) continue
    seenNeedles.add(lower)
    needles.push(candidate)
  }

  const rawRanges = []
  for (const needle of needles) {
    const regex = new RegExp(escapeRegExp(needle), 'gi')
    for (const match of rawLine.matchAll(regex)) {
      if (match.index === undefined || match[0].length === 0) continue
      rawRanges.push([match.index, match.index + match[0].length])
    }
  }

  if (rawRanges.length === 0) return []

  rawRanges.sort((a, b) => (a[0] !== b[0] ? a[0] - b[0] : b[1] - a[1]))

  const merged = []
  for (const [start, end] of rawRanges) {
    if (merged.length === 0) {
      merged.push([start, end])
      continue
    }
    const last = merged[merged.length - 1]
    if (start <= last[1]) {
      last[1] = Math.max(last[1], end)
    } else {
      merged.push([start, end])
    }
  }
  return merged
}
