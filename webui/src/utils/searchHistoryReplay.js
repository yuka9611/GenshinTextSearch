const pendingReplays = new Map()
const supportedTypes = new Set(['text', 'name', 'npc_dialogue', 'voice', 'story', 'catalog'])

// A history record is data, so only scalar filter values reach the search form.
export const normalizeSearchHistoryReplay = (entry) => {
  if (!supportedTypes.has(entry?.search_type)) return null
  const filters = Object.fromEntries(Object.entries(entry.filters || {}).filter(([, value]) => (
    typeof value === 'string' || (typeof value === 'number' && Number.isFinite(value))
  )).map(([key, value]) => [key, String(value)]))
  const language = filters.langCode ?? filters.searchLang
  return {
    searchType: entry.search_type,
    keyword: String(entry.keyword ?? ''),
    language: language !== undefined && /^\d+$/.test(language) ? language : null,
    filters,
  }
}

export const queueSearchHistoryReplay = (entry) => {
  const replay = normalizeSearchHistoryReplay(entry)
  if (!replay) return false
  pendingReplays.set(replay.searchType, replay)
  return true
}

export const takeSearchHistoryReplay = (searchType) => {
  const replay = pendingReplays.get(searchType)
  pendingReplays.delete(searchType)
  return replay || null
}
