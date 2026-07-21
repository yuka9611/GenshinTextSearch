import { isSupabaseConfigured, supabase } from '@/services/supabase'

const LOCAL_PREFERENCES_KEY = 'gts-user-preferences-v1'
const LOCAL_HISTORY_KEY = 'gts-user-search-history-v1'
const MAX_LOCAL_HISTORY = 100

export const SEARCH_TYPE_META = Object.freeze({
  text: { label: '文本检索', path: '/' },
  name: { label: '任务/阅读物查询', path: '/name-search' },
  npc_dialogue: { label: 'NPC 对话查询', path: '/npc-dialogue-search' },
  voice: { label: '角色语音查询', path: '/voice-search' },
  story: { label: '角色故事查询', path: '/story-search' },
  catalog: { label: '图鉴搜索', path: '/catalog-search' },
})

const SEARCH_ENDPOINTS = Object.freeze({
  '/api/keywordQuery': 'text',
  '/api/nameSearch': 'name',
  '/api/npcDialogueSearch': 'npc_dialogue',
  '/api/avatarVoiceSearch': 'voice',
  '/api/avatarStorySearch': 'story',
  '/api/catalogSearch': 'catalog',
})

const safeJsonParse = (raw, fallback) => {
  try {
    return raw ? JSON.parse(raw) : fallback
  } catch {
    return fallback
  }
}

const readLocal = (key, fallback) => {
  if (typeof window === 'undefined') return fallback
  return safeJsonParse(window.localStorage.getItem(key), fallback)
}

const writeLocal = (key, value) => {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(key, JSON.stringify(value))
  } catch {
    // A full or disabled localStorage must not break search.
  }
}

const currentSession = async () => {
  if (!isSupabaseConfigured) return null
  const { data } = await supabase.auth.getSession()
  return data.session || null
}

const normalizeTravelerMode = (value) => {
  if (value === true || value === 'male') return 'male'
  if (value === false || value === 'female') return 'female'
  return 'both'
}

export const toAppPreferences = (row = {}) => ({
  resultLanguages: Array.isArray(row.result_languages)
    ? row.result_languages.map(Number).filter(Number.isFinite)
    : undefined,
  defaultSearchLanguage: Number.isFinite(Number(row.default_search_language))
    ? Number(row.default_search_language)
    : undefined,
  sourceLanguage: Number.isFinite(Number(row.source_language))
    ? Number(row.source_language)
    : undefined,
  isMale: row.traveler_mode === 'male'
    ? true
    : row.traveler_mode === 'female'
      ? false
      : 'both',
})

export const normalizePreferences = (preferences = {}) => {
  const resultLanguages = Array.from(new Set(
    (preferences.resultLanguages || [1])
      .map(Number)
      .filter((value) => Number.isInteger(value) && value >= 0 && value <= 100),
  ))
  return {
    resultLanguages: resultLanguages.length ? resultLanguages : [1],
    defaultSearchLanguage: Number(preferences.defaultSearchLanguage) || 1,
    sourceLanguage: Number(preferences.sourceLanguage) || 1,
    isMale: preferences.isMale === true || preferences.isMale === 'male'
      ? true
      : preferences.isMale === false || preferences.isMale === 'female'
        ? false
        : 'both',
  }
}

const toDatabasePreferences = (userId, preferences) => {
  const normalized = normalizePreferences(preferences)
  return {
    user_id: userId,
    result_languages: normalized.resultLanguages,
    default_search_language: normalized.defaultSearchLanguage,
    source_language: normalized.sourceLanguage,
    traveler_mode: normalizeTravelerMode(normalized.isMale),
    updated_at: new Date().toISOString(),
  }
}

export const loadUserPreferences = async (fallback = {}) => {
  const localPreferences = readLocal(LOCAL_PREFERENCES_KEY, null)
  const base = normalizePreferences(localPreferences || fallback)
  const session = await currentSession()
  if (!session?.user) return base

  const { data, error } = await supabase
    .from('user_preferences')
    .select('result_languages, default_search_language, source_language, traveler_mode')
    .eq('user_id', session.user.id)
    .maybeSingle()

  if (error) throw error
  if (data) {
    const preferences = normalizePreferences(toAppPreferences(data))
    writeLocal(LOCAL_PREFERENCES_KEY, preferences)
    return preferences
  }

  const { error: insertError } = await supabase
    .from('user_preferences')
    .insert(toDatabasePreferences(session.user.id, base))
  if (insertError) throw insertError
  writeLocal(LOCAL_PREFERENCES_KEY, base)
  return base
}

export const saveUserPreferences = async (preferences) => {
  const normalized = normalizePreferences(preferences)
  writeLocal(LOCAL_PREFERENCES_KEY, normalized)

  const session = await currentSession()
  if (!session?.user) return { preferences: normalized, synced: false }

  const { error } = await supabase
    .from('user_preferences')
    .upsert(toDatabasePreferences(session.user.id, normalized), { onConflict: 'user_id' })
  if (error) throw error
  return { preferences: normalized, synced: true }
}

const normalizeApiPath = (url) => {
  try {
    return new URL(url, 'https://local.invalid').pathname
  } catch {
    return String(url || '').split('?')[0]
  }
}

const extractKeyword = (searchType, payload) => {
  if (searchType === 'voice' || searchType === 'story') {
    return String(payload.titleKeyword || '').trim()
  }
  return String(payload.keyword || '').trim()
}

const extractResultCount = (searchType, responseData) => {
  const body = responseData?.data || responseData || {}
  if (Number.isFinite(Number(body.total))) return Number(body.total)
  const contents = body.contents || {}
  if (searchType === 'name') {
    return (contents.quests || []).length + (contents.readables || []).length
  }
  const listKey = {
    npc_dialogue: 'npcs',
    voice: 'voices',
    story: 'stories',
    catalog: 'contents',
  }[searchType]
  const candidate = listKey === 'contents' ? body.contents : contents[listKey]
  return Array.isArray(candidate) ? candidate.length : null
}

const cleanFilters = (payload = {}) => {
  const excluded = new Set(['keyword', 'titleKeyword', 'page', 'pageSize'])
  return Object.fromEntries(
    Object.entries(payload).filter(([key, value]) => {
      if (excluded.has(key)) return false
      if (value === '' || value === null || value === undefined || value === 'all') return false
      return !(Array.isArray(value) && value.length === 0)
    }),
  )
}

const addLocalHistory = (entry) => {
  const history = readLocal(LOCAL_HISTORY_KEY, [])
  const next = [entry, ...history].slice(0, MAX_LOCAL_HISTORY)
  writeLocal(LOCAL_HISTORY_KEY, next)
}

export const recordSearchFromResponse = async (requestConfig, responseData) => {
  try {
    const path = normalizeApiPath(requestConfig?.url)
    const searchType = SEARCH_ENDPOINTS[path]
    const payload = requestConfig?.data && typeof requestConfig.data === 'string'
      ? safeJsonParse(requestConfig.data, {})
      : (requestConfig?.data || {})
    if (!searchType || Number(payload.page || 1) !== 1) return

    const entry = {
      search_type: searchType,
      keyword: extractKeyword(searchType, payload).slice(0, 500),
      filters: cleanFilters(payload),
      result_count: extractResultCount(searchType, responseData),
      created_at: new Date().toISOString(),
    }
    const session = await currentSession()
    if (!session?.user) {
      const randomId = typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
        ? crypto.randomUUID()
        : `${Date.now()}-${Math.random().toString(16).slice(2)}`
      addLocalHistory({ id: `local-${randomId}`, ...entry })
      return
    }

    const { error } = await supabase
      .from('search_history')
      .insert({ user_id: session.user.id, ...entry })
    if (error) console.warn('搜索记录同步失败:', error.message)
  } catch (error) {
    console.warn('搜索记录写入失败:', error?.message || error)
  }
}

export const migrateLocalHistory = async () => {
  const history = readLocal(LOCAL_HISTORY_KEY, [])
  if (!history.length) return 0
  const session = await currentSession()
  if (!session?.user) return 0

  const rows = history.slice(0, MAX_LOCAL_HISTORY).map((entry) => ({
    user_id: session.user.id,
    search_type: entry.search_type,
    keyword: String(entry.keyword || '').slice(0, 500),
    filters: entry.filters || {},
    result_count: Number.isFinite(Number(entry.result_count)) ? Number(entry.result_count) : null,
    created_at: entry.created_at || new Date().toISOString(),
  }))
  const { error } = await supabase.from('search_history').insert(rows)
  if (error) throw error
  writeLocal(LOCAL_HISTORY_KEY, [])
  return rows.length
}

export const listSearchHistory = async ({ limit = 200 } = {}) => {
  const session = await currentSession()
  if (!session?.user) return readLocal(LOCAL_HISTORY_KEY, []).slice(0, limit)

  const { data, error } = await supabase
    .from('search_history')
    .select('id, search_type, keyword, filters, result_count, created_at')
    .order('created_at', { ascending: false })
    .limit(limit)
  if (error) throw error
  return data || []
}

export const deleteSearchHistoryEntry = async (id) => {
  if (String(id).startsWith('local-')) {
    writeLocal(LOCAL_HISTORY_KEY, readLocal(LOCAL_HISTORY_KEY, []).filter((entry) => entry.id !== id))
    return
  }
  const { error } = await supabase.from('search_history').delete().eq('id', id)
  if (error) throw error
}

export const clearSearchHistory = async () => {
  const session = await currentSession()
  if (!session?.user) {
    writeLocal(LOCAL_HISTORY_KEY, [])
    return
  }
  const { error } = await supabase
    .from('search_history')
    .delete()
    .eq('user_id', session.user.id)
  if (error) throw error
}
