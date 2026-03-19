import { ref, watch } from 'vue'

const UNKNOWN_VERSION_TEXT = '未知'

const useSearchCommon = () => {
  const keyword = ref('')
  const keywordLast = ref('')
  const searchSummary = ref('')
  const isLoading = ref(false)
  const createdVersionFilter = ref('')
  const updatedVersionFilter = ref('')

  const normalizeText = (value) => {
    if (!value) return ''
    return String(value).trim().toLowerCase()
  }

  const normalizeVersion = (value) => normalizeText(value)

  const resolveVersionValue = (versionTag, rawVersion) => {
    if (versionTag) return String(versionTag).trim()
    if (rawVersion) return String(rawVersion).trim()
    return ''
  }

  const getNormalizedEntryVersion = (entry, kind) => {
    if (kind === 'created') return normalizeVersion(entry.createdVersion || entry.createdVersionRaw || '')
    return normalizeVersion(entry.updatedVersion || entry.updatedVersionRaw || '')
  }

  const isSameCreatedUpdatedVersion = (entry) => {
    const createdVersion = getNormalizedEntryVersion(entry, 'created')
    const updatedVersion = getNormalizedEntryVersion(entry, 'updated')
    if (!createdVersion || !updatedVersion) return false
    return createdVersion === updatedVersion
  }

  const matchVersionFilter = (entry) => {
    const createdFilter = normalizeVersion(createdVersionFilter.value)
    const updatedFilter = normalizeVersion(updatedVersionFilter.value)
    const createdValue = getNormalizedEntryVersion(entry, 'created')
    const updatedValue = getNormalizedEntryVersion(entry, 'updated')
    if (createdFilter && !createdValue.includes(createdFilter)) return false
    if (updatedFilter) {
      if (!updatedValue.includes(updatedFilter)) return false
      if (isSameCreatedUpdatedVersion(entry)) return false
    }
    return true
  }

  const displayVersion = (entry, kind) => {
    const value = kind === 'created'
      ? resolveVersionValue(entry.createdVersion, entry.createdVersionRaw)
      : resolveVersionValue(entry.updatedVersion, entry.updatedVersionRaw)
    return value || UNKNOWN_VERSION_TEXT
  }

  const showUpdatedVersionTag = (entry) => {
    const updatedValue = resolveVersionValue(entry.updatedVersion, entry.updatedVersionRaw)
    if (!updatedValue) return false
    const createdValue = resolveVersionValue(entry.createdVersion, entry.createdVersionRaw)
    return createdValue !== updatedValue
  }

  const setupVersionWatchers = (callback) => {
    watch(createdVersionFilter, () => {
      callback()
    })
    watch(updatedVersionFilter, () => {
      callback()
    })
  }

  return {
    keyword,
    keywordLast,
    searchSummary,
    isLoading,
    createdVersionFilter,
    updatedVersionFilter,
    normalizeText,
    normalizeVersion,
    getNormalizedEntryVersion,
    isSameCreatedUpdatedVersion,
    matchVersionFilter,
    displayVersion,
    showUpdatedVersionTag,
    setupVersionWatchers
  }
}

export default useSearchCommon
