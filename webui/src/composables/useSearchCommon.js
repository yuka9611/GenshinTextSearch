import { ref, watch } from 'vue'

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
    if (kind === 'created') return entry.createdVersion || entry.createdVersionRaw || '未知'
    return entry.updatedVersion || entry.updatedVersionRaw || '未知'
  }

  const showUpdatedVersionTag = (entry) => {
    return displayVersion(entry, 'created') !== displayVersion(entry, 'updated')
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