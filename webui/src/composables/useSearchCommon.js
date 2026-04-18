import { ref, watch } from 'vue'
import {
  displayVersion,
  getNormalizedEntryVersion,
  isSameCreatedUpdatedVersion,
  matchVersionFilters,
  normalizeText,
  normalizeVersion,
  showUpdatedVersionTag,
} from '@/utils/versionFilters'

const useSearchCommon = () => {
  const keyword = ref('')
  const keywordLast = ref('')
  const searchSummary = ref('')
  const isLoading = ref(false)
  const createdVersionFilter = ref('')
  const updatedVersionFilter = ref('')

  const matchVersionFilter = (entry) => {
    return matchVersionFilters(entry, createdVersionFilter.value, updatedVersionFilter.value)
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
