import { ref, computed } from 'vue'
import api from '@/api/keywordQuery'
import basicInfoApi from '@/api/basicInfo'
import global from '@/global/global'

const useSearch = () => {
  const queryResult = ref([])
  const keyword = ref('')
  const keywordLast = ref('')
  const speakerLast = ref('')
  const speakerKeyword = ref('')
  const searchLangLast = ref(0)
  const voiceFilter = ref('all')
  const voiceFilterLast = ref('all')
  const createdVersionFilter = ref('')
  const updatedVersionFilter = ref('')
  const createdVersionLast = ref('')
  const updatedVersionLast = ref('')
  const versionOptions = ref([])
  const searchSummary = ref('')
  const pageSize = ref(50)
  const currentPage = ref(1)
  const totalCount = ref(0)
  const isLoading = ref(false)
  
  const totalPages = computed(() => Math.max(1, Math.ceil(totalCount.value / pageSize.value)))
  const supportedInputLanguage = computed(() => global.languages)
  
  const normalizeText = (value) => {
    if (!value) return ''
    return String(value).trim().toLowerCase()
  }
  
  const normalizeVersion = (value) => normalizeText(value)
  
  const getNormalizedEntryVersion = (entry, kind) => {
    if (kind === 'created') return normalizeVersion(entry.createdVersion || entry.createdVersionRaw || '')
    return normalizeVersion(entry.updatedVersion || entry.updatedVersionRaw || '')
  }
  
  const shouldKeepByVersionFilter = (entry, updatedFilterRaw) => {
    const updatedFilter = normalizeVersion(updatedFilterRaw)
    if (!updatedFilter) return true

    const updatedValue = getNormalizedEntryVersion(entry, 'updated')
    if (!updatedValue.includes(updatedFilter)) return false

    const createdValue = getNormalizedEntryVersion(entry, 'created')
    if (!createdValue || !updatedValue) return true
    return createdValue !== updatedValue
  }
  
  const fetchAvailableVersions = async () => {
    try {
      const ans = await basicInfoApi.getAvailableVersions()
      versionOptions.value = ans.json || []
    } catch (_) {
      versionOptions.value = []
    }
  }
  
  const fetchPage = async (page, useLast = false) => {
    isLoading.value = true
    try {
      const params = useLast
        ? {
            keyword: keywordLast.value,
            speaker: speakerLast.value,
            langCode: searchLangLast.value,
            voiceFilter: voiceFilterLast.value,
            createdVersion: createdVersionLast.value,
            updatedVersion: updatedVersionLast.value,
          }
        : {
            keyword: keyword.value,
            speaker: speakerKeyword.value,
            langCode: parseInt(global.config.defaultSearchLanguage),
            voiceFilter: voiceFilter.value,
            createdVersion: createdVersionFilter.value,
            updatedVersion: updatedVersionFilter.value,
          }

      const ans = (await api.queryByKeyword(
        params.keyword,
        params.langCode,
        params.speaker,
        page,
        pageSize.value,
        params.voiceFilter,
        params.createdVersion,
        params.updatedVersion
      )).json

      const timeMs = typeof ans.time === 'number' ? ans.time.toFixed(2) : '0.00'
      const total = ans.total || 0

      queryResult.value = (ans.contents || []).filter((entry) => {
        return shouldKeepByVersionFilter(entry, params.updatedVersion)
      })
      totalCount.value = total
      currentPage.value = ans.page || page

      keywordLast.value = params.keyword || ''
      speakerLast.value = params.speaker || ''
      searchLangLast.value = params.langCode
      voiceFilterLast.value = params.voiceFilter || 'all'
      createdVersionLast.value = params.createdVersion || ''
      updatedVersionLast.value = params.updatedVersion || ''

      if (total > 0) {
        const filterText = [createdVersionLast.value, updatedVersionLast.value].filter(Boolean).join(' / ')
        searchSummary.value = filterText
          ? `查询耗时: ${timeMs}ms，总计 ${total} 条，版本筛选: ${filterText}`
          : `查询耗时: ${timeMs}ms，总计 ${total} 条`
      } else {
        searchSummary.value = `查询耗时: ${timeMs}ms，未找到结果`
      }
    } catch (error) {
      console.error('搜索失败:', error)
      searchSummary.value = '搜索失败，请重试'
      queryResult.value = []
      totalCount.value = 0
    } finally {
      isLoading.value = false
    }
  }
  
  const onQueryButtonClicked = async () => {
    currentPage.value = 1
    await fetchPage(1, false)
  }
  
  const goToPage = async (page) => {
    if (!keywordLast.value && !speakerLast.value && !createdVersionLast.value && !updatedVersionLast.value) {
      return
    }
    const safePage = Math.min(Math.max(1, page), totalPages.value)
    await fetchPage(safePage, true)
  }
  
  return {
    // 状态
    queryResult,
    keyword,
    speakerKeyword,
    voiceFilter,
    createdVersionFilter,
    updatedVersionFilter,
    versionOptions,
    supportedInputLanguage,
    searchSummary,
    currentPage,
    totalCount,
    totalPages,
    isLoading,
    
    // 方法
    fetchAvailableVersions,
    onQueryButtonClicked,
    goToPage
  }
}

export default useSearch
