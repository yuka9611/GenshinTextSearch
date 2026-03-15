import { ref, computed } from 'vue'
import api from '@/api/keywordQuery'
import useLanguage from '@/composables/useLanguage'
import useVersion from '@/composables/useVersion'

const useSearch = () => {
  const { selectedInputLanguage, supportedInputLanguage, loadLanguages } = useLanguage()
  const { versionOptions, loadVersionOptions } = useVersion()

  // 初始化时加载语言列表和版本选项
  loadLanguages().catch(console.error)
  loadVersionOptions().catch(console.error)

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
  const searchSummary = ref('')
  const pageSize = ref(50)
  const currentPage = ref(1)
  const totalCount = ref(0)
  const isLoading = ref(false)

  const totalPages = computed(() => Math.max(1, Math.ceil(totalCount.value / pageSize.value)))

  const normalizeText = (value) => {
    if (!value) return ''
    return String(value).trim().toLowerCase()
  }

  const normalizeVersion = (value) => normalizeText(value)

  const getNormalizedEntryVersion = (entry, kind) => {
    if (kind === 'created') return normalizeVersion(entry.createdVersion || entry.createdVersionRaw || '')
    return normalizeVersion(entry.updatedVersion || entry.updatedVersionRaw || '')
  }

  const shouldKeepByVersionFilter = (entry, updatedFilterRaw, createdFilterRaw) => {
    const updatedFilter = normalizeVersion(updatedFilterRaw)
    const createdFilter = normalizeVersion(createdFilterRaw)

    // 检查创建版本筛选
    if (createdFilter) {
      const createdValue = getNormalizedEntryVersion(entry, 'created')
      if (!createdValue.includes(createdFilter)) return false
    }

    // 检查更新版本筛选
    if (updatedFilter) {
      const updatedValue = getNormalizedEntryVersion(entry, 'updated')
      if (!updatedValue.includes(updatedFilter)) return false

      const createdValue = getNormalizedEntryVersion(entry, 'created')
      if (!createdValue || !updatedValue) return true
      return createdValue !== updatedValue
    }

    return true
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
            langCode: parseInt(selectedInputLanguage.value),
            voiceFilter: voiceFilter.value,
            createdVersion: createdVersionFilter.value,
            updatedVersion: updatedVersionFilter.value,
          }

      console.log('搜索参数:', params);
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
        // 不过滤掉空文本的结果，因为说话人筛选可能返回没有翻译的结果
        // if (!entry.translates || Object.values(entry.translates).every(content => !content || content.trim() === '')) {
        //   return false
        // }
        return shouldKeepByVersionFilter(entry, params.updatedVersion, params.createdVersion)
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
    const hasVoiceFilter = voiceFilterLast.value && voiceFilterLast.value !== 'all'
    if (!keywordLast.value && !speakerLast.value && !createdVersionLast.value && !updatedVersionLast.value && !hasVoiceFilter) {
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
    selectedInputLanguage,
    supportedInputLanguage,
    searchSummary,
    currentPage,
    totalCount,
    totalPages,
    isLoading,

    // 方法
    loadVersionOptions,
    onQueryButtonClicked,
    goToPage
  }
}

export default useSearch
