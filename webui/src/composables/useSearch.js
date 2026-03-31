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

  const sourceTypeOptions = Object.freeze([
    { value: '', label: '全部来源' },
    { value: 'dialogue', label: '对话' },
    { value: 'voice', label: '角色语音' },
    { value: 'quest', label: '任务' },
    { value: 'readable', label: '阅读物' },
    { value: 'subtitle', label: '字幕' },
    { value: 'item', label: '道具' },
    { value: 'material', label: '材料' },
    { value: 'food', label: '食物' },
    { value: 'blueprint', label: '图纸' },
    { value: 'gcg', label: '七圣召唤' },
    { value: 'namecard', label: '名片' },
    { value: 'performance', label: '表演诀窍' },
    { value: 'avatar_intro', label: '角色' },
    { value: 'dressing', label: '装扮' },
    { value: 'music_theme', label: '演奏主题' },
    { value: 'avatar_mat', label: '角色突破素材' },
    { value: 'other_mat', label: '其他' },
    { value: 'weapon', label: '武器' },
    { value: 'reliquary', label: '圣遗物' },
    { value: 'furnishing', label: '摆设' },
    { value: 'monster', label: '怪物' },
    { value: 'creature', label: '生物' },
    { value: 'costume', label: '千星奇域' },
    { value: 'suit', label: '千星奇域' },
    { value: 'achievement', label: '成就' },
    { value: 'viewpoint', label: '观景点' },
    { value: 'dungeon', label: '秘境' },
    { value: 'loading_tip', label: '过场提示' },
    { value: 'unknown', label: '未归类' },
  ])

  const queryResult = ref([])
  const keyword = ref('')
  const keywordLast = ref('')
  const speakerLast = ref('')
  const speakerKeyword = ref('')
  const searchLangLast = ref(0)
  const voiceFilter = ref('all')
  const voiceFilterLast = ref('all')
  const sourceTypeFilter = ref('')
  const sourceTypeFilterLast = ref('')
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

  const getSourceTypeLabel = (value) => {
    const normalized = String(value || '').trim()
    const option = sourceTypeOptions.find((item) => item.value === normalized)
    return option?.label || '来源'
  }

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
            sourceType: sourceTypeFilterLast.value,
            createdVersion: createdVersionLast.value,
            updatedVersion: updatedVersionLast.value,
          }
        : {
            keyword: keyword.value,
            speaker: speakerKeyword.value,
            langCode: parseInt(selectedInputLanguage.value),
            voiceFilter: voiceFilter.value,
            sourceType: sourceTypeFilter.value,
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
        params.updatedVersion,
        params.sourceType,
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
      sourceTypeFilterLast.value = params.sourceType || ''
      createdVersionLast.value = params.createdVersion || ''
      updatedVersionLast.value = params.updatedVersion || ''

      if (total > 0) {
        const filterParts = []
        const versionText = [createdVersionLast.value, updatedVersionLast.value].filter(Boolean).join(' / ')
        if (versionText) {
          filterParts.push(`版本筛选: ${versionText}`)
        }
        if (sourceTypeFilterLast.value) {
          filterParts.push(`来源: ${getSourceTypeLabel(sourceTypeFilterLast.value)}`)
        }
        const filterText = filterParts.join('，')
        searchSummary.value = filterText
          ? `查询耗时: ${timeMs}ms，总计 ${total} 条，${filterText}`
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
    const hasSourceTypeFilter = Boolean(sourceTypeFilterLast.value && sourceTypeFilterLast.value.trim())
    if (!keywordLast.value && !speakerLast.value && !createdVersionLast.value && !updatedVersionLast.value && !hasVoiceFilter && !hasSourceTypeFilter) {
      return
    }
    const safePage = Math.min(Math.max(1, page), totalPages.value)
    await fetchPage(safePage, true)
  }

  return {
    // 状态
    queryResult,
    keyword,
    keywordLast,
    speakerKeyword,
    voiceFilter,
    sourceTypeFilter,
    createdVersionFilter,
    updatedVersionFilter,
    versionOptions,
    sourceTypeOptions,
    selectedInputLanguage,
    supportedInputLanguage,
    searchLangLast,
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
