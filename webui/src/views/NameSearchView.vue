<script setup>
import { onBeforeMount, ref, computed, watch } from 'vue'
import { useRouter } from 'vue-router'
import api from '@/api/keywordQuery'
import SearchBar from '@/components/SearchBar.vue'
import StylizedText from '@/components/StylizedText.vue'
import useLanguage from '@/composables/useLanguage'
import useVersion from '@/composables/useVersion'
import useSearchCommon from '@/composables/useSearchCommon'
import ActiveFilterTags from '@/components/ActiveFilterTags.vue'
import formatText from '@/utils/formatText'
import { READABLE_CATEGORY_OPTIONS, getReadableCategoryLabel } from '@/utils/readableCategory'

const router = useRouter()

const uiText = {
  pageTitle: '任务 / 阅读物搜索',
  helpText: '输入任务名称、阅读物标题或版本号，支持仅按版本筛选；搜索结果可直接跳转到详情页面。',
  questSearchPlaceholder: '输入任务名称或版本',
  readableSearchPlaceholder: '输入阅读物标题或版本',
  searchLanguage: '搜索语言',
  emptyInput: '请输入关键词、版本、出场角色或任务类别',
  readableEmptyInput: '请输入关键词、版本或阅读物类别',
  questVersionOnlySummary: '搜索耗时: {time}ms，仅按版本筛选；任务 {count} 条',
  questSummary: '搜索耗时: {time}ms，任务 {count} 条',
  readableVersionOnlySummary: '搜索耗时: {time}ms，仅按版本筛选；阅读物 {count} 条',
  readableSummary: '搜索耗时: {time}ms，阅读物 {count} 条',
  createdVersion: '创建版本',
  updatedVersion: '更新版本',
  speakerKeyword: '出场角色（可选）',
  modeQuest: '任务',
  modeReadable: '阅读物',
  questResults: '任务结果',
  noQuestResults: '没有找到任务结果',
  chapter: '章节',
  questId: '任务 ID',
  created: '创建',
  updated: '更新',
  viewDetails: '查看详情',
  readableResults: '阅读物结果',
  noReadableResults: '没有找到阅读物结果',
  waitingResults: '等待检索',
  category: '类别',
  readableCategory: '阅读物类别',
}

const mode = ref('quest')
const questSourceTypeFilter = ref('')
const readableCategoryFilter = ref('')
const speakerKeyword = ref('')
const questSourceTypeOptions = [
  { value: '', label: '全部' },
  { value: 'AQ', label: '魔神任务' },
  { value: 'LQ', label: '传说任务' },
  { value: 'WQ', label: '世界任务' },
  { value: 'EQ', label: '活动任务' },
  { value: 'IQ', label: '委托任务' },
  { value: 'HANGOUT', label: '邀约事件' },
  { value: 'ANECDOTE', label: '游逸旅闻' },
  { value: 'UNKNOWN', label: '未分类' },
]
const readableCategoryOptions = READABLE_CATEGORY_OPTIONS

const {
  selectedInputLanguage,
  supportedInputLanguage,
  loadLanguages
} = useLanguage()

const {
  versionOptions,
  loadVersionOptions
} = useVersion()

const {
  keyword,
  keywordLast,
  searchSummary,
  createdVersionFilter,
  updatedVersionFilter,
  matchVersionFilter,
  displayVersion,
  showUpdatedVersionTag,
  setupVersionWatchers
} = useSearchCommon()

const questResults = ref([])
const readableResults = ref([])
const hasSearched = ref(false)
const lastSearchTime = ref(null)

onBeforeMount(async () => {
  await loadLanguages()
  await loadVersionOptions()
})

const currentResults = computed(() => (mode.value === 'quest' ? filteredQuestResults.value : filteredReadableResults.value))
const currentSearchPlaceholder = computed(() => (
  mode.value === 'quest' ? uiText.questSearchPlaceholder : uiText.readableSearchPlaceholder
))
const currentResultLabel = computed(() => (mode.value === 'quest' ? uiText.modeQuest : uiText.modeReadable))
const currentEmptyDescription = computed(() => {
  return mode.value === 'quest' ? uiText.noQuestResults : uiText.noReadableResults
})
const showQuestSpecificFilters = computed(() => mode.value === 'quest')
const showReadableSpecificFilters = computed(() => mode.value === 'readable')

const filteredQuestResults = computed(() => {
  return questResults.value.filter(matchVersionFilter)
})

const filteredReadableResults = computed(() => {
  return readableResults.value.filter(matchVersionFilter)
})

const getActiveQuestSourceType = () => (mode.value === 'quest' ? questSourceTypeFilter.value : '')
const getActiveSpeakerKeyword = () => (mode.value === 'quest' ? speakerKeyword.value : '')
const getActiveReadableCategory = () => (mode.value === 'readable' ? readableCategoryFilter.value : '')

const hasModeSpecificFilters = () => {
  const keywordText = keyword.value.trim()
  const createdText = createdVersionFilter.value.trim()
  const updatedText = updatedVersionFilter.value.trim()
  if (mode.value === 'quest') {
    return Boolean(
      keywordText ||
      createdText ||
      updatedText ||
      questSourceTypeFilter.value.trim() ||
      speakerKeyword.value.trim()
    )
  }
  return Boolean(keywordText || createdText || updatedText || readableCategoryFilter.value.trim())
}

const updateSearchSummary = () => {
  if (!hasSearched.value || lastSearchTime.value === null) {
    return
  }

  const values = {
    time: Number(lastSearchTime.value).toFixed(2),
    count: mode.value === 'quest' ? questResults.value.length : readableResults.value.length,
  }
  const keywordText = keyword.value.trim()
  const createdText = createdVersionFilter.value.trim()
  const updatedText = updatedVersionFilter.value.trim()
  const activeSpeakerText = getActiveSpeakerKeyword().trim()
  const activeSourceType = getActiveQuestSourceType().trim()
  const activeReadableCategory = getActiveReadableCategory().trim()
  const isVersionOnly = !keywordText && !activeSpeakerText && !activeSourceType && !activeReadableCategory && (createdText || updatedText)

  if (mode.value === 'quest') {
    searchSummary.value = formatText(
      isVersionOnly ? uiText.questVersionOnlySummary : uiText.questSummary,
      values,
    )
    return
  }

  searchSummary.value = formatText(
    isVersionOnly ? uiText.readableVersionOnlySummary : uiText.readableSummary,
    values,
  )
}

const onSearchClicked = async () => {
  if (!hasModeSpecificFilters()) {
    searchSummary.value = mode.value === 'quest' ? uiText.emptyInput : uiText.readableEmptyInput
    questResults.value = []
    readableResults.value = []
    hasSearched.value = false
    lastSearchTime.value = null
    return
  }

  const ans = (await api.searchByName(
    keyword.value,
    selectedInputLanguage.value,
    createdVersionFilter.value,
    updatedVersionFilter.value,
    getActiveQuestSourceType(),
    getActiveSpeakerKeyword(),
    getActiveReadableCategory(),
  )).json
  const contents = ans.contents
  keywordLast.value = keyword.value
  questResults.value = contents.quests || []
  readableResults.value = contents.readables || []
  hasSearched.value = true
  lastSearchTime.value = ans.time

  updateSearchSummary()
}

watch(mode, async () => {
  if (!hasModeSpecificFilters()) {
    questResults.value = []
    readableResults.value = []
    hasSearched.value = false
    lastSearchTime.value = null
    searchSummary.value = mode.value === 'quest' ? uiText.emptyInput : uiText.readableEmptyInput
    return
  }

  await onSearchClicked()
})

const gotoQuest = (questId) => {
  router.push({
    path: '/talk',
    query: {
      questId,
      keyword: keywordLast.value,
      searchLang: selectedInputLanguage.value,
    },
  })
}

const gotoReadable = (entry) => {
  router.push({
    path: '/talk',
    query: {
      readableId: entry.readableId,
      fileName: entry.fileName,
      keyword: keywordLast.value,
      searchLang: selectedInputLanguage.value,
    },
  })
}

setupVersionWatchers(onSearchClicked)

const activeFilters = computed(() => {
  const filters = []
  if (mode.value === 'quest') {
    if (speakerKeyword.value?.trim()) {
      filters.push({ key: 'speaker', label: `出场角色: ${speakerKeyword.value.trim()}` })
    }
    if (questSourceTypeFilter.value) {
      const opt = questSourceTypeOptions.find(o => o.value === questSourceTypeFilter.value)
      filters.push({ key: 'questSourceType', label: `任务类别: ${opt?.label || questSourceTypeFilter.value}` })
    }
  } else {
    if (readableCategoryFilter.value) {
      const opt = readableCategoryOptions.find(o => o.value === readableCategoryFilter.value)
      filters.push({ key: 'readableCategory', label: `阅读物类别: ${opt?.label || readableCategoryFilter.value}` })
    }
  }
  if (createdVersionFilter.value) {
    filters.push({ key: 'createdVersion', label: `创建版本: ${createdVersionFilter.value}` })
  }
  if (updatedVersionFilter.value) {
    filters.push({ key: 'updatedVersion', label: `更新版本: ${updatedVersionFilter.value}` })
  }
  return filters
})

const clearFilter = (key) => {
  const map = {
    speaker: () => { speakerKeyword.value = '' },
    questSourceType: () => { questSourceTypeFilter.value = '' },
    readableCategory: () => { readableCategoryFilter.value = '' },
    createdVersion: () => { createdVersionFilter.value = '' },
    updatedVersion: () => { updatedVersionFilter.value = '' },
  }
  map[key]?.()
  onSearchClicked()
}

const clearAllFilters = () => {
  speakerKeyword.value = ''
  questSourceTypeFilter.value = ''
  readableCategoryFilter.value = ''
  createdVersionFilter.value = ''
  updatedVersionFilter.value = ''
  onSearchClicked()
}
</script>

<template>
  <div class="viewWrapper">
    <div class="stickySearchSection">
      <h1 class="pageTitle">{{ uiText.pageTitle }}</h1>
      <p class="helpText">{{ uiText.helpText }}</p>
      <div class="filterTopRow">
        <el-radio-group v-model="mode" size="large" class="modeSwitch">
          <el-radio-button label="quest">{{ uiText.modeQuest }}</el-radio-button>
          <el-radio-button label="readable">{{ uiText.modeReadable }}</el-radio-button>
        </el-radio-group>
      </div>

      <SearchBar
        v-model:keyword="keyword"
        v-model:selectedLanguage="selectedInputLanguage"
        :supportedLanguages="supportedInputLanguage"
        :inputPlaceholder="currentSearchPlaceholder"
        :languagePlaceholder="uiText.searchLanguage"
        historyKey="name"
        @search="onSearchClicked"
      />

      <div class="filterBar">
        <div v-if="showQuestSpecificFilters" class="filterItem">
          <span class="filterLabel">{{ uiText.speakerKeyword }}</span>
          <el-input
            v-model="speakerKeyword"
            :placeholder="uiText.speakerKeyword"
            class="speakerInput"
            clearable
            @keyup.enter.native="onSearchClicked"
          />
        </div>
        <div class="filterItem">
          <span class="filterLabel">{{ uiText.createdVersion }}</span>
          <el-select v-model="createdVersionFilter" :placeholder="uiText.createdVersion" clearable filterable>
            <el-option v-for="version in versionOptions" :key="`created-${version}`" :label="version" :value="version" />
          </el-select>
        </div>
        <div class="filterItem">
          <span class="filterLabel">{{ uiText.updatedVersion }}</span>
          <el-select v-model="updatedVersionFilter" :placeholder="uiText.updatedVersion" clearable filterable>
            <el-option v-for="version in versionOptions" :key="`updated-${version}`" :label="version" :value="version" />
          </el-select>
        </div>
        <div v-if="showQuestSpecificFilters" class="filterItem">
          <span class="filterLabel">{{ uiText.category }}</span>
          <el-select v-model="questSourceTypeFilter" placeholder="任务类别" clearable>
            <el-option
              v-for="option in questSourceTypeOptions"
              :key="`quest-type-${option.value || 'all'}`"
              :label="option.label"
              :value="option.value"
            />
          </el-select>
        </div>
        <div v-if="showReadableSpecificFilters" class="filterItem">
          <span class="filterLabel">{{ uiText.readableCategory }}</span>
          <el-select v-model="readableCategoryFilter" :placeholder="uiText.readableCategory" clearable>
            <el-option
              v-for="option in readableCategoryOptions"
              :key="`readable-type-${option.value || 'all'}`"
              :label="option.label"
              :value="option.value"
            />
          </el-select>
        </div>
      </div>

      <ActiveFilterTags
        :filters="activeFilters"
        @clear-filter="clearFilter"
        @clear-all="clearAllFilters"
      />
    </div>

    <div class="resultSection resultsSection">
      <div v-if="currentResults.length > 0" class="resultSummary">
        <span class="resultCount">{{ currentResultLabel }}共 <strong>{{ currentResults.length }}</strong> 条结果</span>
      </div>
      <el-empty v-if="hasSearched && currentResults.length === 0" :description="currentEmptyDescription" />
      <div v-else class="resultGrid cardGrid">
        <template v-if="mode === 'quest'">
          <el-card v-for="quest in currentResults" :key="quest.questId" class="resultCard cardPanel">
            <div class="cardTitle cardTitleText">
              <StylizedText :text="quest.title" :keyword="keywordLast" />
            </div>
            <div v-if="quest.contentPreview" class="cardPreview">
              <StylizedText :text="quest.contentPreview" :keyword="keywordLast" />
            </div>
            <div v-if="quest.chapterName" class="cardMeta cardMetaText">{{ uiText.chapter }}: {{ quest.chapterName }}</div>
            <div class="cardMeta cardMetaText">{{ uiText.questId }}: {{ quest.questId }}</div>
            <div class="versionTags tagRow">
              <el-tag v-if="quest.sourceTypeLabel" size="small" effect="plain" type="success">{{ quest.sourceTypeLabel }}</el-tag>
              <span class="versionTag created">✦ {{ uiText.created }}: {{ displayVersion(quest, 'created') }}</span>
              <span v-if="showUpdatedVersionTag(quest)" class="versionTag updated">↻ {{ uiText.updated }}: {{ displayVersion(quest, 'updated') }}</span>
            </div>
            <el-button size="small" type="primary" @click="gotoQuest(quest.questId)">{{ uiText.viewDetails }}</el-button>
          </el-card>
        </template>

        <template v-else>
          <el-card v-for="readable in currentResults" :key="`${readable.readableId}-${readable.fileName}`" class="resultCard cardPanel">
            <div class="cardTitle cardTitleText">
              <StylizedText :text="readable.title" :keyword="keywordLast" />
            </div>
            <div v-if="readable.contentPreview" class="cardPreview">
              <StylizedText :text="readable.contentPreview" :keyword="keywordLast" />
            </div>
            <div class="versionTags tagRow">
              <el-tag
                v-if="getReadableCategoryLabel(readable.readableCategory)"
                size="small"
                effect="plain"
                type="success"
              >
                {{ getReadableCategoryLabel(readable.readableCategory) }}
              </el-tag>
              <span class="versionTag created">✦ {{ uiText.created }}: {{ displayVersion(readable, 'created') }}</span>
              <span v-if="showUpdatedVersionTag(readable)" class="versionTag updated">↻ {{ uiText.updated }}: {{ displayVersion(readable, 'updated') }}</span>
            </div>
            <el-button size="small" type="primary" @click="gotoReadable(readable)">{{ uiText.viewDetails }}</el-button>
          </el-card>
        </template>
      </div>
    </div>
  </div>
</template>

<style scoped>
.filterTopRow {
  display: flex;
  margin-bottom: 10px;
}

.modeSwitch {
  display: inline-flex;
  flex-wrap: wrap;
}

.modeSwitch:deep(.el-radio-button__inner) {
  min-width: 96px;
  background: rgba(255, 251, 244, 0.9);
  border-color: rgba(190, 164, 124, 0.32);
  color: var(--theme-text-muted);
  box-shadow: none;
  transition: background-color 0.12s ease, border-color 0.12s ease, color 0.12s ease, box-shadow 0.12s ease;
}

.modeSwitch:deep(.el-radio-button__inner:hover) {
  color: var(--theme-primary);
}

.modeSwitch:deep(.el-radio-button__original-radio:checked + .el-radio-button__inner) {
  background: linear-gradient(135deg, var(--theme-primary), var(--theme-primary-strong));
  border-color: transparent;
  color: #fff;
  box-shadow: 0 10px 18px rgba(var(--theme-primary-rgb), 0.18);
}

.filterBar {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.speakerInput {
  width: 100%;
  min-width: 0;
}

.cardPreview {
  color: var(--theme-text-muted);
  font-size: 13px;
  line-height: 1.6;
}

.cardPreview:deep(p) {
  margin: 0;
}

.cardTitle :deep(p) {
  margin: 0;
}

</style>

<style>
/* Dark-mode overrides for mode switch — unscoped */
[data-theme="dark"] .modeSwitch .el-radio-button__inner {
    background: rgba(30, 40, 37, 0.9);
    border-color: var(--theme-border);
    color: var(--theme-text-muted);
}
[data-theme="dark"] .modeSwitch .el-radio-button__inner:hover {
    color: var(--theme-primary);
}
[data-theme="dark"] .modeSwitch .el-radio-button__original-radio:checked + .el-radio-button__inner {
    background: linear-gradient(135deg, var(--theme-primary), var(--theme-primary-strong));
    border-color: transparent;
    color: #fff;
}
</style>
