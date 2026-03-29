<script setup>
import { onBeforeMount, ref, computed } from 'vue'
import { Search } from '@element-plus/icons-vue'
import { useRouter } from 'vue-router'
import api from '@/api/keywordQuery'
import StylizedText from '@/components/StylizedText.vue'
import useLanguage from '@/composables/useLanguage'
import useVersion from '@/composables/useVersion'
import useSearchCommon from '@/composables/useSearchCommon'

const router = useRouter()

const uiText = {
  pageTitle: '任务 / 可读物搜索',
  helpText: '输入任务名称、可读物标题或版本号，支持仅按版本筛选；搜索结果可直接跳转到详情页面。',
  searchPlaceholder: '输入关键词或版本',
  searchLanguage: '搜索语言',
  emptyInput: '请输入关键词、版本、出场角色或任务类别',
  versionOnlySummary: '搜索耗时: {time}ms，仅按版本筛选；任务 {questCount} 条，可读物 {readableCount} 条',
  summary: '搜索耗时: {time}ms，任务 {questCount} 条，可读物 {readableCount} 条',
  createdVersion: '创建版本',
  updatedVersion: '更新版本',
  speakerKeyword: '出场角色（可选）',
  questResults: '任务结果',
  noQuestResults: '没有找到任务结果',
  chapter: '章节',
  questId: '任务 ID',
  created: '创建',
  updated: '更新',
  viewDetails: '查看详情',
  readableResults: '可读物结果',
  noReadableResults: '没有找到可读物结果',
}

const questSourceTypeFilter = ref('')
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

const formatText = (template, values) => {
  return template.replace(/\{(\w+)\}/g, (_, key) => String(values[key] ?? ''))
}

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

onBeforeMount(async () => {
  await loadLanguages()
  await loadVersionOptions()
})

const filteredQuestResults = computed(() => {
  return questResults.value.filter(matchVersionFilter)
})

const filteredReadableResults = computed(() => {
  return readableResults.value.filter(matchVersionFilter)
})

const onSearchClicked = async () => {
  const keywordText = keyword.value.trim()
  const createdText = createdVersionFilter.value.trim()
  const updatedText = updatedVersionFilter.value.trim()
  const sourceTypeText = questSourceTypeFilter.value.trim()
  const speakerText = speakerKeyword.value.trim()

  if (!keywordText && !createdText && !updatedText && !sourceTypeText && !speakerText) {
    searchSummary.value = uiText.emptyInput
    questResults.value = []
    readableResults.value = []
    return
  }

  const ans = (await api.searchByName(
    keyword.value,
    selectedInputLanguage.value,
    createdVersionFilter.value,
    updatedVersionFilter.value,
    questSourceTypeFilter.value,
    speakerKeyword.value,
  )).json
  const contents = ans.contents
  keywordLast.value = keyword.value
  questResults.value = contents.quests || []
  readableResults.value = contents.readables || []

  const summaryValues = {
    time: ans.time.toFixed(2),
    questCount: questResults.value.length,
    readableCount: readableResults.value.length,
  }
  if (!keywordText && !speakerText && (createdText || updatedText)) {
    searchSummary.value = formatText(uiText.versionOnlySummary, summaryValues)
  } else {
    searchSummary.value = formatText(uiText.summary, summaryValues)
  }
}

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
</script>

<template>
  <div class="viewWrapper">
    <h1 class="pageTitle">{{ uiText.pageTitle }}</h1>
    <div class="helpText">
      <p>{{ uiText.helpText }}</p>
    </div>

    <div class="stickySearchSection">
      <div class="searchBar">
        <el-input
          v-model="keyword"
          style="max-width: 600px;"
          :placeholder="uiText.searchPlaceholder"
          class="input-with-select"
          @keyup.enter.native="onSearchClicked"
          clearable
        >
          <template #prepend>
            <el-select v-model="selectedInputLanguage" :placeholder="uiText.searchLanguage" class="languageSelector">
              <el-option v-for="(v, k) in supportedInputLanguage" :key="k" :label="v" :value="k" />
            </el-select>
          </template>
          <template #append>
            <el-button :icon="Search" @click="onSearchClicked" />
          </template>
        </el-input>
        <span class="searchSummary">{{ searchSummary }}</span>
      </div>

      <div class="filterBar">
        <el-input
          v-model="speakerKeyword"
          :placeholder="uiText.speakerKeyword"
          class="speakerInput"
          clearable
          @keyup.enter.native="onSearchClicked"
        />
        <el-select v-model="createdVersionFilter" :placeholder="uiText.createdVersion" class="versionInput" clearable filterable>
          <el-option v-for="version in versionOptions" :key="`created-${version}`" :label="version" :value="version" />
        </el-select>
        <el-select v-model="updatedVersionFilter" :placeholder="uiText.updatedVersion" class="versionInput" clearable filterable>
          <el-option v-for="version in versionOptions" :key="`updated-${version}`" :label="version" :value="version" />
        </el-select>
        <el-select v-model="questSourceTypeFilter" placeholder="任务类别" class="versionInput" clearable>
          <el-option
            v-for="option in questSourceTypeOptions"
            :key="`quest-type-${option.value || 'all'}`"
            :label="option.label"
            :value="option.value"
          />
        </el-select>
      </div>
    </div>

    <div class="resultSection">
      <h2>{{ uiText.questResults }}</h2>
      <el-empty v-if="filteredQuestResults.length === 0" :description="uiText.noQuestResults" />
      <div v-else class="resultGrid">
        <el-card v-for="quest in filteredQuestResults" :key="quest.questId" class="resultCard">
          <div class="cardTitle">
            <StylizedText :text="quest.title" :keyword="keywordLast" />
          </div>
          <div v-if="quest.contentPreview" class="cardPreview">
            <StylizedText :text="quest.contentPreview" :keyword="keywordLast" />
          </div>
          <div v-if="quest.chapterName" class="cardMeta">{{ uiText.chapter }}: {{ quest.chapterName }}</div>
          <div class="cardMeta">{{ uiText.questId }}: {{ quest.questId }}</div>
          <div class="versionTags">
            <el-tag size="small" effect="plain">{{ uiText.created }}: {{ displayVersion(quest, 'created') }}</el-tag>
            <el-tag v-if="showUpdatedVersionTag(quest)" size="small" effect="plain">{{ uiText.updated }}: {{ displayVersion(quest, 'updated') }}</el-tag>
          </div>
          <el-button size="small" type="primary" @click="gotoQuest(quest.questId)">{{ uiText.viewDetails }}</el-button>
        </el-card>
      </div>
    </div>

    <div class="resultSection">
      <h2>{{ uiText.readableResults }}</h2>
      <el-empty v-if="filteredReadableResults.length === 0" :description="uiText.noReadableResults" />
      <div v-else class="resultGrid">
        <el-card v-for="readable in filteredReadableResults" :key="`${readable.readableId}-${readable.fileName}`" class="resultCard">
          <div class="cardTitle">
            <StylizedText :text="readable.title" :keyword="keywordLast" />
          </div>
          <div v-if="readable.contentPreview" class="cardPreview">
            <StylizedText :text="readable.contentPreview" :keyword="keywordLast" />
          </div>
          <div class="versionTags">
            <el-tag size="small" effect="plain">{{ uiText.created }}: {{ displayVersion(readable, 'created') }}</el-tag>
            <el-tag v-if="showUpdatedVersionTag(readable)" size="small" effect="plain">{{ uiText.updated }}: {{ displayVersion(readable, 'updated') }}</el-tag>
          </div>
          <el-button size="small" type="primary" @click="gotoReadable(readable)">{{ uiText.viewDetails }}</el-button>
        </el-card>
      </div>
    </div>
  </div>
</template>

<style scoped>
.viewWrapper {
  position: relative;
  width: var(--page-width);
  margin: 0 auto;
  background-color: #fff;
  box-shadow: var(--page-shadow);
  border-radius: var(--page-radius);
  padding: var(--page-padding);
  overflow: visible;
}

.pageTitle {
  border-bottom: 1px #ccc solid;
  padding-bottom: 10px;
}

.helpText {
  margin: 20px 0;
  color: #999;
}

.searchBar {
  padding-bottom: 8px;
  box-sizing: border-box;
}

.languageSelector {
  width: 120px;
}

.languageSelector:deep(input) {
  text-align: center;
}

.searchSummary {
  margin-left: 10px;
  color: var(--el-input-text-color, var(--el-text-color-regular));
  font-size: 14px;
}

.filterBar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  width: 100%;
  max-width: 960px;
  margin: 10px 0 6px;
}

.versionInput {
  width: 180px;
}

.speakerInput {
  width: 180px;
  min-width: 180px;
}

.resultSection {
  margin-top: 20px;
}

.resultSection h2 {
  margin-bottom: 12px;
}

.resultGrid {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
}

.resultCard {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.cardTitle {
  font-weight: 600;
}

.cardPreview {
  color: #666;
  font-size: 13px;
  line-height: 1.6;
}

.cardPreview:deep(p) {
  margin: 0;
}

.cardTitle :deep(p) {
  margin: 0;
}

.cardMeta {
  color: #888;
  font-size: 13px;
}

.versionTags {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

@media (max-width: 720px) {
  .searchSummary {
    display: block;
    margin-left: 0;
    margin-top: 8px;
  }

}
</style>
