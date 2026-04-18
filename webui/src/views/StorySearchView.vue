<script setup>
import { onBeforeMount, ref, computed } from 'vue'
import api from '@/api/keywordQuery'
import SearchBar from '@/components/SearchBar.vue'
import TranslateDisplay from '@/components/ResultEntry.vue'
import useAvatarScopedSearch from '@/composables/useAvatarScopedSearch'
import useLanguage from '@/composables/useLanguage'
import useVersion from '@/composables/useVersion'
import useSearchCommon from '@/composables/useSearchCommon'
import ActiveFilterTags from '@/components/ActiveFilterTags.vue'
import formatText from '@/utils/formatText'

const uiText = {
  pageTitle: '角色故事搜索',
  helpText: '输入角色名可先筛出角色，也可以直接按故事标题或版本进行全局搜索。',
  avatarPlaceholder: '输入角色名',
  storyPlaceholder: '故事标题或内容',
  searchLanguage: '搜索语言',
  createdVersion: '创建版本',
  updatedVersion: '更新版本',
  emptyInput: '请输入角色名、标题或版本',
  avatarSummary: '搜索耗时: {time}ms，找到 {count} 个角色',
  avatarSearchFailed: '角色搜索失败，请检查控制台日志',
  globalMatchedAvatar: '全部匹配角色（按当前条件）',
  globalAvatar: '全部角色（按当前条件）',
  fallbackAvatarName: '角色',
  filteredAvatarSummary: '搜索耗时: {time}ms，筛出 {avatarCount} 个角色',
  storySummary: '搜索耗时: {time}ms，找到 {storyCount} 条故事',
  storySearchFailed: '故事搜索失败，请检查控制台日志',
  invalidAvatarId: '角色 ID 无效',
  filteredByAvatar: '已按角色过滤，当前 {storyCount} 条故事',
  loadStoryFailed: '加载角色故事失败，请检查控制台日志',
  avatarResults: '角色结果',
  noAvatarResults: '没有找到角色',
  avatarId: '角色 ID',
  viewStories: '查看故事',
  storyResults: '故事结果',
  noStoryResults: '没有故事结果',
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
  searchSummary,
  createdVersionFilter,
  updatedVersionFilter,
  normalizeText,
  normalizeVersion,
  getNormalizedEntryVersion,
  isSameCreatedUpdatedVersion,
  setupVersionWatchers
} = useSearchCommon()

const storySummary = ref('')
const loadingStories = ref(false)
const textFilter = ref('')
const {
  avatarResults,
  scopedEntries: storyEntries,
  useGlobalEntries: useGlobalStoryEntries,
  selectedAvatar,
  resetScopedState,
  setAvatarMatches,
  beginGlobalResults,
  showGlobalEntries,
  filterEntriesByMatchedAvatarIds,
  selectAvatarFromGlobalEntries,
  setAvatarEntries,
} = useAvatarScopedSearch({
  fallbackAvatarName: uiText.fallbackAvatarName,
  matchedAvatarLabel: uiText.globalMatchedAvatar,
  globalAvatarLabel: uiText.globalAvatar,
})

onBeforeMount(async () => {
  await loadLanguages()
  await loadVersionOptions()
})

const filteredStories = computed(() => {
  const text = normalizeText(textFilter.value)
  const createdFilter = normalizeVersion(createdVersionFilter.value)
  const updatedFilter = normalizeVersion(updatedVersionFilter.value)

  return storyEntries.value.filter((entry) => {
    const translates = entry.translates || {}
    if (Object.keys(translates).length === 0) return false

    const createdVersion = getNormalizedEntryVersion(entry, 'created')
    const updatedVersion = getNormalizedEntryVersion(entry, 'updated')
    if (createdFilter && !createdVersion.includes(createdFilter)) return false
    if (updatedFilter) {
      if (!updatedVersion.includes(updatedFilter)) return false
      if (isSameCreatedUpdatedVersion(entry)) return false
    }

    if (!text) return true

    const title = normalizeText(entry.storyTitle || entry.origin || '')
    if (title.includes(text)) return true

    for (const key of Object.keys(translates)) {
      const content = normalizeText(translates[key] || '')
      if (content.includes(text)) return true
    }
    return false
  })
})

const highlightKeyword = computed(() => textFilter.value.trim())
const storyResultContext = computed(() => selectedAvatar.value?.name || uiText.storyResults)
const storyEmptyDescription = computed(() => storySummary.value || uiText.noStoryResults)

const resetStoryState = () => {
  storySummary.value = ''
  resetScopedState()
}

const onSearchClicked = async () => {
  const avatarKeyword = keyword.value.trim()
  const titleKeyword = textFilter.value.trim()
  const createdKeyword = createdVersionFilter.value.trim()
  const updatedKeyword = updatedVersionFilter.value.trim()
  const hasGlobalFilter = titleKeyword || createdKeyword || updatedKeyword

  if (!avatarKeyword && !hasGlobalFilter) {
    searchSummary.value = uiText.emptyInput
    avatarResults.value = []
    resetStoryState()
    return
  }

  let matchedAvatarIds = null
  if (avatarKeyword) {
    try {
      const ans = (await api.searchAvatar(keyword.value, selectedInputLanguage.value)).json
      const contents = ans.contents || {}
      matchedAvatarIds = setAvatarMatches(contents.avatars || [])
      searchSummary.value = formatText(uiText.avatarSummary, {
        time: ans.time.toFixed(2),
        count: avatarResults.value.length,
      })
      resetStoryState()
    } catch (err) {
      searchSummary.value = uiText.avatarSearchFailed
      avatarResults.value = []
      resetStoryState()
      if (!err?.network) err?.defaultHandler?.()
      return
    }

    if (!hasGlobalFilter) {
      return
    }
  }

  loadingStories.value = true
  if (!avatarKeyword) {
    avatarResults.value = []
  }
  beginGlobalResults(Boolean(avatarKeyword))

  try {
    const ans = (await api.searchAvatarStories(
      titleKeyword,
      createdVersionFilter.value,
      updatedVersionFilter.value,
      selectedInputLanguage.value,
    )).json
    const contents = ans.contents || {}
    const scopedStories = filterEntriesByMatchedAvatarIds(contents.stories || [], matchedAvatarIds)
    showGlobalEntries(scopedStories)

    searchSummary.value = formatText(uiText.filteredAvatarSummary, {
      time: ans.time.toFixed(2),
      avatarCount: avatarResults.value.length,
    })
    storySummary.value = ''
  } catch (err) {
    searchSummary.value = uiText.storySearchFailed
    resetStoryState()
    if (!err?.network) err?.defaultHandler?.()
  } finally {
    loadingStories.value = false
  }
}

const onAvatarClicked = async (avatar) => {
  const avatarId = Number.parseInt(avatar?.avatarId, 10)
  if (Number.isNaN(avatarId)) {
    storySummary.value = uiText.invalidAvatarId
    return
  }

  storySummary.value = ''

  if (useGlobalStoryEntries.value) {
    selectAvatarFromGlobalEntries(avatar)
    storySummary.value = ''
    return
  }

  storyEntries.value = []
  loadingStories.value = true

  try {
    const ans = (await api.getAvatarStories(avatarId, selectedInputLanguage.value)).json
    const contents = ans.contents || {}
    setAvatarEntries(avatar, contents.stories || [])
    storySummary.value = ''
  } catch (err) {
    storyEntries.value = []
    storySummary.value = uiText.loadStoryFailed
    if (!err?.network) err?.defaultHandler?.()
  } finally {
    loadingStories.value = false
  }
}

setupVersionWatchers(onSearchClicked)

const activeFilters = computed(() => {
  const filters = []
  if (textFilter.value?.trim()) {
    filters.push({ key: 'text', label: `标题/内容: ${textFilter.value.trim()}` })
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
    text: () => { textFilter.value = '' },
    createdVersion: () => { createdVersionFilter.value = '' },
    updatedVersion: () => { updatedVersionFilter.value = '' },
  }
  map[key]?.()
  onSearchClicked()
}

const clearAllFilters = () => {
  textFilter.value = ''
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
      <SearchBar
        v-model:keyword="keyword"
        v-model:selectedLanguage="selectedInputLanguage"
        :supportedLanguages="supportedInputLanguage"
        :inputPlaceholder="uiText.avatarPlaceholder"
        :languagePlaceholder="uiText.searchLanguage"
        historyKey="story"
        @search="onSearchClicked"
      />

      <div class="filterBar topFilterBar">
        <div class="filterItem filterItem--wide">
          <span class="filterLabel">{{ uiText.storyPlaceholder }}</span>
          <el-input v-model="textFilter" :placeholder="uiText.storyPlaceholder" clearable @keyup.enter.native="onSearchClicked" />
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
      </div>

      <ActiveFilterTags
        :filters="activeFilters"
        @clear-filter="clearFilter"
        @clear-all="clearAllFilters"
      />
    </div>

    <div v-if="keyword.trim() || textFilter.trim() || createdVersionFilter.trim() || updatedVersionFilter.trim()" class="resultSection resultsSection">
      <div v-if="avatarResults.length > 0" class="resultSummary">
        <span class="resultCount">匹配到 <strong>{{ avatarResults.length }}</strong> 个角色</span>
      </div>
      <el-empty v-if="avatarResults.length === 0" :description="uiText.noAvatarResults" />
      <div v-else class="resultGrid cardGrid">
        <el-card v-for="avatar in avatarResults" :key="avatar.avatarId" class="resultCard cardPanel">
          <div class="cardTitle cardTitleText">{{ avatar.name }}</div>
          <div class="cardMeta cardMetaText">{{ uiText.avatarId }}: {{ avatar.avatarId }}</div>
          <el-button size="small" type="primary" @click="onAvatarClicked(avatar)">
            {{ uiText.viewStories }}
          </el-button>
        </el-card>
      </div>
    </div>

    <div class="resultSection resultsSection">
      <div v-if="filteredStories.length > 0" class="resultSummary">
        <span class="resultCount">{{ storyResultContext }}共 <strong>{{ filteredStories.length }}</strong> 条故事</span>
      </div>

      <el-empty
        v-if="!loadingStories && filteredStories.length === 0 && (useGlobalStoryEntries || selectedAvatar)"
        :description="storyEmptyDescription"
      />
      <div v-else>
        <TranslateDisplay
          v-for="story in filteredStories"
          :key="story.hash"
          :translate-obj="story"
          :keyword="highlightKeyword"
          :search-lang="selectedInputLanguage"
          class="translate textResultItem"
        />
      </div>
    </div>
  </div>
</template>

<style scoped>
.topFilterBar {
  grid-template-columns: minmax(0, 1.35fr) repeat(2, minmax(0, 1fr));
}

</style>
