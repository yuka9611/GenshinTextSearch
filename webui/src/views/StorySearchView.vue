<script setup>
import { onBeforeMount, ref, computed } from 'vue'
import { Search } from '@element-plus/icons-vue'
import api from '@/api/keywordQuery'
import TranslateDisplay from '@/components/ResultEntry.vue'
import useLanguage from '@/composables/useLanguage'
import useVersion from '@/composables/useVersion'
import useSearchCommon from '@/composables/useSearchCommon'

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
const avatarResults = ref([])
const storyEntries = ref([])
const globalStoryEntries = ref([])
const useGlobalStoryEntries = ref(false)
const selectedAvatar = ref(null)
const loadingStories = ref(false)
const textFilter = ref('')

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

const resetStoryState = () => {
  storySummary.value = ''
  storyEntries.value = []
  globalStoryEntries.value = []
  useGlobalStoryEntries.value = false
  selectedAvatar.value = null
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
      avatarResults.value = contents.avatars || []
      matchedAvatarIds = new Set(
        avatarResults.value
          .map((avatar) => Number(avatar.avatarId))
          .filter((avatarId) => !Number.isNaN(avatarId))
      )
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
  selectedAvatar.value = {
    avatarId: null,
    name: avatarKeyword ? uiText.globalMatchedAvatar : uiText.globalAvatar,
  }

  try {
    const ans = (await api.searchAvatarStories(
      titleKeyword,
      createdVersionFilter.value,
      updatedVersionFilter.value,
      selectedInputLanguage.value,
    )).json
    const contents = ans.contents || {}
    let scopedStories = contents.stories || []
    if (matchedAvatarIds) {
      scopedStories = scopedStories.filter((entry) => matchedAvatarIds.has(Number(entry.avatarId)))
    }

    storyEntries.value = scopedStories
    globalStoryEntries.value = scopedStories
    useGlobalStoryEntries.value = true

    const avatarMap = new Map()
    for (const entry of scopedStories) {
      if (entry.avatarId === null || entry.avatarId === undefined) continue
      if (avatarMap.has(entry.avatarId)) continue
      avatarMap.set(entry.avatarId, {
        avatarId: entry.avatarId,
        name: (entry.avatarName || '').trim() || `${uiText.fallbackAvatarName} ${entry.avatarId}`
      })
    }
    avatarResults.value = Array.from(avatarMap.values()).sort((a, b) => a.name.localeCompare(b.name))

    searchSummary.value = formatText(uiText.filteredAvatarSummary, {
      time: ans.time.toFixed(2),
      avatarCount: avatarResults.value.length,
    })
    storySummary.value = formatText(uiText.storySummary, {
      time: ans.time.toFixed(2),
      storyCount: scopedStories.length,
    })
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

  selectedAvatar.value = { ...avatar, avatarId }
  storySummary.value = ''

  if (useGlobalStoryEntries.value) {
    storyEntries.value = globalStoryEntries.value.filter((entry) => Number(entry.avatarId) === avatarId)
    storySummary.value = formatText(uiText.filteredByAvatar, {
      storyCount: storyEntries.value.length,
    })
    return
  }

  storyEntries.value = []
  loadingStories.value = true

  try {
    const ans = (await api.getAvatarStories(avatarId, selectedInputLanguage.value)).json
    const contents = ans.contents || {}
    storyEntries.value = contents.stories || []
    storySummary.value = formatText(uiText.storySummary, {
      time: ans.time.toFixed(2),
      storyCount: storyEntries.value.length,
    })
  } catch (err) {
    storyEntries.value = []
    storySummary.value = uiText.loadStoryFailed
    if (!err?.network) err?.defaultHandler?.()
  } finally {
    loadingStories.value = false
  }
}

setupVersionWatchers(onSearchClicked)
</script>

<template>
  <div class="viewWrapper">
    <h1 class="pageTitle">{{ uiText.pageTitle }}</h1>
    <div class="helpText">
      <p>{{ uiText.helpText }}</p>
    </div>

    <div class="searchBar">
      <el-input
        v-model="keyword"
        style="max-width: 600px;"
        :placeholder="uiText.avatarPlaceholder"
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

    <div class="filterBar topFilterBar">
      <el-input v-model="textFilter" :placeholder="uiText.storyPlaceholder" class="filterInput" clearable @keyup.enter.native="onSearchClicked" />
      <el-select v-model="createdVersionFilter" :placeholder="uiText.createdVersion" class="versionInput" clearable filterable>
        <el-option v-for="version in versionOptions" :key="`created-${version}`" :label="version" :value="version" />
      </el-select>
      <el-select v-model="updatedVersionFilter" :placeholder="uiText.updatedVersion" class="versionInput" clearable filterable>
        <el-option v-for="version in versionOptions" :key="`updated-${version}`" :label="version" :value="version" />
      </el-select>
    </div>

    <div class="searchSpacer"></div>

    <div v-if="keyword.trim() || textFilter.trim() || createdVersionFilter.trim() || updatedVersionFilter.trim()" class="resultSection">
      <h2>{{ uiText.avatarResults }}</h2>
      <el-empty v-if="avatarResults.length === 0" :description="uiText.noAvatarResults" />
      <div v-else class="resultGrid">
        <el-card v-for="avatar in avatarResults" :key="avatar.avatarId" class="resultCard">
          <div class="cardTitle">{{ avatar.name }}</div>
          <div class="cardMeta">{{ uiText.avatarId }}: {{ avatar.avatarId }}</div>
          <el-button size="small" type="primary" @click="onAvatarClicked(avatar)">
            {{ uiText.viewStories }}
          </el-button>
        </el-card>
      </div>
    </div>

    <div class="resultSection">
      <h2 v-if="selectedAvatar">{{ uiText.storyResults }} - {{ selectedAvatar.name }}</h2>
      <div v-if="storySummary" class="storySummary">{{ storySummary }}</div>

      <el-empty v-if="!loadingStories && filteredStories.length === 0" :description="uiText.noStoryResults" />
      <div v-else>
        <TranslateDisplay
          v-for="story in filteredStories"
          :key="story.hash"
          :translate-obj="story"
          :keyword="highlightKeyword"
          :search-lang="selectedInputLanguage"
          class="translate"
        />
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
  position: sticky;
  top: 0;
  z-index: 3;
  background-color: #fff;
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

.searchSpacer {
  display: none;
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

.cardMeta {
  color: #888;
  font-size: 13px;
}

.storySummary {
  margin: 8px 0 12px;
  color: #666;
  font-size: 13px;
}

.filterBar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
  margin-bottom: 12px;
}

.topFilterBar {
  margin-top: 8px;
}

.filterInput {
  max-width: 260px;
}

.versionInput {
  width: 180px;
}

.translate:not(:last-child) {
  border-bottom: 1px solid #ccc;
}

@media (max-width: 720px) {
  .searchSummary {
    display: block;
    margin-left: 0;
    margin-top: 8px;
  }

  .searchSpacer {
    display: none;
    height: 0;
  }
}
</style>