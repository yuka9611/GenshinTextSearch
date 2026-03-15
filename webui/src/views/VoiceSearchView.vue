<script setup>
import { onBeforeMount, ref, computed, reactive, watch } from 'vue'
import { Search, Close } from '@element-plus/icons-vue'
import global from '@/global/global'
import api from '@/api/keywordQuery'
import TranslateDisplay from '@/components/ResultEntry.vue'
import AudioPlayer from '@liripeng/vue-audio-player'
import * as converter from '@/assets/wem2wav'
import { ElMessage } from 'element-plus'
import useLanguage from '@/composables/useLanguage'
import useVersion from '@/composables/useVersion'
import useSearchCommon from '@/composables/useSearchCommon'

const uiText = {
  pageTitle: '角色语音搜索',
  helpLine1: '输入角色名可先筛出角色，再点击查看该角色的语音。',
  helpLine2: '也可以直接用标题、创建版本、更新版本做全局语音搜索。',
  avatarPlaceholder: '输入角色名',
  titlePlaceholder: '语音标题或内容',
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
  voiceSummary: '搜索耗时: {time}ms，找到 {voiceCount} 条语音',
  voiceSearchFailed: '语音搜索失败，请检查控制台日志',
  filteredByAvatar: '已按角色过滤，当前 {voiceCount} 条语音',
  loadVoiceFailed: '加载角色语音失败，请检查控制台日志',
  uncategorized: '未分类',
  avatarResults: '角色结果',
  noAvatarResults: '没有找到角色',
  avatarId: '角色 ID',
  viewVoices: '查看语音',
  voiceResults: '语音结果',
  category: '分类',
  all: '全部',
  voiceLanguage: '语音语言',
  playCurrent: '播放当前结果',
  clearPlaylist: '清空播放列表',
  noVoiceResults: '没有语音结果',
  loadingVoices: '加载语音中',
  audioUnavailable: '当前语言暂无语音',
  audioMissing: '未找到语音文件',
  noPlayableAudio: '所选语言没有可播放的语音',
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

const voiceSummary = ref('')
const avatarResults = ref([])
const voiceEntries = ref([])
const globalVoiceEntries = ref([])
const useGlobalVoiceEntries = ref(false)
const selectedAvatar = ref(null)
const loadingVoices = ref(false)

const categoryFilter = ref('all')
const textFilter = ref('')
const selectedVoiceLanguage = ref('')

const showPlayer = ref(false)
const firstShowPlayer = ref(true)
const audio = ref([])
const voicePlayer = ref()

const playlistLoading = reactive({
  show: false,
  total: 0,
  current: 0,
  percentage: 0
})

onBeforeMount(async () => {
  await loadLanguages()
  await loadVersionOptions()
})

const availableVoiceLanguages = computed(() => global.voiceLanguages || {})

const deriveCategory = (title) => {
  const raw = (title || '').trim()
  if (!raw) return uiText.uncategorized
  const separators = ['·', '•', ':', '-', ' ']
  for (const sep of separators) {
    if (raw.includes(sep)) {
      const head = raw.split(sep)[0].trim()
      return head || raw
    }
  }
  return raw
}

const categories = computed(() => {
  const set = new Set()
  for (const entry of voiceEntries.value) {
    set.add(deriveCategory(entry.voiceTitle || ''))
  }
  return ['all', ...Array.from(set)]
})

const filteredVoices = computed(() => {
  const category = categoryFilter.value
  const text = normalizeText(textFilter.value)
  const createdVersion = normalizeVersion(createdVersionFilter.value)
  const updatedVersion = normalizeVersion(updatedVersionFilter.value)

  return voiceEntries.value.filter((entry) => {
    if (category !== 'all') {
      const entryCategory = deriveCategory(entry.voiceTitle || '')
      if (entryCategory !== category) return false
    }

    if (createdVersion) {
      const source = getNormalizedEntryVersion(entry, 'created')
      if (!source.includes(createdVersion)) return false
    }

    if (updatedVersion) {
      const source = getNormalizedEntryVersion(entry, 'updated')
      if (!source.includes(updatedVersion)) return false
      if (isSameCreatedUpdatedVersion(entry)) return false
    }

    if (!text) return true

    const title = normalizeText(entry.voiceTitle || '')
    if (title.includes(text)) return true

    const translates = entry.translates || {}
    for (const key of Object.keys(translates)) {
      const content = normalizeText(translates[key] || '')
      if (content.includes(text)) return true
    }
    return false
  })
})

const isVoiceAvailableForEntry = (entry, langCode) => {
  const availableLangs = entry?.availableVoiceLangs || []
  return entry?.voicePaths?.length > 0 && availableLangs.includes(Number(langCode))
}

const voiceLanguageOptions = computed(() => {
  const activeLangs = new Set()
  for (const entry of filteredVoices.value) {
    for (const langCode of entry.availableVoiceLangs || []) {
      const key = String(langCode)
      if (availableVoiceLanguages.value[key]) {
        activeLangs.add(key)
      }
    }
  }

  return Array.from(activeLangs).map((key) => ({
    id: key,
    name: availableVoiceLanguages.value[key]
  }))
})

const playableFilteredVoices = computed(() => {
  return filteredVoices.value.filter((entry) => isVoiceAvailableForEntry(entry, selectedVoiceLanguage.value))
})

watch(voiceLanguageOptions, (options) => {
  const firstOption = options[0]?.id || ''
  const currentOptionStillValid = options.some((option) => option.id === selectedVoiceLanguage.value)
  if (!currentOptionStillValid) {
    selectedVoiceLanguage.value = firstOption
  }
}, { immediate: true })

const highlightKeyword = computed(() => textFilter.value.trim())

const resetVoiceState = () => {
  voiceSummary.value = ''
  voiceEntries.value = []
  globalVoiceEntries.value = []
  useGlobalVoiceEntries.value = false
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
    resetVoiceState()
    return
  }

  let matchedAvatarIds = null
  if (avatarKeyword) {
    try {
      const response = await api.searchAvatar(keyword.value, selectedInputLanguage.value)
      const ans = response.json
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
      resetVoiceState()
    } catch (error) {
      console.error('search avatar failed:', error)
      searchSummary.value = uiText.avatarSearchFailed
      avatarResults.value = []
      resetVoiceState()
      return
    }

    if (!hasGlobalFilter) {
      return
    }
  }

  loadingVoices.value = true
  categoryFilter.value = 'all'
  if (!avatarKeyword) {
    avatarResults.value = []
  }
  selectedAvatar.value = {
    avatarId: null,
    name: avatarKeyword ? uiText.globalMatchedAvatar : uiText.globalAvatar,
  }

  try {
    const response = await api.searchAvatarVoices(
      titleKeyword,
      createdVersionFilter.value,
      updatedVersionFilter.value,
      selectedInputLanguage.value,
    )
    const ans = response.json
    const contents = ans.contents || {}
    let scopedVoices = contents.voices || []
    if (matchedAvatarIds) {
      scopedVoices = scopedVoices.filter((entry) => matchedAvatarIds.has(Number(entry.avatarId)))
    }

    voiceEntries.value = scopedVoices
    globalVoiceEntries.value = scopedVoices
    useGlobalVoiceEntries.value = true

    const avatarMap = new Map()
    for (const entry of scopedVoices) {
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
    voiceSummary.value = formatText(uiText.voiceSummary, {
      time: ans.time.toFixed(2),
      voiceCount: scopedVoices.length,
    })
  } catch (error) {
    console.error('search voices failed:', error)
    searchSummary.value = uiText.voiceSearchFailed
    resetVoiceState()
  } finally {
    loadingVoices.value = false
  }
}

const onAvatarClicked = async (avatar) => {
  selectedAvatar.value = avatar
  voiceSummary.value = ''
  categoryFilter.value = 'all'

  if (useGlobalVoiceEntries.value) {
    const avatarId = Number(avatar?.avatarId)
    voiceEntries.value = globalVoiceEntries.value.filter((entry) => Number(entry.avatarId) === avatarId)
    voiceSummary.value = formatText(uiText.filteredByAvatar, {
      voiceCount: voiceEntries.value.length,
    })
    return
  }

  voiceEntries.value = []
  loadingVoices.value = true

  try {
    const response = await api.getAvatarVoices(avatar.avatarId, selectedInputLanguage.value)
    const ans = response.json
    const contents = ans.contents || {}
    voiceEntries.value = contents.voices || []
    voiceSummary.value = formatText(uiText.voiceSummary, {
      time: ans.time.toFixed(2),
      voiceCount: voiceEntries.value.length,
    })
  } catch (error) {
    console.error('load avatar voices failed:', error)
    voiceSummary.value = uiText.loadVoiceFailed
    voiceEntries.value = []
  } finally {
    loadingVoices.value = false
  }
}

const onVoicePlay = (voiceUrl) => {
  if (!voiceUrl) return
  if (firstShowPlayer.value) {
    showPlayer.value = true
    firstShowPlayer.value = false
  }
  if (audio.value.length > 0 && voiceUrl === audio.value[0]) {
    if (voicePlayer.value.isPlaying) {
      voicePlayer.value.pause()
    } else {
      voicePlayer.value.play()
    }
    return
  }
  audio.value = [voiceUrl]
  voicePlayer.value.currentPlayIndex = 0
  setTimeout(() => {
    voicePlayer.value.play()
  }, 100)
}

const getAudioUrlForEntry = async (entry) => {
  const voicePath = entry.voicePaths?.[0]
  const langCode = selectedVoiceLanguage.value
  if (!voicePath || !langCode || !isVoiceAvailableForEntry(entry, langCode)) return null
  try {
    const buffer = await api.getVoiceOver(voicePath, langCode)
    if (!buffer) return null
    return await converter.convertBufferedArray(buffer)
  } catch (error) {
    console.error(error)
    return null
  }
}

const loadPlaylist = async () => {
  const items = playableFilteredVoices.value
  if (items.length === 0) {
    ElMessage.warning(uiText.noPlayableAudio)
    return
  }
  playlistLoading.total = items.length
  playlistLoading.current = 0
  playlistLoading.percentage = 0
  playlistLoading.show = true

  const urls = []
  for (const entry of items) {
    const url = await getAudioUrlForEntry(entry)
    if (url) {
      urls.push(url)
    }
    playlistLoading.current += 1
    playlistLoading.percentage = playlistLoading.total
      ? Math.round((100 * playlistLoading.current) / playlistLoading.total)
      : 0
  }

  playlistLoading.show = false
  if (urls.length === 0) {
    ElMessage.warning(uiText.noPlayableAudio)
    return
  }

  if (firstShowPlayer.value) {
    showPlayer.value = true
    firstShowPlayer.value = false
  }
  audio.value = urls
  voicePlayer.value.currentPlayIndex = 0
  setTimeout(() => {
    voicePlayer.value.play()
  }, 100)
}

const clearPlaylist = () => {
  audio.value = []
}

const onHidePlayerButtonClicked = () => {
  showPlayer.value = false
}

const onShowPlayerButtonClicked = () => {
  showPlayer.value = true
}

setupVersionWatchers(onSearchClicked)
</script>

<template>
  <div class="viewWrapper">
    <h1 class="pageTitle">{{ uiText.pageTitle }}</h1>
    <div class="helpText">
      <p>{{ uiText.helpLine1 }}</p>
      <p>{{ uiText.helpLine2 }}</p>
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
      <el-input v-model="textFilter" :placeholder="uiText.titlePlaceholder" class="filterInput" clearable @keyup.enter.native="onSearchClicked" />
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
            {{ uiText.viewVoices }}
          </el-button>
        </el-card>
      </div>
    </div>

    <div class="resultSection">
      <h2 v-if="selectedAvatar">{{ uiText.voiceResults }} - {{ selectedAvatar.name }}</h2>
      <div v-if="voiceSummary" class="voiceSummary">{{ voiceSummary }}</div>

      <div v-if="selectedAvatar" class="filterBar">
        <el-select v-model="categoryFilter" :placeholder="uiText.category" class="filterSelect">
          <el-option
            v-for="item in categories"
            :key="item"
            :label="item === 'all' ? uiText.all : item"
            :value="item"
          />
        </el-select>
        <el-select v-model="selectedVoiceLanguage" :placeholder="uiText.voiceLanguage" class="filterSelect">
          <el-option
            v-for="item in voiceLanguageOptions"
            :key="item.id"
            :label="item.name"
            :value="item.id"
          />
        </el-select>
        <el-button size="small" type="primary" :disabled="playableFilteredVoices.length === 0" @click="loadPlaylist">{{ uiText.playCurrent }}</el-button>
        <el-button size="small" @click="clearPlaylist">{{ uiText.clearPlaylist }}</el-button>
      </div>

      <el-empty v-if="!loadingVoices && filteredVoices.length === 0" :description="uiText.noVoiceResults" />
      <div v-else>
        <TranslateDisplay
          v-for="voice in filteredVoices"
          :key="voice.hash"
          :translate-obj="voice"
          :keyword="highlightKeyword"
          :search-lang="selectedInputLanguage"
          class="translate"
          @onVoicePlay="onVoicePlay"
        />
      </div>
    </div>
  </div>

  <div v-if="audio.length > 0" v-show="showPlayer" class="viewWrapper voicePlayerContainer">
    <span class="hideIcon" @click="onHidePlayerButtonClicked">
      <el-icon>
        <Close />
      </el-icon>
    </span>

    <AudioPlayer
      ref="voicePlayer"
      :audio-list="audio"
      :show-prev-button="true"
      :show-next-button="true"
      :is-loop="false"
      :progress-interval="25"
      theme-color="var(--el-color-primary)"
    />
  </div>

  <div v-show="!showPlayer && audio.length > 0" class="showPlayerButton" @click="onShowPlayerButtonClicked">
    <i class="fi fi-sr-waveform-path"></i>
  </div>

  <el-dialog v-model="playlistLoading.show" :width="300" :title="uiText.loadingVoices" align-center>
    <el-progress :percentage="playlistLoading.percentage">
      {{ playlistLoading.current }} / {{ playlistLoading.total }}
    </el-progress>
  </el-dialog>
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

.voiceSummary {
  margin: 8px 0 12px;
  color: #666;
  font-size: 13px;
}

.filterBar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
  justify-content: flex-start;
  margin-bottom: 12px;
}

.topFilterBar {
  margin-top: 8px;
}

.filterSelect {
  min-width: 140px;
}

.filterInput {
  flex: 1 1 240px;
  max-width: 320px;
}

.versionInput {
  width: 180px;
}

.translate:not(:last-child) {
  border-bottom: 1px solid #ccc;
}

.voicePlayerContainer {
  margin-top: 10px;
  bottom: 0;
  position: sticky !important;
  box-shadow: 0 0 5px 5px rgba(36, 37, 38, 0.05);
  z-index: 3;
  background-color: #fff;
}

.showPlayerButton {
  position: absolute;
  right: 7.5%;
  bottom: 80px;
  height: 70px;
  width: 70px;
  border-radius: 50%;
  background-color: var(--el-color-primary);
  color: #fff;
  font-size: 25px;
  box-shadow: 0 6px 15px rgba(36, 37, 38, 0.2);
  text-align: center;
  line-height: 75px;
  cursor: pointer;
  z-index: 3;
}

.showPlayerButton:hover {
  background-color: var(--el-color-primary-light-3);
}

.hideIcon {
  cursor: pointer;
  position: absolute;
  top: 10px;
  right: 10px;
}

.hideIcon:hover {
  color: #888;
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

  .voicePlayerContainer {
    position: fixed !important;
    left: 8px;
    right: 8px;
    bottom: 0;
    width: auto;
    margin-top: 0;
    border-radius: 0;
    z-index: 3;
    box-sizing: border-box;
    box-shadow: none;
  }

  .showPlayerButton {
    position: fixed;
    right: 16px;
    bottom: 24px;
    width: 56px;
    height: 56px;
    line-height: 60px;
    z-index: 3;
    box-shadow: none;
  }
}
</style>
