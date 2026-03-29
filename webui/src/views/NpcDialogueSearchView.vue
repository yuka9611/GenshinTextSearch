<script setup>
import { computed, onBeforeMount, ref, watch } from 'vue'
import { Search } from '@element-plus/icons-vue'
import { useRouter } from 'vue-router'
import api from '@/api/keywordQuery'
import useLanguage from '@/composables/useLanguage'
import useVersion from '@/composables/useVersion'

const router = useRouter()

const uiText = {
  pageTitle: 'NPC 对话搜索',
  helpLine1: '上方先按 NPC 名称、创建版本、更新版本筛选 NPC 本身。',
  helpLine2: '选中 NPC 后，下方再按对话创建版本、更新版本筛选该 NPC 的非任务对话卡片。',
  npcPlaceholder: '输入 NPC 名称',
  searchLanguage: '搜索语言',
  npcCreatedVersion: 'NPC 创建版本',
  npcUpdatedVersion: 'NPC 更新版本',
  dialogueCreatedVersion: '对话创建版本',
  dialogueUpdatedVersion: '对话更新版本',
  emptyNpcInput: '请输入 NPC 名称或 NPC 版本',
  npcResults: 'NPC 结果',
  noNpcResults: '没有找到 NPC',
  dialogueResults: '对话卡片',
  noDialogueResults: '没有找到符合条件的对话卡片',
  lineCount: '非任务台词数',
  created: '创建',
  updated: '更新',
  viewDialogues: '查看对话',
  viewDetails: '查看详情',
  talkId: 'Talk ID',
  dialogueId: 'Dialogue ID',
  totalGroups: '共 {count} 张卡片，当前 {page} / {totalPages} 页',
  npcSummary: '搜索耗时: {time}ms，找到 {count} 个 NPC',
  dialogueSummary: '搜索耗时: {time}ms，找到 {count} 张对话卡片',
  unknown: '未知',
  selectedNpc: '当前名称组',
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

const npcKeyword = ref('')
const npcCreatedVersionFilter = ref('')
const npcUpdatedVersionFilter = ref('')
const dialogueCreatedVersionFilter = ref('')
const dialogueUpdatedVersionFilter = ref('')
const npcSummary = ref('')
const dialogueSummary = ref('')
const npcResults = ref([])
const selectedNpc = ref(null)
const dialogueGroups = ref([])
const dialoguePage = ref(1)
const dialoguePageSize = ref(20)
const dialogueTotalGroups = ref(0)
const npcSearched = ref(false)
const dialogueLoaded = ref(false)
const loadingNpcs = ref(false)
const loadingDialogues = ref(false)

const resolveVersionValue = (entry, kind) => {
  const tag = kind === 'created' ? entry?.createdVersion : entry?.updatedVersion
  const raw = kind === 'created' ? entry?.createdVersionRaw : entry?.updatedVersionRaw
  return String(tag || raw || uiText.unknown)
}

const showUpdatedVersionTag = (entry) => {
  const createdValue = resolveVersionValue(entry, 'created')
  const updatedValue = resolveVersionValue(entry, 'updated')
  return updatedValue && updatedValue !== uiText.unknown && createdValue !== updatedValue
}

const getNpcCardKey = (entry) => {
  return `${entry?.name || ''}::${(entry?.npcIds || []).join(',')}`
}

const totalDialoguePages = computed(() => {
  return Math.max(1, Math.ceil(dialogueTotalGroups.value / dialoguePageSize.value))
})

const clearDialogueState = () => {
  selectedNpc.value = null
  dialogueGroups.value = []
  dialogueSummary.value = ''
  dialogueTotalGroups.value = 0
  dialoguePage.value = 1
  dialogueLoaded.value = false
}

watch([npcKeyword, npcCreatedVersionFilter, npcUpdatedVersionFilter], () => {
  clearDialogueState()
})

watch([dialogueCreatedVersionFilter, dialogueUpdatedVersionFilter], async () => {
  if (!selectedNpc.value) return
  dialoguePage.value = 1
  await loadDialogueGroups(1)
})

onBeforeMount(async () => {
  await loadLanguages()
  await loadVersionOptions()
})

const onNpcSearchClicked = async () => {
  const hasKeyword = npcKeyword.value.trim() !== ''
  const hasCreated = npcCreatedVersionFilter.value.trim() !== ''
  const hasUpdated = npcUpdatedVersionFilter.value.trim() !== ''
  if (!hasKeyword && !hasCreated && !hasUpdated) {
    npcSummary.value = uiText.emptyNpcInput
    npcResults.value = []
    npcSearched.value = true
    clearDialogueState()
    return
  }

  clearDialogueState()
  loadingNpcs.value = true
  npcSearched.value = true
  try {
    const ans = (await api.searchNpcDialogues(
      npcKeyword.value,
      selectedInputLanguage.value,
      npcCreatedVersionFilter.value,
      npcUpdatedVersionFilter.value,
    )).json
    const contents = ans.contents || {}
    npcResults.value = contents.npcs || []
    npcSummary.value = formatText(uiText.npcSummary, {
      time: ans.time.toFixed(2),
      count: npcResults.value.length,
    })
  } catch (error) {
    console.error('npc dialogue search failed:', error)
    npcResults.value = []
    npcSummary.value = uiText.noNpcResults
  } finally {
    loadingNpcs.value = false
  }
}

const loadDialogueGroups = async (page = 1) => {
  if (!selectedNpc.value) return
  loadingDialogues.value = true
  try {
    const ans = (await api.getNpcDialogues(
      selectedNpc.value.npcIds || [],
      selectedInputLanguage.value,
      dialogueCreatedVersionFilter.value,
      dialogueUpdatedVersionFilter.value,
      page,
      dialoguePageSize.value,
    )).json
    const contents = ans.contents || {}
    dialogueGroups.value = contents.groups || []
    dialogueTotalGroups.value = Number(contents.totalGroups || 0)
    dialoguePage.value = Number(contents.page || page || 1)
    dialogueLoaded.value = true
    dialogueSummary.value = formatText(uiText.dialogueSummary, {
      time: ans.time.toFixed(2),
      count: dialogueTotalGroups.value,
    })
  } catch (error) {
    console.error('load npc dialogues failed:', error)
    dialogueGroups.value = []
    dialogueTotalGroups.value = 0
    dialogueLoaded.value = true
    dialogueSummary.value = uiText.noDialogueResults
  } finally {
    loadingDialogues.value = false
  }
}

const onNpcClicked = async (npc) => {
  selectedNpc.value = npc
  dialoguePage.value = 1
  await loadDialogueGroups(1)
}

const goToDialoguePage = (page) => {
  const nextPage = Math.min(Math.max(1, page), totalDialoguePages.value)
  dialoguePage.value = nextPage
  loadDialogueGroups(nextPage)
}

const gotoDialogueDetail = (group) => {
  const query = {
    talkId: group.talkId,
    searchLang: selectedInputLanguage.value,
    groupMode: 'npc',
  }
  if (group.coopQuestId !== null && group.coopQuestId !== undefined) {
    query.coopQuestId = group.coopQuestId
  }
  if (group.dialogueIdFallback !== null && group.dialogueIdFallback !== undefined) {
    query.dialogueIdFallback = group.dialogueIdFallback
  }
  router.push({
    path: '/talk',
    query,
  })
}
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
        v-model="npcKeyword"
        style="max-width: 600px;"
        :placeholder="uiText.npcPlaceholder"
        class="input-with-select"
        @keyup.enter.native="onNpcSearchClicked"
        clearable
      >
        <template #prepend>
          <el-select v-model="selectedInputLanguage" :placeholder="uiText.searchLanguage" class="languageSelector">
            <el-option v-for="(v, k) in supportedInputLanguage" :key="k" :label="v" :value="k" />
          </el-select>
        </template>
        <template #append>
          <el-button :icon="Search" @click="onNpcSearchClicked" />
        </template>
      </el-input>
      <span class="searchSummary">{{ npcSummary }}</span>
    </div>

    <div class="filterBar">
      <el-select v-model="npcCreatedVersionFilter" :placeholder="uiText.npcCreatedVersion" class="versionInput" clearable filterable>
        <el-option v-for="version in versionOptions" :key="`npc-created-${version}`" :label="version" :value="version" />
      </el-select>
      <el-select v-model="npcUpdatedVersionFilter" :placeholder="uiText.npcUpdatedVersion" class="versionInput" clearable filterable>
        <el-option v-for="version in versionOptions" :key="`npc-updated-${version}`" :label="version" :value="version" />
      </el-select>
    </div>

    <div class="resultSection">
      <h2>{{ uiText.npcResults }}</h2>
      <el-empty v-if="npcSearched && !loadingNpcs && npcResults.length === 0" :description="uiText.noNpcResults" />
      <div v-else class="resultGrid">
        <el-card
          v-for="npc in npcResults"
          :key="getNpcCardKey(npc)"
          class="resultCard"
          :class="{ selectedCard: selectedNpc && getNpcCardKey(selectedNpc) === getNpcCardKey(npc) }"
        >
          <div class="cardTitle">{{ npc.name }}</div>
          <div class="versionTags">
            <el-tag size="small" effect="plain">{{ uiText.created }}: {{ resolveVersionValue(npc, 'created') }}</el-tag>
            <el-tag v-if="showUpdatedVersionTag(npc)" size="small" effect="plain">{{ uiText.updated }}: {{ resolveVersionValue(npc, 'updated') }}</el-tag>
          </div>
          <el-button size="small" type="primary" @click="onNpcClicked(npc)">{{ uiText.viewDialogues }}</el-button>
        </el-card>
      </div>
    </div>

    <div v-if="selectedNpc" class="resultSection">
      <h2>{{ uiText.dialogueResults }} - {{ selectedNpc.name }}</h2>
      <div class="selectedMeta">{{ uiText.selectedNpc }}: {{ selectedNpc.name }}</div>
      <div class="filterBar">
        <el-select v-model="dialogueCreatedVersionFilter" :placeholder="uiText.dialogueCreatedVersion" class="versionInput" clearable filterable>
          <el-option v-for="version in versionOptions" :key="`dialogue-created-${version}`" :label="version" :value="version" />
        </el-select>
        <el-select v-model="dialogueUpdatedVersionFilter" :placeholder="uiText.dialogueUpdatedVersion" class="versionInput" clearable filterable>
          <el-option v-for="version in versionOptions" :key="`dialogue-updated-${version}`" :label="version" :value="version" />
        </el-select>
      </div>

      <div v-if="dialogueSummary" class="searchSummary dialogueSummary">{{ dialogueSummary }}</div>

      <div v-if="dialogueTotalGroups > 0" class="resultControls">
        <span>{{ formatText(uiText.totalGroups, { count: dialogueTotalGroups, page: dialoguePage, totalPages: totalDialoguePages }) }}</span>
        <el-button size="small" :disabled="dialoguePage <= 1" @click="goToDialoguePage(1)">首页</el-button>
        <el-button size="small" :disabled="dialoguePage <= 1" @click="goToDialoguePage(dialoguePage - 1)">上一页</el-button>
        <el-button size="small" :disabled="dialoguePage >= totalDialoguePages" @click="goToDialoguePage(dialoguePage + 1)">下一页</el-button>
        <el-button size="small" :disabled="dialoguePage >= totalDialoguePages" @click="goToDialoguePage(totalDialoguePages)">末页</el-button>
      </div>

      <el-empty v-if="dialogueLoaded && !loadingDialogues && dialogueGroups.length === 0" :description="uiText.noDialogueResults" />
      <div v-else class="resultGrid">
        <el-card v-for="group in dialogueGroups" :key="group.groupKey" class="resultCard dialogueCard">
          <div class="cardTitle">{{ uiText.talkId }}: {{ group.talkId }}</div>
          <div v-if="group.dialogueIdFallback !== null && group.dialogueIdFallback !== undefined" class="cardMeta">
            {{ uiText.dialogueId }}: {{ group.dialogueIdFallback }}
          </div>
          <div class="cardMeta">{{ uiText.lineCount }}: {{ group.lineCount }}</div>
          <div v-if="group.previewLines?.length" class="previewBlock">
            <div v-for="(line, index) in group.previewLines" :key="`${group.groupKey}-${index}`" class="previewLine">
              {{ line }}
            </div>
          </div>
          <div class="versionTags">
            <el-tag size="small" effect="plain">{{ uiText.created }}: {{ resolveVersionValue(group, 'created') }}</el-tag>
            <el-tag v-if="showUpdatedVersionTag(group)" size="small" effect="plain">{{ uiText.updated }}: {{ resolveVersionValue(group, 'updated') }}</el-tag>
          </div>
          <el-button size="small" type="primary" @click="gotoDialogueDetail(group)">{{ uiText.viewDetails }}</el-button>
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

.dialogueSummary {
  display: block;
  margin: 0 0 12px;
}

.filterBar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin: 10px 0 12px;
}

.versionInput {
  width: 180px;
}

.resultSection {
  margin-top: 20px;
}

.resultGrid {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
}

.resultCard {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.selectedCard {
  border-color: var(--el-color-primary);
}

.dialogueCard {
  min-height: 220px;
}

.cardTitle {
  font-weight: 600;
}

.cardMeta {
  color: #888;
  font-size: 13px;
}

.selectedMeta {
  margin-bottom: 8px;
  color: #666;
}

.previewBlock {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-height: 72px;
}

.previewLine {
  color: #666;
  font-size: 13px;
  line-height: 1.6;
}

.versionTags {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

.resultControls {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 12px;
}

@media (max-width: 720px) {
  .searchSummary {
    display: block;
    margin-left: 0;
    margin-top: 8px;
  }

  .versionInput {
    width: 100%;
  }
}
</style>
