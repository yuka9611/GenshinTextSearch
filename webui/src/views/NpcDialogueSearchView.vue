<script setup>
import { computed, onBeforeMount, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import api from '@/api/keywordQuery'
import SearchBar from '@/components/SearchBar.vue'
import ActiveFilterTags from '@/components/ActiveFilterTags.vue'
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
const dialogueEmptyDescription = computed(() => dialogueSummary.value || uiText.noDialogueResults)

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
    npcSummary.value = ''
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
    dialogueSummary.value = ''
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

const onDialoguePageSizeChange = () => {
  dialoguePage.value = 1
  loadDialogueGroups(1)
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

const npcActiveFilters = computed(() => {
  const filters = []
  if (npcCreatedVersionFilter.value) {
    filters.push({ key: 'npcCreatedVersion', label: `NPC创建版本: ${npcCreatedVersionFilter.value}` })
  }
  if (npcUpdatedVersionFilter.value) {
    filters.push({ key: 'npcUpdatedVersion', label: `NPC更新版本: ${npcUpdatedVersionFilter.value}` })
  }
  return filters
})

const clearNpcFilter = (key) => {
  const map = {
    npcCreatedVersion: () => { npcCreatedVersionFilter.value = '' },
    npcUpdatedVersion: () => { npcUpdatedVersionFilter.value = '' },
  }
  map[key]?.()
}

const clearAllNpcFilters = () => {
  npcCreatedVersionFilter.value = ''
  npcUpdatedVersionFilter.value = ''
}

const dialogueActiveFilters = computed(() => {
  const filters = []
  if (dialogueCreatedVersionFilter.value) {
    filters.push({ key: 'dialogueCreatedVersion', label: `对话创建版本: ${dialogueCreatedVersionFilter.value}` })
  }
  if (dialogueUpdatedVersionFilter.value) {
    filters.push({ key: 'dialogueUpdatedVersion', label: `对话更新版本: ${dialogueUpdatedVersionFilter.value}` })
  }
  return filters
})

const clearDialogueFilter = (key) => {
  const map = {
    dialogueCreatedVersion: () => { dialogueCreatedVersionFilter.value = '' },
    dialogueUpdatedVersion: () => { dialogueUpdatedVersionFilter.value = '' },
  }
  map[key]?.()
}

const clearAllDialogueFilters = () => {
  dialogueCreatedVersionFilter.value = ''
  dialogueUpdatedVersionFilter.value = ''
}
</script>

<template>
  <div class="viewWrapper">
    <div class="stickySearchSection">
      <h1 class="pageTitle">{{ uiText.pageTitle }}</h1>
      <div class="helpText">
        <p>{{ uiText.helpLine1 }}</p>
        <p>{{ uiText.helpLine2 }}</p>
      </div>
      <SearchBar
        v-model:keyword="npcKeyword"
        v-model:selectedLanguage="selectedInputLanguage"
        :supportedLanguages="supportedInputLanguage"
        :inputPlaceholder="uiText.npcPlaceholder"
        :languagePlaceholder="uiText.searchLanguage"
        historyKey="npc"
        @search="onNpcSearchClicked"
      />

      <div class="filterBar">
        <div class="filterItem">
          <span class="filterLabel">{{ uiText.npcCreatedVersion }}</span>
          <el-select v-model="npcCreatedVersionFilter" :placeholder="uiText.npcCreatedVersion" clearable filterable>
            <el-option v-for="version in versionOptions" :key="`npc-created-${version}`" :label="version" :value="version" />
          </el-select>
        </div>
        <div class="filterItem">
          <span class="filterLabel">{{ uiText.npcUpdatedVersion }}</span>
          <el-select v-model="npcUpdatedVersionFilter" :placeholder="uiText.npcUpdatedVersion" clearable filterable>
            <el-option v-for="version in versionOptions" :key="`npc-updated-${version}`" :label="version" :value="version" />
          </el-select>
        </div>
      </div>

      <ActiveFilterTags
        :filters="npcActiveFilters"
        @clear-filter="clearNpcFilter"
        @clear-all="clearAllNpcFilters"
      />
    </div>

    <div class="resultSection resultsSection">
      <div v-if="npcResults.length > 0" class="resultSummary">
        <span class="resultCount">匹配到 <strong>{{ npcResults.length }}</strong> 个 NPC</span>
      </div>
      <el-empty v-if="npcSearched && !loadingNpcs && npcResults.length === 0" :description="uiText.noNpcResults" />
      <div v-else class="resultGrid cardGrid cardGrid--wide">
        <el-card
          v-for="npc in npcResults"
          :key="getNpcCardKey(npc)"
          class="resultCard cardPanel"
          :class="{ selectedCard: selectedNpc && getNpcCardKey(selectedNpc) === getNpcCardKey(npc) }"
        >
          <div class="cardTitle cardTitleText">{{ npc.name }}</div>
          <div class="versionTags tagRow">
            <span class="versionTag created">✦ {{ uiText.created }}: {{ resolveVersionValue(npc, 'created') }}</span>
            <span v-if="showUpdatedVersionTag(npc)" class="versionTag updated">↻ {{ uiText.updated }}: {{ resolveVersionValue(npc, 'updated') }}</span>
          </div>
          <el-button size="small" type="primary" @click="onNpcClicked(npc)">{{ uiText.viewDialogues }}</el-button>
        </el-card>
      </div>
    </div>

    <div v-if="selectedNpc" class="resultSection resultsSection">
      <div v-if="dialogueTotalGroups > 0" class="resultSummary">
        <span class="resultCount">{{ selectedNpc.name }}共 <strong>{{ dialogueTotalGroups }}</strong> 张对话卡片</span>
      </div>
      <div class="filterBar dialogueFilterBar">
        <div class="filterItem">
          <span class="filterLabel">{{ uiText.dialogueCreatedVersion }}</span>
          <el-select v-model="dialogueCreatedVersionFilter" :placeholder="uiText.dialogueCreatedVersion" clearable filterable>
            <el-option v-for="version in versionOptions" :key="`dialogue-created-${version}`" :label="version" :value="version" />
          </el-select>
        </div>
        <div class="filterItem">
          <span class="filterLabel">{{ uiText.dialogueUpdatedVersion }}</span>
          <el-select v-model="dialogueUpdatedVersionFilter" :placeholder="uiText.dialogueUpdatedVersion" clearable filterable>
            <el-option v-for="version in versionOptions" :key="`dialogue-updated-${version}`" :label="version" :value="version" />
          </el-select>
        </div>
      </div>

      <ActiveFilterTags
        :filters="dialogueActiveFilters"
        @clear-filter="clearDialogueFilter"
        @clear-all="clearAllDialogueFilters"
      />

      <el-empty v-if="dialogueLoaded && !loadingDialogues && dialogueGroups.length === 0" :description="dialogueEmptyDescription" />
      <div v-else class="resultGrid cardGrid cardGrid--wide">
        <el-card v-for="group in dialogueGroups" :key="group.groupKey" class="resultCard cardPanel dialogueCard">
          <div class="cardTitle cardTitleText">{{ uiText.talkId }}: {{ group.talkId }}</div>
          <div v-if="group.dialogueIdFallback !== null && group.dialogueIdFallback !== undefined" class="cardMeta cardMetaText">
            {{ uiText.dialogueId }}: {{ group.dialogueIdFallback }}
          </div>
          <div class="cardMeta cardMetaText">{{ uiText.lineCount }}: {{ group.lineCount }}</div>
          <div v-if="group.previewLines?.length" class="previewBlock">
            <div v-for="(line, index) in group.previewLines" :key="`${group.groupKey}-${index}`" class="previewLine">
              {{ line }}
            </div>
          </div>
          <div class="versionTags tagRow">
            <span class="versionTag created">✦ {{ uiText.created }}: {{ resolveVersionValue(group, 'created') }}</span>
            <span v-if="showUpdatedVersionTag(group)" class="versionTag updated">↻ {{ uiText.updated }}: {{ resolveVersionValue(group, 'updated') }}</span>
          </div>
          <el-button size="small" type="primary" @click="gotoDialogueDetail(group)">{{ uiText.viewDetails }}</el-button>
        </el-card>
      </div>

      <el-pagination
        v-if="dialogueTotalGroups > 0"
        class="resultPagination"
        v-model:current-page="dialoguePage"
        v-model:page-size="dialoguePageSize"
        :page-sizes="[10, 20, 50]"
        :total="dialogueTotalGroups"
        layout="prev, pager, next, sizes"
        @current-change="goToDialoguePage"
        @size-change="onDialoguePageSizeChange"
      />
    </div>
  </div>
</template>

<style scoped>
.filterBar {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.dialogueFilterBar {
  margin-bottom: 12px;
}

.selectedCard {
  box-shadow: 0 0 0 2px rgba(var(--theme-primary-rgb), 0.18), 0 18px 32px rgba(44, 57, 54, 0.10);
}

.dialogueCard {
  min-height: 220px;
}

.previewBlock {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-height: 72px;
}

.previewLine {
  color: var(--theme-text-muted);
  font-size: 13px;
  line-height: 1.6;
}

.dialogueResultCount {
  font-weight: 600;
  color: var(--theme-text);
}

@media (max-width: 680px) {
  .filterBar {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
