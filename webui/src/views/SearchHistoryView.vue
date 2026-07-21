<template>
  <div class="viewWrapper historyView">
    <h1 class="pageTitle">搜索记录</h1>

    <section class="syncBanner">
      <span class="syncIcon"><i class="fi fi-rr-cloud"></i></span>
      <div class="syncCopy">
        <strong>{{ isSignedIn ? '搜索记录与偏好已按账号同步' : '登录后可同步搜索记录与偏好设置' }}</strong>
        <span>{{ isSignedIn ? `当前账号：${displayName}` : '访客记录仅保存在当前浏览器，登录后会自动迁移到账号。' }}</span>
      </div>
      <div v-if="!isSignedIn" class="syncActions">
        <el-button v-if="providers.includes('github')" @click="login('github')">
          <i class="fi fi-brands-github"></i> 使用 GitHub 登录
        </el-button>
        <el-button v-if="providers.includes('google')" @click="login('google')">
          <span class="googleMark">G</span> 使用 Google 登录
        </el-button>
        <span v-if="!providers.length" class="authPending">GitHub / Google 登录方式待配置</span>
      </div>
    </section>

    <section class="historyPanel">
      <div class="historyToolbar">
        <el-select v-model="typeFilter" class="typeFilter" placeholder="全部类型">
          <el-option label="全部类型" value="" />
          <el-option
            v-for="(meta, key) in SEARCH_TYPE_META"
            :key="key"
            :label="meta.label"
            :value="key"
          />
        </el-select>
        <el-input v-model="keywordFilter" clearable class="keywordFilter" placeholder="筛选关键词或条件">
          <template #prefix><i class="fi fi-rr-search"></i></template>
        </el-input>
        <el-button class="clearButton" :disabled="!history.length" @click="confirmClear">
          <i class="fi fi-rr-trash"></i> 清空搜索记录
        </el-button>
      </div>

      <div v-if="loading" class="historyLoading"><el-skeleton :rows="6" animated /></div>
      <el-empty v-else-if="!filteredHistory.length" description="暂无符合条件的搜索记录" />
      <div v-else class="historyTable" role="table" aria-label="搜索记录">
        <div class="historyHeader" role="row">
          <span>搜索类型</span><span>关键词</span><span>筛选条件</span><span>结果数</span><span>时间</span><span>操作</span>
        </div>
        <div v-for="entry in pagedHistory" :key="entry.id" class="historyRow" role="row">
          <span class="historyType"><i class="fi" :class="typeIcon(entry.search_type)"></i>{{ typeLabel(entry.search_type) }}</span>
          <strong class="historyKeyword">{{ entry.keyword || '仅条件筛选' }}</strong>
          <span class="historyFilters" :title="formatFilters(entry.filters)">{{ formatFilters(entry.filters) }}</span>
          <span>{{ entry.result_count ?? '—' }}</span>
          <span class="historyTime">{{ formatTime(entry.created_at) }}</span>
          <span class="historyActions">
            <el-button size="small" @click="reopen(entry)">打开搜索</el-button>
            <el-button size="small" text type="danger" aria-label="删除此记录" @click="remove(entry)">
              <i class="fi fi-rr-trash"></i>
            </el-button>
          </span>
        </div>
      </div>

      <div v-if="filteredHistory.length" class="historyFooter">
        <span>共 {{ filteredHistory.length }} 条</span>
        <el-pagination
          v-model:current-page="currentPage"
          :page-size="pageSize"
          :total="filteredHistory.length"
          layout="prev, pager, next"
        />
      </div>
    </section>
  </div>
</template>

<script setup>
import { computed, onActivated, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useAccount } from '@/composables/useAccount'
import {
  SEARCH_TYPE_META,
  clearSearchHistory,
  deleteSearchHistoryEntry,
  listSearchHistory,
} from '@/services/userData'

const router = useRouter()
const { isSignedIn, displayName, providers, signInWithProvider, initializeAccount } = useAccount()
const history = ref([])
const loading = ref(false)
const typeFilter = ref('')
const keywordFilter = ref('')
const currentPage = ref(1)
const pageSize = 20

const filteredHistory = computed(() => {
  const needle = keywordFilter.value.trim().toLowerCase()
  return history.value.filter((entry) => {
    if (typeFilter.value && entry.search_type !== typeFilter.value) return false
    if (!needle) return true
    return `${entry.keyword || ''} ${formatFilters(entry.filters)}`.toLowerCase().includes(needle)
  })
})

const pagedHistory = computed(() => {
  const start = (currentPage.value - 1) * pageSize
  return filteredHistory.value.slice(start, start + pageSize)
})

watch([typeFilter, keywordFilter], () => { currentPage.value = 1 })

const loadHistory = async () => {
  loading.value = true
  try {
    await initializeAccount()
    history.value = await listSearchHistory()
  } catch (error) {
    ElMessage.error(error?.message || '搜索记录加载失败')
  } finally {
    loading.value = false
  }
}

onActivated(loadHistory)

const login = async (provider) => {
  try {
    await signInWithProvider(provider)
  } catch (error) {
    ElMessage.error(error?.message || '登录失败')
  }
}

const typeLabel = (type) => SEARCH_TYPE_META[type]?.label || type
const typeIcon = (type) => ({
  text: 'fi-rr-search',
  name: 'fi-rr-book',
  npc_dialogue: 'fi-rr-comment',
  voice: 'fi-rr-volume',
  story: 'fi-rr-book-open-cover',
  catalog: 'fi-rr-apps',
}[type] || 'fi-rr-search')

const FILTER_LABELS = Object.freeze({
  langCode: '语言', searchLang: '语言', speaker: '说话人', voiceFilter: '语音',
  sourceType: '来源', createdVersion: '创建版本', updatedVersion: '更新版本',
  questSourceType: '任务类别', speakerKeyword: '出场角色', readableCategory: '阅读物类别',
  npcCreatedVersion: 'NPC 创建版本', npcUpdatedVersion: 'NPC 更新版本',
  sourceTypeCode: '主分类', subCategory: '子分类',
})

const formatFilters = (filters = {}) => {
  const text = Object.entries(filters || {}).map(([key, value]) => {
    const display = Array.isArray(value) ? value.join('、') : String(value)
    return `${FILTER_LABELS[key] || key}: ${display}`
  }).join('；')
  return text || '无额外筛选'
}

const formatTime = (value) => {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '未知'
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
  }).format(date)
}

const reopen = (entry) => {
  const meta = SEARCH_TYPE_META[entry.search_type]
  if (!meta) return
  router.push(meta.path)
}

const remove = async (entry) => {
  try {
    await deleteSearchHistoryEntry(entry.id)
    history.value = history.value.filter((item) => item.id !== entry.id)
    ElMessage.success('记录已删除')
  } catch (error) {
    ElMessage.error(error?.message || '删除失败')
  }
}

const confirmClear = async () => {
  try {
    await ElMessageBox.confirm('将清空当前用户的全部搜索记录，此操作不可撤销。', '清空搜索记录', {
      confirmButtonText: '确认清空', cancelButtonText: '取消', type: 'warning',
    })
    await clearSearchHistory()
    history.value = []
    ElMessage.success('搜索记录已清空')
  } catch (action) {
    if (action !== 'cancel' && action !== 'close') ElMessage.error(action?.message || '清空失败')
  }
}
</script>

<style scoped>
.historyView { gap: 18px; }
.syncBanner {
  display: flex; align-items: center; gap: 16px; padding: 18px 20px;
  border: 1px solid rgba(47, 105, 101, 0.22); border-radius: 18px;
  background: linear-gradient(90deg, rgba(225, 237, 233, 0.76), rgba(255, 255, 255, 0.62));
}
.syncIcon { width: 44px; height: 44px; flex: 0 0 auto; border-radius: 50%; display: grid; place-items: center; color: var(--theme-primary); background: rgba(47, 105, 101, 0.09); font-size: 20px; }
.syncCopy { display: flex; flex-direction: column; gap: 4px; min-width: 0; flex: 1; }
.syncCopy strong { color: var(--theme-ink); }
.syncCopy span { color: var(--theme-text-muted); font-size: 13px; }
.syncActions { display: flex; gap: 8px; flex-wrap: wrap; }
.syncActions .el-button { margin-left: 0; }
.authPending { color: var(--theme-text-muted); font-size: 13px; }
.googleMark { color: #4285f4; font-weight: 800; }
.historyPanel { border: 1px solid var(--theme-border); border-radius: 18px; background: rgba(255, 255, 255, 0.54); overflow: hidden; box-shadow: 0 12px 24px rgba(44, 57, 54, 0.07); }
.historyToolbar { display: flex; gap: 12px; padding: 16px 18px; border-bottom: 1px solid var(--theme-border); }
.typeFilter { width: 190px; }
.keywordFilter { flex: 1; min-width: 180px; }
.clearButton { margin-left: auto; color: var(--theme-accent, #9d6d29); }
.historyLoading { padding: 20px; }
.historyTable { width: 100%; }
.historyHeader, .historyRow { display: grid; grid-template-columns: 150px minmax(140px, 1fr) minmax(220px, 2fr) 80px 110px 148px; align-items: center; gap: 12px; padding: 12px 18px; }
.historyHeader { color: var(--theme-text-muted); font-size: 12px; font-weight: 700; background: rgba(47, 105, 101, 0.035); border-bottom: 1px solid var(--theme-border); }
.historyRow { min-height: 58px; border-bottom: 1px solid var(--theme-border); color: var(--theme-text); }
.historyRow:hover { background: rgba(47, 105, 101, 0.035); }
.historyType { display: flex; align-items: center; gap: 8px; }
.historyType i { color: var(--theme-primary); }
.historyKeyword { color: var(--theme-primary-strong); overflow-wrap: anywhere; }
.historyFilters { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--theme-text-muted); font-size: 13px; }
.historyTime { font-size: 13px; color: var(--theme-text-muted); }
.historyActions { display: flex; align-items: center; gap: 2px; }
.historyActions .el-button { margin-left: 0; }
.historyFooter { display: flex; justify-content: space-between; align-items: center; padding: 14px 18px; color: var(--theme-text-muted); font-size: 13px; }
[data-theme="dark"] .syncBanner { background: linear-gradient(90deg, rgba(47, 105, 101, 0.2), rgba(30, 40, 37, 0.72)); }
[data-theme="dark"] .historyPanel { background: rgba(30, 40, 37, 0.55); }
@media (max-width: 980px) {
  .historyHeader { display: none; }
  .historyRow { grid-template-columns: 1fr auto; gap: 7px 12px; padding: 16px; }
  .historyType, .historyKeyword, .historyFilters { grid-column: 1; }
  .historyRow > span:nth-child(4), .historyTime, .historyActions { grid-column: 2; }
  .historyRow > span:nth-child(4) { grid-row: 1; text-align: right; }
  .historyTime { grid-row: 2; }
  .historyActions { grid-row: 3 / span 2; }
}
@media (max-width: 680px) {
  .syncBanner { align-items: flex-start; flex-wrap: wrap; padding: 16px; }
  .syncActions { width: 100%; }
  .syncActions .el-button { flex: 1; }
  .historyToolbar { flex-wrap: wrap; }
  .typeFilter, .keywordFilter, .clearButton { width: 100%; flex: 1 1 100%; margin-left: 0; }
  .historyRow { grid-template-columns: 1fr; }
  .historyRow > *, .historyType, .historyKeyword, .historyFilters, .historyRow > span:nth-child(4), .historyTime, .historyActions { grid-column: 1; grid-row: auto; text-align: left; }
  .historyActions { justify-content: flex-end; }
  .historyFooter { flex-direction: column; gap: 10px; }
}
</style>
