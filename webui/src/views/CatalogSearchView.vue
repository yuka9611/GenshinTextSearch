<script setup>
import { ref, computed, onBeforeMount, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '@/api/keywordQuery'
import useLanguage from '@/composables/useLanguage'

const router = useRouter()
const { selectedInputLanguage, supportedInputLanguage, loadLanguages } = useLanguage()

const keyword = ref('')
const selectedMainCategory = ref('')
const selectedSubCategory = ref('')
const results = ref([])
const totalCount = ref(0)
const currentPage = ref(1)
const pageSize = ref(50)
const isLoading = ref(false)
const searchSummary = ref('')

const mainCategories = ref({})
const subCategories = ref({})

const totalPages = computed(() => Math.max(1, Math.ceil(totalCount.value / pageSize.value)))

// Build list of sub-categories relevant to the selected main category
// Since sub_category codes are shared across source_type_codes, show all when filtering
const subCategoryOptions = computed(() => {
  const entries = Object.entries(subCategories.value)
  if (!entries.length) return []
  return entries.map(([code, label]) => ({ value: code, label }))
})

const loadMeta = async () => {
  try {
    const ans = (await api.getCatalogMeta()).json
    mainCategories.value = ans.mainCategories || {}
    subCategories.value = ans.subCategories || {}
  } catch (_) {
    // ignore
  }
}

const doSearch = async (page = 1) => {
  const kw = keyword.value.trim()
  if (!kw) return

  isLoading.value = true
  try {
    const langCode = parseInt(selectedInputLanguage.value)
    const stc = selectedMainCategory.value || null
    const sub = selectedSubCategory.value || null

    const ans = (await api.catalogSearch(kw, langCode, stc, sub, page, pageSize.value)).json
    results.value = ans.contents || []
    totalCount.value = ans.total || 0
    currentPage.value = ans.page || page
    const timeMs = typeof ans.time === 'number' ? ans.time.toFixed(2) : '0.00'
    searchSummary.value = `共 ${totalCount.value} 条结果，耗时 ${timeMs}ms`
  } catch (_) {
    ElMessage.error('搜索失败')
    results.value = []
  } finally {
    isLoading.value = false
  }
}

const handleSearch = () => {
  currentPage.value = 1
  doSearch(1)
}

const handlePageChange = (page) => {
  doSearch(page)
}

const gotoEntity = (item) => {
  router.push({
    path: '/entity',
    query: {
      sourceTypeCode: item.sourceTypeCode,
      entityId: item.entityId,
      keyword: keyword.value,
      searchLang: selectedInputLanguage.value,
    },
  })
}

onBeforeMount(async () => {
  await loadLanguages()
  await loadMeta()
})

watch(selectedMainCategory, () => {
  selectedSubCategory.value = ''
})
</script>

<template>
  <div class="viewWrapper pageShell">
    <h2 class="pageTitle">图鉴搜索</h2>

    <div class="searchArea">
      <div class="searchRow">
        <el-input
          v-model="keyword"
          placeholder="输入物品名称"
          clearable
          class="searchInput"
          @keyup.enter="handleSearch"
        />
        <el-button type="primary" :loading="isLoading" @click="handleSearch">搜索</el-button>
      </div>

      <div class="filterRow">
        <el-select v-model="selectedInputLanguage" class="filterSelect" placeholder="语言" filterable>
          <el-option v-for="(name, code) in supportedInputLanguage" :key="`lang-${code}`" :label="name" :value="code" />
        </el-select>

        <el-select v-model="selectedMainCategory" class="filterSelect" placeholder="全部大分类" clearable>
          <el-option label="全部大分类" value="" />
          <el-option v-for="(label, code) in mainCategories" :key="`cat-${code}`" :label="label" :value="code" />
        </el-select>

        <el-select v-model="selectedSubCategory" class="filterSelect" placeholder="全部二级分类" clearable>
          <el-option label="全部二级分类" value="" />
          <el-option v-for="opt in subCategoryOptions" :key="`sub-${opt.value}`" :label="opt.label" :value="opt.value" />
        </el-select>
      </div>
    </div>

    <div v-if="searchSummary" class="searchSummary">{{ searchSummary }}</div>

    <div v-if="isLoading" class="loadingArea">
      <el-skeleton :rows="6" animated />
    </div>

    <div v-else-if="results.length > 0" class="resultList">
      <div
        v-for="item in results"
        :key="`${item.sourceTypeCode}-${item.entityId}`"
        class="resultItem cardPanel"
        @click="gotoEntity(item)"
      >
        <div class="resultTitle">{{ item.title }}</div>
        <div class="resultMeta">
          <el-tag size="small" effect="plain">{{ item.sourceTypeLabel }}</el-tag>
          <el-tag v-if="item.subCategoryLabel" size="small" effect="plain" type="info">{{ item.subCategoryLabel }}</el-tag>
          <span class="resultId">ID: {{ item.entityId }}</span>
        </div>
      </div>
    </div>

    <div v-if="totalPages > 1" class="paginationArea">
      <el-pagination
        :current-page="currentPage"
        :page-size="pageSize"
        :total="totalCount"
        layout="prev, pager, next"
        @current-change="handlePageChange"
      />
    </div>
  </div>
</template>

<style scoped>
.pageTitle {
  margin: 0 0 16px 0;
}

.searchArea {
  margin-bottom: 16px;
}

.searchRow {
  display: flex;
  gap: 8px;
  margin-bottom: 10px;
}

.searchInput {
  flex: 1;
  max-width: 400px;
}

.filterRow {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.filterSelect {
  width: 160px;
}

.searchSummary {
  margin-bottom: 12px;
  color: var(--theme-text-muted);
  font-size: 13px;
}

.loadingArea {
  margin-top: 16px;
}

.resultList {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.resultItem {
  padding: 12px 16px;
  cursor: pointer;
  border-radius: 8px;
  transition: background-color 0.15s;
}

.resultItem:hover {
  background-color: var(--el-fill-color-light);
}

.resultTitle {
  font-size: 15px;
  font-weight: 500;
  margin-bottom: 6px;
}

.resultMeta {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
}

.resultId {
  color: var(--theme-text-muted);
  margin-left: 4px;
}

.paginationArea {
  margin-top: 16px;
  display: flex;
  justify-content: center;
}
</style>
