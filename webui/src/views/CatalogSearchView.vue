<script setup>
import { ref, computed, onBeforeMount, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '@/api/keywordQuery'
import useLanguage from '@/composables/useLanguage'
import useVersion from '@/composables/useVersion'
import SearchBar from '@/components/SearchBar.vue'
import VersionFilter from '@/components/VersionFilter.vue'
import ActiveFilterTags from '@/components/ActiveFilterTags.vue'

const router = useRouter()
const { selectedInputLanguage, supportedInputLanguage, loadLanguages } = useLanguage()
const { versionOptions, loadVersionOptions } = useVersion()

const keyword = ref('')
const createdVersionFilter = ref('')
const updatedVersionFilter = ref('')
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
const subCategoryGroups = ref({})
const uncategorizedSubCategory = ref({ value: '0', label: '其他' })

const totalPages = computed(() => Math.max(1, Math.ceil(totalCount.value / pageSize.value)))
const suppressAutoSearch = ref(false)

const subCategoryOptions = computed(() => {
  const entries = Object.entries(subCategories.value)
  if (!entries.length) return []

  const selectedMain = String(selectedMainCategory.value || '').trim()
  if (!selectedMain) {
    return entries.map(([code, label]) => ({ value: code, label }))
  }

  const allowedCodes = Array.isArray(subCategoryGroups.value[selectedMain])
    ? subCategoryGroups.value[selectedMain].map(code => String(code))
    : []
  const allowedSet = new Set(allowedCodes)
  const options = entries
    .filter(([code]) => allowedSet.has(String(code)))
    .map(([code, label]) => ({ value: code, label }))

  const uncategorizedValue = String(uncategorizedSubCategory.value?.value || '0')
  const uncategorizedLabel = uncategorizedSubCategory.value?.label || '其他'
  if (allowedSet.has(uncategorizedValue)) {
    options.push({ value: uncategorizedValue, label: uncategorizedLabel })
  }

  return options
})

const hasSearchCriteria = computed(() => {
  return Boolean(
    keyword.value.trim() ||
    selectedMainCategory.value ||
    selectedSubCategory.value ||
    createdVersionFilter.value.trim() ||
    updatedVersionFilter.value.trim()
  )
})

const displayVersion = (item, kind) => {
  const v = kind === 'created'
    ? (item.createdVersion || item.createdVersionRaw)
    : (item.updatedVersion || item.updatedVersionRaw)
  return v ? String(v).trim() : '未知'
}

const showUpdatedVersionTag = (item) => {
  const updated = item.updatedVersion || item.updatedVersionRaw
  if (!updated) return false
  const created = item.createdVersion || item.createdVersionRaw
  return String(updated).trim() !== String(created || '').trim()
}

const loadMeta = async () => {
  try {
    const ans = (await api.getCatalogMeta()).json
    mainCategories.value = ans.mainCategories || {}
    subCategories.value = ans.subCategories || {}
    subCategoryGroups.value = ans.subCategoryGroups || {}
    uncategorizedSubCategory.value = ans.uncategorizedSubCategory || { value: '0', label: '其他' }
  } catch (_) {
    // ignore
  }
}

const clearSearchState = () => {
  results.value = []
  totalCount.value = 0
  currentPage.value = 1
  searchSummary.value = ''
}

const doSearch = async (page = 1) => {
  const kw = keyword.value.trim()
  const stc = selectedMainCategory.value || null
  const sub = selectedSubCategory.value || null

  if (!hasSearchCriteria.value) {
    clearSearchState()
    return
  }

  const langCode = parseInt(selectedInputLanguage.value)
  if (!selectedInputLanguage.value || isNaN(langCode)) {
    ElMessage.warning('请先选择语言')
    return
  }

  isLoading.value = true
  try {
    const ans = (await api.catalogSearch(kw, langCode, stc, sub, page, pageSize.value, createdVersionFilter.value, updatedVersionFilter.value)).json
    results.value = ans.contents || []
    totalCount.value = ans.total || 0
    currentPage.value = ans.page || page
    const timeMs = typeof ans.time === 'number' ? ans.time.toFixed(2) : '0.00'
    searchSummary.value = `共 ${totalCount.value} 条结果，耗时 ${timeMs}ms`
  } catch (_) {
    ElMessage.error('搜索失败')
    clearSearchState()
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

const onPageSizeChange = (size) => {
  pageSize.value = size
  currentPage.value = 1
  doSearch(1)
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
  await loadVersionOptions()
})

const batchFilterUpdate = (callback) => {
  suppressAutoSearch.value = true
  try {
    callback()
  } finally {
    suppressAutoSearch.value = false
  }
  handleSearch()
}

watch(selectedMainCategory, () => {
  if (suppressAutoSearch.value) return
  suppressAutoSearch.value = true
  selectedSubCategory.value = ''
  suppressAutoSearch.value = false
  handleSearch()
})
watch(selectedSubCategory, () => {
  if (suppressAutoSearch.value) return
  handleSearch()
})
watch(selectedInputLanguage, () => {
  if (suppressAutoSearch.value) return
  handleSearch()
})

const activeFilters = computed(() => {
  const filters = []
  if (selectedMainCategory.value) {
    const label = mainCategories.value[selectedMainCategory.value] || selectedMainCategory.value
    filters.push({ key: 'mainCategory', label: `大分类: ${label}` })
  }
  if (selectedSubCategory.value) {
    const opt = subCategoryOptions.value.find(o => o.value === selectedSubCategory.value)
    filters.push({ key: 'subCategory', label: `二级分类: ${opt?.label || selectedSubCategory.value}` })
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
    mainCategory: () => { selectedMainCategory.value = '' },
    subCategory: () => { selectedSubCategory.value = '' },
    createdVersion: () => { createdVersionFilter.value = '' },
    updatedVersion: () => { updatedVersionFilter.value = '' },
  }
  const action = map[key]
  if (!action) return
  batchFilterUpdate(action)
}

const clearAllFilters = () => {
  batchFilterUpdate(() => {
    selectedMainCategory.value = ''
    selectedSubCategory.value = ''
    createdVersionFilter.value = ''
    updatedVersionFilter.value = ''
  })
}
</script>

<template>
  <div class="viewWrapper">
    <div class="stickySearchSection">
      <h2 class="pageTitle">图鉴搜索</h2>
      <SearchBar
        v-model:keyword="keyword"
        v-model:selectedLanguage="selectedInputLanguage"
        :supportedLanguages="supportedInputLanguage"
        inputPlaceholder="输入物品名称"
        historyKey="catalog"
        @search="handleSearch"
      />

      <div class="filterBar">
        <div class="filterItem">
          <span class="filterLabel">大分类</span>
          <el-select v-model="selectedMainCategory" placeholder="全部大分类" clearable>
            <el-option label="全部大分类" value="" />
            <el-option v-for="(label, code) in mainCategories" :key="`cat-${code}`" :label="label" :value="code" />
          </el-select>
        </div>

        <div class="filterItem">
          <span class="filterLabel">二级分类</span>
          <el-select v-model="selectedSubCategory" placeholder="全部二级分类" clearable>
            <el-option label="全部二级分类" value="" />
            <el-option v-for="opt in subCategoryOptions" :key="`sub-${opt.value}`" :label="opt.label" :value="opt.value" />
          </el-select>
        </div>

        <VersionFilter
          :versionOptions="versionOptions"
          v-model:createdVersion="createdVersionFilter"
          v-model:updatedVersion="updatedVersionFilter"
          @search="handleSearch"
        />
      </div>

      <ActiveFilterTags
        :filters="activeFilters"
        @clear-filter="clearFilter"
        @clear-all="clearAllFilters"
      />
    </div>

    <div v-if="totalCount > 0" class="resultSummary">
      <span class="resultCount">
        搜索 "<strong>{{ keyword || '全部' }}</strong>" 共 <strong>{{ totalCount }}</strong> 条结果
      </span>
    </div>

    <div v-if="isLoading" class="loadingArea">
      <el-skeleton :rows="6" animated />
    </div>

    <div v-else class="resultSection resultsSection">
      <el-empty v-if="hasSearchCriteria && results.length === 0" description="暂无结果" />
      <div v-else class="resultGrid cardGrid">
        <el-card
          v-for="item in results"
          :key="`${item.sourceTypeCode}-${item.entityId}`"
          class="resultCard cardPanel"
        >
          <div class="cardTitle cardTitleText">{{ item.title }}</div>
          <div class="cardMeta cardMetaText">ID: {{ item.entityId }}</div>
          <div class="versionTags tagRow">
            <el-tag v-if="item.sourceTypeLabel" size="small" effect="plain" type="success">{{ item.sourceTypeLabel }}</el-tag>
            <el-tag v-if="item.subCategoryLabel" size="small" effect="plain" type="info">{{ item.subCategoryLabel }}</el-tag>
            <span class="versionTag created">✦ 创建: {{ displayVersion(item, 'created') }}</span>
            <span v-if="showUpdatedVersionTag(item)" class="versionTag updated">↻ 更新: {{ displayVersion(item, 'updated') }}</span>
          </div>
          <el-button size="small" type="primary" @click="gotoEntity(item)">查看详情</el-button>
        </el-card>
      </div>
    </div>

    <el-pagination
      v-if="totalCount > 0"
      class="resultPagination"
      v-model:current-page="currentPage"
      v-model:page-size="pageSize"
      :page-sizes="[20, 50, 100]"
      :total="totalCount"
      layout="prev, pager, next, sizes"
      @current-change="handlePageChange"
      @size-change="onPageSizeChange"
    />
  </div>
</template>

<style scoped>
.loadingArea {
  margin-top: 16px;
}

/* VersionFilter spans 2 grid columns in the 5-col filterBar */
:deep(.versionFilterGroup) {
  grid-column: span 2;
}

/* CatalogSearchView: 2 category filters + VersionFilter (span 2) = 4 columns */
.filterBar {
  grid-template-columns: repeat(2, minmax(0, 1fr)) repeat(2, minmax(0, 0.85fr));
}

@media (max-width: 860px) {
  .filterBar {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }
}

@media (max-width: 680px) {
  .filterBar {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
