<script setup>
import { onBeforeMount, ref, watch, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import api from '@/api/keywordQuery'
import StylizedText from '@/components/StylizedText.vue'
import useLanguage from '@/composables/useLanguage'

const uiText = {
  pageTitle: '来源详情',
  back: '返回',
  entityId: 'ID',
  language: '显示语言',
  empty: '未找到条目来源数据（可能需要更新数据库）',
  viewText: '查看文本',
}

const route = useRoute()
const router = useRouter()

const { selectedInputLanguage, supportedInputLanguage, loadLanguages } = useLanguage()

const loading = ref(false)
const entity = ref(null)

const sourceTypeCode = computed(() => {
  const raw = route.query.sourceTypeCode
  if (raw === undefined || raw === null) return null
  const n = Number(raw)
  return Number.isFinite(n) ? n : null
})

const entityId = computed(() => {
  const raw = route.query.entityId
  if (raw === undefined || raw === null) return null
  const n = Number(raw)
  return Number.isFinite(n) ? n : null
})

const keyword = computed(() => String(route.query.keyword || ''))

const title = computed(() => entity.value?.title || uiText.pageTitle)
const metaLabel = computed(() => {
  const main = entity.value?.sourceTypeLabel || ''
  const sub = entity.value?.subCategoryLabel || ''
  return sub ? `${main} · ${sub}` : main
})

const resolveDisplayText = (textObj) => {
  if (!textObj || !textObj.translates) return ''
  const langKey = String(selectedInputLanguage.value)
  if (textObj.translates[langKey]) return textObj.translates[langKey]
  const keys = Object.keys(textObj.translates)
  if (keys.length === 0) return ''
  return textObj.translates[keys[0]] || ''
}

const loadEntity = async () => {
  if (sourceTypeCode.value === null || entityId.value === null) {
    entity.value = null
    return
  }
  loading.value = true
  try {
    const ans = (await api.getEntityTexts(sourceTypeCode.value, entityId.value, selectedInputLanguage.value)).json
    entity.value = ans.contents || null
  } catch (_) {
    ElMessage.error('加载失败')
    entity.value = null
  } finally {
    loading.value = false
  }
}

const gotoText = (entry) => {
  const detail = entry?.detailQuery
  if (detail?.kind === 'readable') {
    router.push({
      path: '/talk',
      query: {
        readableId: detail.readableId ?? entry?.readableId,
        fileName: detail.fileName ?? entry?.fileName,
        keyword: keyword.value,
        searchLang: selectedInputLanguage.value,
      },
    })
    return
  }

  const textHash = detail?.textHash ?? entry?.textHash
  if (textHash === undefined || textHash === null) return

  router.push({
    path: '/talk',
    query: {
      textHash,
      keyword: keyword.value,
      searchLang: selectedInputLanguage.value,
    },
  })
}

onBeforeMount(async () => {
  await loadLanguages()
  if (route.query.searchLang !== undefined && route.query.searchLang !== null && String(route.query.searchLang).trim() !== '') {
    selectedInputLanguage.value = String(route.query.searchLang)
  }
  await loadEntity()
})

watch(() => route.fullPath, async () => {
  await loadEntity()
})

watch(selectedInputLanguage, async () => {
  await loadEntity()
})
</script>

<template>
  <div class="viewWrapper pageShell">
    <div class="pageHeader">
      <div class="headerLeft">
        <h1 class="pageTitle">{{ title }}</h1>
        <div v-if="metaLabel" class="pageMeta">{{ metaLabel }} · {{ uiText.entityId }} {{ entity?.entityId }}</div>
      </div>
      <div class="headerRight">
        <el-select v-model="selectedInputLanguage" class="langSelect" :placeholder="uiText.language" filterable>
          <el-option v-for="(name, code) in supportedInputLanguage" :key="`lang-${code}`" :label="name" :value="code" />
        </el-select>
        <el-button size="small" @click="router.back()">{{ uiText.back }}</el-button>
      </div>
    </div>

    <el-skeleton v-if="loading" :rows="6" animated />
    <el-empty v-else-if="!entity || !entity.entries || entity.entries.length === 0" :description="uiText.empty" />

    <div v-else class="entityEntries">
      <el-card
        v-for="entry in entity.entries"
        :key="`entity-text-${entry.readableId ?? entry.fileName ?? entry.textHash}-${entry.fieldLabel}`"
        class="entityCard cardPanel"
      >
        <div class="entityCardHeader">
          <div class="entityCardTitle">
            <el-tag size="small" effect="plain">{{ entry.fieldLabel }}</el-tag>
            <span class="entityCardSubtitle">{{ entry.subtitle }}</span>
          </div>
          <el-button size="small" type="primary" @click="gotoText(entry)">{{ uiText.viewText }}</el-button>
        </div>
        <div class="entityCardBody">
          <StylizedText :text="resolveDisplayText(entry.text)" :keyword="keyword" />
        </div>
      </el-card>
    </div>
  </div>
</template>

<style scoped>
.pageHeader {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
  margin-bottom: 18px;
}

.headerLeft {
  min-width: 0;
}

.headerRight {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.pageTitle {
  margin: 0;
}

.pageMeta {
  margin-top: 6px;
  color: var(--theme-text-muted);
  font-size: 13px;
}

.langSelect {
  min-width: 160px;
}

.entityEntries {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.entityCardHeader {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 10px;
}

.entityCardTitle {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  min-width: 0;
}

.entityCardSubtitle {
  color: var(--theme-text-muted);
  font-size: 13px;
}

.entityCardBody :deep(p) {
  margin: 0;
}
</style>
