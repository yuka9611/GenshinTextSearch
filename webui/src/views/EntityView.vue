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
const entryCount = computed(() => entity.value?.entries?.length || 0)
const emptyDescription = computed(() => entity.value?.emptyMessage || uiText.empty)

const resolveVersionValue = (versionTag, rawVersion) => {
  if (versionTag) return String(versionTag).trim()
  if (rawVersion) return String(rawVersion).trim()
  return ''
}

const formatVersionTag = (versionTag, rawVersion) => {
  return resolveVersionValue(versionTag, rawVersion) || '未知'
}

const shouldShowUpdatedVersionTag = (createdTag, createdRaw, updatedTag, updatedRaw) => {
  const updatedValue = resolveVersionValue(updatedTag, updatedRaw)
  if (!updatedValue) return false
  return updatedValue !== resolveVersionValue(createdTag, createdRaw)
}

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
  <div class="viewWrapper pageShell entityView">
    <div class="pageHeader entityHeader">
      <div class="headerLeft">
        <div v-if="entity" class="entityMetaTags tagRow">
          <el-tag v-if="entity.sourceTypeLabel" effect="plain">{{ entity.sourceTypeLabel }}</el-tag>
          <el-tag v-if="entity.subCategoryLabel" effect="plain" type="info">{{ entity.subCategoryLabel }}</el-tag>
          <el-tag effect="plain">{{ uiText.entityId }}: {{ entity.entityId }}</el-tag>
        </div>
        <h1 class="pageTitle">{{ title }}</h1>
        <div v-if="entity" class="versionTags tagRow">
          <span v-if="resolveVersionValue(entity.createdVersion, entity.createdVersionRaw)" class="versionTag created" :title="entity.createdVersionRaw || ''">
            ✦ 创建: {{ formatVersionTag(entity.createdVersion, entity.createdVersionRaw) }}
          </span>
          <span
            v-if="shouldShowUpdatedVersionTag(entity.createdVersion, entity.createdVersionRaw, entity.updatedVersion, entity.updatedVersionRaw)"
            class="versionTag updated"
            :title="entity.updatedVersionRaw || ''"
          >
            ↻ 更新: {{ formatVersionTag(entity.updatedVersion, entity.updatedVersionRaw) }}
          </span>
        </div>
        <div v-if="entryCount" class="pageMeta">共 {{ entryCount }} 个字段文本</div>
      </div>
      <div class="headerRight">
        <el-select v-model="selectedInputLanguage" class="langSelect" :placeholder="uiText.language" filterable>
          <el-option v-for="(name, code) in supportedInputLanguage" :key="`lang-${code}`" :label="name" :value="code" />
        </el-select>
        <el-button class="controlButton entityBackButton" @click="router.back()">{{ uiText.back }}</el-button>
      </div>
    </div>

    <el-skeleton v-if="loading" :rows="6" animated />
    <el-empty v-else-if="!entity || !entity.entries || entity.entries.length === 0" :description="emptyDescription" />

    <div v-else class="entityEntries">
      <el-card
        v-for="entry in entity.entries"
        :key="`entity-text-${entry.readableId ?? entry.fileName ?? entry.textHash}-${entry.fieldLabel}`"
        class="entityCard resultCard cardPanel"
      >
        <div class="entityCardHeader">
          <div class="entityCardTitle">
            <el-tag size="small" effect="plain">{{ entry.fieldLabel }}</el-tag>
            <span class="entityCardSubtitle">{{ entry.subtitle }}</span>
          </div>
        </div>
        <div class="entityCardBody">
          <StylizedText :text="resolveDisplayText(entry.text)" :keyword="keyword" />
        </div>
      </el-card>
    </div>
  </div>
</template>

<style scoped>
.entityView {
  gap: 16px;
}

.pageHeader {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.headerLeft {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.headerRight {
  display: inline-flex;
  align-items: stretch;
  gap: 10px;
  flex-wrap: wrap;
}

.pageTitle {
  margin: 0;
}

.pageMeta {
  color: var(--theme-text-muted);
  font-size: 13px;
}

.entityMetaTags {
  margin-bottom: 2px;
}

.langSelect {
  min-width: 160px;
}

.entityBackButton {
  align-self: stretch;
  min-height: 34px;
  padding: 0 14px;
  font-size: 13px;
}

.entityEntries {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.entityCard {
  gap: 12px;
}

.entityCard:hover {
  transform: none;
}

.entityCardHeader {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 0;
  padding-bottom: 12px;
  border-bottom: 1px solid rgba(190, 164, 124, 0.18);
}

.entityCardTitle {
  display: inline-flex;
  align-items: center;
  gap: 8px;
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

@media (max-width: 680px) {
  .headerRight {
    width: 100%;
  }

  .langSelect {
    min-width: 0;
    flex: 1 1 180px;
  }
}
</style>
