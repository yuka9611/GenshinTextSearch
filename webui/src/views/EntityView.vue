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
  sourceCount: '来源数',
  textHash: 'Text Hash',
  language: '显示语言',
  empty: '未找到条目来源数据（可能需要更新数据库）',
  emptySources: '未找到可展示的来源文本',
  copy: '复制',
  copied: '已复制',
  noTextToCopy: '没有可复制的文本',
  copyFailed: '复制失败，请手动选择文本',
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

const textHash = computed(() => {
  const raw = route.query.textHash
  if (raw === undefined || raw === null) return null
  const n = Number(raw)
  return Number.isFinite(n) ? n : null
})

const isGroupedView = computed(() => textHash.value !== null)

const keyword = computed(() => String(route.query.keyword || ''))

const title = computed(() => {
  if (isGroupedView.value) return uiText.pageTitle
  return entity.value?.title || uiText.pageTitle
})
const entryCount = computed(() => entity.value?.entries?.length || 0)
const groups = computed(() => Array.isArray(entity.value?.groups) ? entity.value.groups : [])
const sourceCount = computed(() => Number(entity.value?.sourceCount) || groups.value.length || 0)
const displayRouteTextHash = computed(() => normalizeDisplayHash(textHash.value))
const emptyDescription = computed(() => {
  if (isGroupedView.value) {
    return entity.value?.emptyMessage || uiText.emptySources
  }
  return entity.value?.emptyMessage || uiText.empty
})
const isEmptyState = computed(() => {
  if (isGroupedView.value) {
    return groups.value.length === 0
  }
  return entryCount.value === 0
})

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

const normalizeCopyText = (text) => {
  if (!text) return ''
  const normalized = text.replace(/\\n/g, '\n')
  return normalized.replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim()
}

const isCopyableText = (text) => {
  return Boolean(normalizeCopyText(text))
}

const copyToClipboard = async (text) => {
  const normalized = normalizeCopyText(text)
  if (!normalized) {
    ElMessage.warning(uiText.noTextToCopy)
    return
  }

  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(normalized)
      ElMessage.success(uiText.copied)
      return
    }

    const textarea = document.createElement('textarea')
    textarea.value = normalized
    textarea.setAttribute('readonly', '')
    textarea.style.position = 'absolute'
    textarea.style.left = '-9999px'
    document.body.appendChild(textarea)
    textarea.select()
    document.execCommand('copy')
    document.body.removeChild(textarea)
    ElMessage.success(uiText.copied)
  } catch (error) {
    console.error(error)
    ElMessage.error(uiText.copyFailed)
  }
}

const normalizeDisplayHash = (value) => {
  if (typeof value === 'bigint') {
    return value > 0n ? value.toString() : ''
  }
  if (typeof value === 'number') {
    if (!Number.isInteger(value) || value <= 0) return ''
    return String(value)
  }
  if (typeof value === 'string') {
    const normalized = value.trim()
    return /^[1-9]\d*$/.test(normalized) ? normalized : ''
  }
  return ''
}

const resolveEntryTextHash = (entry) => {
  if (!entry || typeof entry !== 'object') return ''
  return normalizeDisplayHash(entry.textHash) || normalizeDisplayHash(entry.text?.hash)
}

const resolveEntries = (payload) => {
  if (!payload || !Array.isArray(payload.entries)) return []
  return payload.entries
}

const resolveGroupTitle = (group) => {
  if (group?.title) return String(group.title)
  if (group?.primarySource?.title) return String(group.primarySource.title)
  return uiText.pageTitle
}

const resolveGroupSubtitle = (group) => {
  const subtitle = group?.primarySource?.subtitle
  if (subtitle !== undefined && subtitle !== null && String(subtitle).trim() !== '') {
    return String(subtitle).trim()
  }
  const origin = group?.origin
  if (origin !== undefined && origin !== null && String(origin).trim() !== '') {
    return String(origin).trim()
  }
  return ''
}

const loadEntity = async () => {
  if (isGroupedView.value) {
    loading.value = true
    try {
      const ans = (await api.getTextEntitySources(textHash.value, selectedInputLanguage.value)).json
      entity.value = ans.contents || null
    } catch (_) {
      ElMessage.error('加载失败')
      entity.value = null
    } finally {
      loading.value = false
    }
    return
  }

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
        <div v-if="entity && !isGroupedView" class="entityMetaTags tagRow">
          <el-tag v-if="entity.sourceTypeLabel" effect="plain">{{ entity.sourceTypeLabel }}</el-tag>
          <el-tag v-if="entity.subCategoryLabel" effect="plain" type="info">{{ entity.subCategoryLabel }}</el-tag>
          <el-tag effect="plain">{{ uiText.entityId }}: {{ entity.entityId }}</el-tag>
        </div>
        <div v-else-if="entity && isGroupedView" class="entityMetaTags tagRow">
          <el-tag v-if="displayRouteTextHash" effect="plain">{{ uiText.textHash }}: {{ displayRouteTextHash }}</el-tag>
          <el-tag v-if="sourceCount" effect="plain" type="info">{{ uiText.sourceCount }}: {{ sourceCount }}</el-tag>
        </div>
        <h1 class="pageTitle">{{ title }}</h1>
        <div v-if="entity && !isGroupedView" class="versionTags tagRow">
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
      </div>
      <div class="headerRight">
        <el-select v-model="selectedInputLanguage" class="langSelect" :placeholder="uiText.language" filterable>
          <el-option v-for="(name, code) in supportedInputLanguage" :key="`lang-${code}`" :label="name" :value="code" />
        </el-select>
        <el-button class="controlButton entityBackButton" @click="router.back()">{{ uiText.back }}</el-button>
      </div>
    </div>

    <el-skeleton v-if="loading" :rows="6" animated />
    <el-empty v-else-if="!entity || isEmptyState" :description="emptyDescription" />

    <div v-else-if="isGroupedView" class="sourceGroups">
      <section
        v-for="group in groups"
        :key="`entity-group-${group.sourceTypeCode}-${group.entityId}`"
        class="sourceGroup"
      >
        <div class="sourceGroupHeader resultCard cardPanel">
          <div class="entityMetaTags tagRow">
            <el-tag v-if="group.sourceTypeLabel" effect="plain">{{ group.sourceTypeLabel }}</el-tag>
            <el-tag v-if="group.subCategoryLabel" effect="plain" type="info">{{ group.subCategoryLabel }}</el-tag>
            <el-tag effect="plain">{{ uiText.entityId }}: {{ group.entityId }}</el-tag>
          </div>
          <h2 class="groupTitle">{{ resolveGroupTitle(group) }}</h2>
          <p v-if="resolveGroupSubtitle(group)" class="groupSubtitle">{{ resolveGroupSubtitle(group) }}</p>
        </div>

        <div class="entityEntries">
          <el-card
            v-for="entry in resolveEntries(group)"
            :key="`entity-text-${group.sourceTypeCode}-${group.entityId}-${entry.readableId ?? entry.fileName ?? entry.textHash}-${entry.fieldLabel}`"
            class="entityCard resultCard cardPanel"
          >
            <div class="entityCardHeader">
              <div class="entityCardTitle">
                <el-tag size="small" effect="plain">{{ entry.fieldLabel }}</el-tag>
                <el-tag
                  v-if="entry.readableCategoryLabel && entry.readableCategoryLabel !== entry.fieldLabel"
                  size="small"
                  effect="plain"
                  type="success"
                >
                  {{ entry.readableCategoryLabel }}
                </el-tag>
                <el-tag v-if="resolveEntryTextHash(entry)" size="small" effect="plain" type="info">
                  {{ uiText.textHash }}: {{ resolveEntryTextHash(entry) }}
                </el-tag>
              </div>
              <button
                v-if="isCopyableText(resolveDisplayText(entry.text))"
                type="button"
                class="copyButton"
                :title="uiText.copy"
                @click="copyToClipboard(resolveDisplayText(entry.text))"
              >
                <i class="fi fi-rr-copy"></i>
              </button>
            </div>
            <div class="entityCardBody">
              <StylizedText :text="resolveDisplayText(entry.text)" :keyword="keyword" />
            </div>
          </el-card>
        </div>
      </section>
    </div>

    <div v-else class="entityEntries">
      <el-card
        v-for="entry in entity.entries"
        :key="`entity-text-${entry.readableId ?? entry.fileName ?? entry.textHash}-${entry.fieldLabel}`"
        class="entityCard resultCard cardPanel"
      >
        <div class="entityCardHeader">
          <div class="entityCardTitle">
            <el-tag size="small" effect="plain">{{ entry.fieldLabel }}</el-tag>
                <el-tag
                  v-if="entry.readableCategoryLabel && entry.readableCategoryLabel !== entry.fieldLabel"
                  size="small"
                  effect="plain"
                  type="success"
                >
                  {{ entry.readableCategoryLabel }}
                </el-tag>
            <el-tag v-if="resolveEntryTextHash(entry)" size="small" effect="plain" type="info">
              {{ uiText.textHash }}: {{ resolveEntryTextHash(entry) }}
            </el-tag>
          </div>
          <button
            v-if="isCopyableText(resolveDisplayText(entry.text))"
            type="button"
            class="copyButton"
            :title="uiText.copy"
            @click="copyToClipboard(resolveDisplayText(entry.text))"
          >
            <i class="fi fi-rr-copy"></i>
          </button>
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

.sourceGroups {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.sourceGroup {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.sourceGroupHeader {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.sourceGroupHeader:hover {
  transform: none;
}

.groupTitle {
  margin: 0;
  font-size: 20px;
}

.groupSubtitle {
  margin: 0;
  color: var(--theme-text-muted);
  font-size: 13px;
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

.copyButton {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  padding: 0;
  border-radius: 50%;
  border: 1px solid var(--theme-border);
  background: var(--theme-input);
  color: var(--theme-text-muted);
  font-size: 13px;
  cursor: pointer;
  transition: all 0.18s ease;
  line-height: 1;
  box-shadow: 0 4px 12px rgba(44, 57, 54, 0.06);
}

.copyButton i {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 14px;
  height: 14px;
  line-height: 1;
}

.copyButton:hover {
  color: var(--theme-accent);
  border-color: var(--theme-accent);
  background: var(--theme-accent-soft);
  transform: scale(1.08);
}

.copyButton:focus-visible {
  outline: none;
  color: var(--theme-accent);
  border-color: var(--theme-accent);
  background: var(--theme-accent-soft);
  box-shadow: 0 0 0 3px rgba(var(--theme-primary-rgb), 0.18);
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
