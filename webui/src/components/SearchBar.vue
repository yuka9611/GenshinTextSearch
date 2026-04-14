<template>
  <div class="searchBlock">
    <div class="searchBar">
      <div class="searchMainRow">
        <el-input
          v-model="localKeyword"
          :style="inputStyle"
          :placeholder="inputPlaceholder"
          class="input-with-select"
          @keyup.enter.native="onSearch"
          clearable
        >
          <template #prepend>
            <el-select v-model="localSelectedLanguage" :placeholder="languagePlaceholder" class="languageSelector">
              <el-option v-for="(v, k) in supportedLanguages" :label="v" :value="k" :key="k" />
            </el-select>
          </template>
        </el-input>
        <button class="searchBtn" @click="onSearch">
          <i class="fi fi-rr-search"></i>
          <span>搜索</span>
        </button>
      </div>
    </div>
    <div v-if="historyList.length > 0" class="searchHistory">
      <span class="historyLabel">最近搜索:</span>
      <span
        v-for="(item, index) in historyList"
        :key="index"
        class="historyTag"
        @click="onHistoryClick(item)"
      >{{ item }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, onMounted, computed } from 'vue'

const HISTORY_KEY = 'genshin-search-history'
const MAX_HISTORY = 10

const props = defineProps({
  keyword: {
    type: [String, Number],
    default: ''
  },
  selectedLanguage: {
    type: [String, Number],
    default: ''
  },
  supportedLanguages: {
    type: Object,
    default: () => {}
  },
  summary: {
    type: String,
    default: ''
  },
  inputPlaceholder: {
    type: String,
    default: '输入关键词'
  },
  inputWidth: {
    type: String,
    default: ''
  },
  languagePlaceholder: {
    type: String,
    default: '选择语言'
  },
  historyKey: {
    type: String,
    default: ''
  }
})

const resolvedHistoryKey = computed(() => {
  return props.historyKey ? `${HISTORY_KEY}-${props.historyKey}` : HISTORY_KEY
})

const inputStyle = computed(() => {
  return props.inputWidth ? { maxWidth: props.inputWidth } : undefined
})

const emit = defineEmits(['update:keyword', 'update:selectedLanguage', 'search'])

const localKeyword = ref(props.keyword)
const localSelectedLanguage = ref(props.selectedLanguage)
const historyList = ref([])

const loadHistory = () => {
  try {
    const raw = localStorage.getItem(resolvedHistoryKey.value)
    historyList.value = raw ? JSON.parse(raw) : []
  } catch {
    historyList.value = []
  }
}

const saveToHistory = (term) => {
  const trimmed = String(term || '').trim()
  if (!trimmed) return
  const list = historyList.value.filter(item => item !== trimmed)
  list.unshift(trimmed)
  if (list.length > MAX_HISTORY) list.length = MAX_HISTORY
  historyList.value = list
  try {
    localStorage.setItem(resolvedHistoryKey.value, JSON.stringify(list))
  } catch { /* quota exceeded — ignore */ }
}

const onHistoryClick = (term) => {
  localKeyword.value = term
  emit('update:keyword', term)
  emit('search')
}

onMounted(loadHistory)

watch(localKeyword, (newValue) => {
  emit('update:keyword', newValue)
})

watch(() => props.keyword, (newValue) => {
  localKeyword.value = newValue
})

watch(localSelectedLanguage, (newValue) => {
  emit('update:selectedLanguage', newValue)
})

watch(() => props.selectedLanguage, (newValue) => {
  localSelectedLanguage.value = newValue
})

const onSearch = () => {
  saveToHistory(localKeyword.value)
  emit('search')
}
</script>

<style scoped>
.searchBlock {
  margin-bottom: 16px;
}

.searchBar {
  --search-control-height: 40px;
  display: flex;
  flex-direction: column;
  gap: 0;
  box-sizing: border-box;
}

.searchMainRow {
  display: flex;
  align-items: stretch;
  gap: 12px;
  min-width: 0;
}

.searchMainRow :deep(.input-with-select) {
  --el-input-height: var(--search-control-height);
  flex: 1 1 0;
  display: flex;
  min-width: 0;
  border: 1px solid var(--search-section-border);
  border-radius: 16px;
  background: var(--search-section-input-bg);
  overflow: hidden;
  transition: border-color 0.18s ease, box-shadow 0.18s ease, background 0.18s ease;
}

.searchMainRow :deep(.input-with-select:hover) {
  border-color: var(--search-section-border-hover);
}

.searchMainRow :deep(.input-with-select:focus-within) {
  border-color: var(--search-section-border-focus);
  box-shadow: 0 0 0 4px var(--search-section-focus-ring);
}

.searchMainRow :deep(.input-with-select .el-input-group__prepend) {
  display: inline-flex;
  align-items: stretch;
  border-radius: 0;
  border: none;
  border-right: 1px solid var(--search-section-border);
  background: var(--search-section-highlight-bg);
  box-shadow: none;
}

.searchMainRow :deep(.input-with-select .el-input-group__prepend .el-select),
.searchMainRow :deep(.input-with-select .el-input-group__prepend .el-input),
.searchMainRow :deep(.input-with-select .el-input__wrapper),
.searchMainRow :deep(.input-with-select .el-select .el-input__wrapper) {
  min-height: var(--search-control-height);
  height: var(--search-control-height);
}

.searchMainRow :deep(.input-with-select .el-input__wrapper) {
  border-radius: 0;
  border: none;
  background: transparent;
  box-shadow: none !important;
}

.searchMainRow :deep(.input-with-select .el-input__inner) {
  color: var(--theme-text);
}

.searchMainRow :deep(.input-with-select .el-input__inner::placeholder) {
  color: var(--search-section-label-color);
}

.searchBtn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  min-height: var(--search-control-height);
  height: var(--search-control-height);
  padding: 0 24px;
  border: none;
  border-radius: 999px;
  background: linear-gradient(135deg, var(--theme-primary), var(--theme-primary-strong));
  color: #fff;
  font-size: 0.8125rem;
  font-weight: 600;
  cursor: pointer;
  box-shadow: 0 8px 20px rgba(47, 105, 101, 0.25);
  transition: transform 0.2s ease, box-shadow 0.2s ease;
  white-space: nowrap;
  flex-shrink: 0;
}

.searchBtn:hover {
  transform: translateY(-1px);
  box-shadow: 0 12px 28px rgba(47, 105, 101, 0.32);
}

.searchBtn .fi {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 14px;
  height: 14px;
  font-size: 14px;
  line-height: 1;
}

.languageSelector {
  width: 120px;
}

.languageSelector:deep(input) {
  text-align: center;
}

:global([data-theme="dark"] .searchMainRow .input-with-select:focus-within) {
  box-shadow: 0 0 0 4px rgba(74, 154, 149, 0.10);
}

:global([data-theme="dark"] .searchBtn) {
  color: #141c1b;
  box-shadow: 0 8px 20px rgba(74, 154, 149, 0.20);
}

.searchHistory {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 8px;
}

.historyLabel {
  font-size: 0.75rem;
  color: var(--search-section-label-color);
}

.historyTag {
  padding: 3px 10px;
  border-radius: 999px;
  background: var(--search-section-muted-bg);
  color: var(--theme-text-muted);
  font-size: 0.75rem;
  cursor: pointer;
  transition: all 0.15s ease;
  border: 1px solid transparent;
}

.historyTag:hover {
  color: var(--theme-primary);
  border-color: var(--search-section-border-hover);
  background: var(--theme-primary-soft);
}

@media (max-width: 860px) {
  .searchBlock {
    margin-bottom: 12px;
  }

  .searchBar {
    --search-control-height: 40px;
  }

  .searchMainRow {
    gap: 8px;
  }

  .searchBtn {
    padding: 0 20px;
  }
}

@media (max-width: 760px) {
  .searchBtn {
    padding: 0 18px;
  }
}

@media (max-width: 680px) {
  .searchBlock {
    margin-bottom: 8px;
  }

  .searchBar {
    --search-control-height: 38px;
  }

  .searchMainRow {
    flex-wrap: wrap;
    gap: 8px;
  }

  .searchMainRow :deep(.input-with-select) {
    flex-basis: 100%;
  }

  .searchBtn {
    width: 100%;
    justify-content: center;
    border-radius: 8px;
    padding: 0 16px;
  }

  .searchHistory {
    display: none;
  }
}
</style>
