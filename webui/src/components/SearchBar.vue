<template>
  <div class="searchBar">
    <el-input
      v-model="localKeyword"
      :style="{ maxWidth: inputWidth }"
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
      <template #append>
        <el-button :icon="Search" @click="onSearch" />
      </template>
    </el-input>
    <span v-if="summary" class="searchSummary">{{ summary }}</span>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import { Search } from '@element-plus/icons-vue'

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
    default: '600px'
  },
  languagePlaceholder: {
    type: String,
    default: '选择语言'
  }
})

const emit = defineEmits(['update:keyword', 'update:selectedLanguage', 'search'])

const localKeyword = ref(props.keyword)
const localSelectedLanguage = ref(props.selectedLanguage)

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
  emit('search')
}
</script>

<style scoped>
.searchBar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 12px;
  padding-bottom: 6px;
  box-sizing: border-box;
}

.searchBar :deep(.input-with-select) {
  flex: 1 1 560px;
}

.languageSelector {
  width: 120px;
}

.languageSelector:deep(input) {
  text-align: center;
}

.searchSummary {
  display: inline-flex;
  align-items: center;
  min-height: 38px;
  padding: 0 14px;
  border-radius: 999px;
  background: rgba(47, 105, 101, 0.08);
  color: var(--theme-text-muted);
  font-size: 13px;
  border: 1px solid rgba(47, 105, 101, 0.12);
}

@media (max-width: 680px) {
  .searchBar :deep(.input-with-select) {
    flex-basis: 100%;
  }

  .searchSummary {
    width: 100%;
    min-height: 34px;
    padding: 8px 12px;
  }
}
</style>
