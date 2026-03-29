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
        <el-select v-model="localSelectedLanguage" placeholder="选择语言" class="languageSelector">
          <el-option v-for="(v, k) in supportedLanguages" :label="v" :value="k" :key="k" />
        </el-select>
      </template>
      <template #append>
        <el-button :icon="Search" @click="onSearch" />
      </template>
    </el-input>
    <span class="searchSummary">{{ summary }}</span>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import { Search } from '@element-plus/icons-vue'

const props = defineProps({
  keyword: {
    type: String,
    default: ''
  },
  selectedLanguage: {
    type: String,
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

@media (max-width: 720px) {
  .searchSummary {
    display: block;
    margin-left: 0;
    margin-top: 8px;
  }
}
</style>
