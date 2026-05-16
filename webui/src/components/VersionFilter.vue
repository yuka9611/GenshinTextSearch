<template>
  <div class="versionFilterGroup">
    <div class="versionFilterItem">
      <span class="versionFilterLabel">创建版本</span>
      <el-select
        v-model="localCreatedVersion"
        placeholder="创建版本"
        class="versionFilter"
        clearable
        filterable
      >
        <el-option v-for="version in createdOptions" :key="`created-${version}`" :label="version" :value="version" />
      </el-select>
    </div>
    <div class="versionFilterItem">
      <span class="versionFilterLabel">更新版本</span>
      <el-select
        v-model="localUpdatedVersion"
        placeholder="更新版本"
        class="versionFilter"
        clearable
        filterable
      >
        <el-option v-for="version in updatedOptions" :key="`updated-${version}`" :label="version" :value="version" />
      </el-select>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue'

const props = defineProps({
  createdVersion: {
    type: String,
    default: ''
  },
  updatedVersion: {
    type: String,
    default: ''
  },
  versionOptions: {
    type: Array,
    default: () => []
  },
  createdVersionOptions: {
    type: Array,
    default: () => []
  },
  updatedVersionOptions: {
    type: Array,
    default: () => []
  }
})

const emit = defineEmits(['update:createdVersion', 'update:updatedVersion', 'search'])

const localCreatedVersion = ref(props.createdVersion)
const localUpdatedVersion = ref(props.updatedVersion)
const createdOptions = computed(() => props.createdVersionOptions.length ? props.createdVersionOptions : props.versionOptions)
const updatedOptions = computed(() => props.updatedVersionOptions.length ? props.updatedVersionOptions : props.versionOptions)

watch(localCreatedVersion, (newValue) => {
  emit('update:createdVersion', newValue)
  emit('search')
})

watch(localUpdatedVersion, (newValue) => {
  emit('update:updatedVersion', newValue)
  emit('search')
})

watch(() => props.createdVersion, (newValue) => {
  localCreatedVersion.value = newValue
})

watch(() => props.updatedVersion, (newValue) => {
  localUpdatedVersion.value = newValue
})
</script>

<style scoped>
.versionFilterGroup {
  display: flex;
  flex-direction: row;
  align-items: end;
  gap: 12px;
  margin: 0;
  width: 100%;
}

.versionFilterItem {
  flex: 1 1 0;
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 0;
}

.versionFilterLabel {
  font-size: 0.75rem;
  color: var(--search-section-label-color, var(--theme-text-soft));
  text-transform: uppercase;
  letter-spacing: 0.05em;
  font-weight: 600;
}

.versionFilter {
  width: 100%;
  min-width: 0;
}

@media (max-width: 680px) {
  .versionFilterLabel {
    display: none;
  }
}
</style>
