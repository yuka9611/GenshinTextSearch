<template>
  <div class="filterBar">
    <el-select 
      v-model="localCreatedVersion" 
      placeholder="创建版本" 
      class="versionInput" 
      clearable 
      filterable
    >
      <el-option v-for="version in versionOptions" :key="`created-${version}`" :label="version" :value="version" />
    </el-select>
    <el-select 
      v-model="localUpdatedVersion" 
      placeholder="更新版本" 
      class="versionInput" 
      clearable 
      filterable
    >
      <el-option v-for="version in versionOptions" :key="`updated-${version}`" :label="version" :value="version" />
    </el-select>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'

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
  }
})

const emit = defineEmits(['update:createdVersion', 'update:updatedVersion', 'search'])

const localCreatedVersion = ref(props.createdVersion)
const localUpdatedVersion = ref(props.updatedVersion)

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
.filterBar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin: 0;
}

.versionInput {
  width: 150px;
}

@media (max-width: 720px) {
  .versionInput {
    margin-left: 0;
    display: block;
  }
}
</style>