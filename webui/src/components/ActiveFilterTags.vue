<script setup>
defineProps({
    filters: { type: Array, default: () => [] }
})

const emit = defineEmits(['clear-filter', 'clear-all'])
</script>

<template>
    <div v-if="filters.length > 0" class="activeFilters">
        <el-tag
            v-for="filter in filters"
            :key="filter.key"
            closable
            size="small"
            class="activeFilterTag"
            @close="emit('clear-filter', filter.key)"
        >
            {{ filter.label }}
        </el-tag>
        <button v-if="filters.length > 1" class="resetFiltersBtn" @click="emit('clear-all')">✕ 重置全部</button>
    </div>
</template>

<style scoped>
.activeFilters {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin-top: 12px;
}

.activeFilterTag {
    --el-tag-bg-color: var(--search-filter-tag-bg);
    --el-tag-border-color: var(--search-filter-tag-border);
    --el-tag-text-color: var(--search-filter-tag-text);
    border-radius: 999px;
    font-weight: 500;
    padding: 4px 10px 4px 12px;
}

.activeFilterTag :deep(.el-tag__close) {
    width: 16px;
    height: 16px;
    margin-left: 6px;
    border-radius: 50%;
    background: var(--search-filter-tag-close-bg);
    color: var(--search-filter-tag-text);
    font-size: 10px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    transition: background 0.15s ease;
}

.activeFilterTag :deep(.el-tag__close:hover) {
    background: var(--search-filter-tag-close-hover-bg);
}

.resetFiltersBtn {
    padding: 4px 12px;
    border-radius: 999px;
    border: 1px dashed var(--search-section-border);
    background: transparent;
    color: var(--theme-text-muted);
    font-size: 0.75rem;
    cursor: pointer;
    transition: all 0.15s ease;
    line-height: 1.5;
}

.resetFiltersBtn:hover {
    border-color: var(--theme-danger);
    color: var(--theme-danger);
}

@media (max-width: 680px) {
    .activeFilters {
        margin-top: 8px;
    }
}
</style>

<style>
[data-theme="dark"] .activeFilterTag {
    --el-tag-border-color: var(--search-filter-tag-border);
}
</style>
