<template>
    <div class="viewWrapper">
        <div class="stickySearchSection">
            <h1 class="pageTitle">关键词搜索</h1>
            <div class="helpText">
                <p>支持关键词、说话人、来源类别、语音存在性、创建版本、更新版本组合筛选。</p>
                <p>当关键词留空时，只要填写了说话人、来源类别或版本也可以查询。</p>
            </div>
            <SearchBar
                v-model:keyword="keyword"
                v-model:selectedLanguage="selectedInputLanguage"
                :supportedLanguages="supportedInputLanguage"
                historyKey="text"
                @search="handleSearch"
            />

            <div class="filterBar">
                <div class="filterItem">
                    <span class="filterLabel">说话人</span>
                    <el-input
                        v-model="speakerKeyword"
                        placeholder="角色名..."
                        class="speakerInput"
                        @keyup.enter.native="handleSearch"
                        clearable
                    />
                </div>

                <div class="filterItem">
                    <span class="filterLabel">语音</span>
                    <el-select
                        v-model="voiceFilter"
                        class="voiceFilter"
                        placeholder="语音筛选"
                        @change="handleSearch"
                    >
                        <el-option label="全部(语音)" value="all" />
                        <el-option label="有语音" value="with" />
                        <el-option label="无语音" value="without" />
                    </el-select>
                </div>

                <div class="filterItem">
                    <span class="filterLabel">来源类型</span>
                    <el-select
                        v-model="sourceTypeFilter"
                        class="sourceFilter"
                        placeholder="来源类别"
                        clearable
                        @change="handleSearch"
                    >
                        <el-option
                            v-for="option in sourceTypeOptions"
                            :key="`source-type-${option.value || 'all'}`"
                            :label="option.label"
                            :value="option.value"
                        />
                    </el-select>
                </div>

                <VersionFilter
                    v-model:createdVersion="createdVersionFilter"
                    v-model:updatedVersion="updatedVersionFilter"
                    :versionOptions="versionOptions"
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
                搜索 "<strong>{{ keywordLast || '全部' }}</strong>" 共 <strong>{{ totalCount }}</strong> 条结果
            </span>
        </div>

        <div v-if="isLoading" class="loading-container">
            <el-skeleton :rows="5" animated />
        </div>

        <div v-else-if="hasSearched && queryResult.length === 0 && totalCount === 0" class="no-results">
            <el-empty description="未找到结果" />
        </div>

        <div v-else>
            <TranslateDisplay
                v-for="translate in queryResult"
                :key="`${translate.hash}-${translate.origin || ''}`"
                :translate-obj="translate"
                class="translate textResultItem"
                @onVoicePlay="onVoicePlay"
                :keyword="keywordLast"
                :search-lang="searchLangLast"
            />
        </div>

        <el-pagination
            v-if="totalCount > 0"
            class="resultPagination"
            v-model:current-page="currentPage"
            v-model:page-size="pageSize"
            :page-sizes="[20, 50, 100]"
            :total="totalCount"
            layout="prev, pager, next, sizes"
            @current-change="goToPage"
            @size-change="onPageSizeChange"
        />
    </div>

    <div class="viewWrapper pageShell pageShell--compact voicePlayerContainer audioDock" v-show="showPlayer && queryResult.length > 0">
        <span class="hideIcon audioDockClose" @click="onHidePlayerButtonClicked">
            <el-icon>
                <Close />
            </el-icon>
        </span>

        <AudioPlayer
            ref="voicePlayer"
            :audio-list="audio"
            :show-prev-button="false"
            :show-next-button="false"
            :is-loop="false"
            :progress-interval="25"
            theme-color="var(--el-color-primary)"
        />
    </div>

    <div class="showPlayerButton audioDockToggle" @click="onShowPlayerButtonClicked" v-show="!showPlayer && queryResult.length > 0">
        <i class="fi fi-sr-waveform-path"></i>
    </div>
</template>

<script setup>
import { onBeforeMount, computed, ref } from 'vue'
import { Close } from '@element-plus/icons-vue'
import TranslateDisplay from '@/components/ResultEntry.vue'
import AudioPlayer from '@liripeng/vue-audio-player'
import SearchBar from '@/components/SearchBar.vue'
import VersionFilter from '@/components/VersionFilter.vue'
import ActiveFilterTags from '@/components/ActiveFilterTags.vue'
import useSearch from '@/composables/useSearch'
import useAudioPlayer from '@/composables/useAudioPlayer'



// 使用搜索组合式API
const {
  queryResult,
  keyword,
  keywordLast,
  speakerKeyword,
  voiceFilter,
  sourceTypeFilter,
  createdVersionFilter,
  updatedVersionFilter,
  versionOptions,
  sourceTypeOptions,
  selectedInputLanguage,
  supportedInputLanguage,
  searchLangLast,
  currentPage,
  pageSize,
  totalCount,
  isLoading,
  loadVersionOptions,
  onQueryButtonClicked,
  goToPage
} = useSearch()

const hasSearched = ref(false)

// 使用音频播放组合式API
const {
  voicePlayer,
  showPlayer,
  audio,
  onHidePlayerButtonClicked,
  onShowPlayerButtonClicked,
  onVoicePlay
} = useAudioPlayer()

const voiceFilterLabels = { with: '有语音', without: '无语音' }

const activeFilters = computed(() => {
  const filters = []
  if (speakerKeyword.value?.trim()) {
    filters.push({ key: 'speaker', label: `说话人: ${speakerKeyword.value.trim()}` })
  }
  if (voiceFilter.value && voiceFilter.value !== 'all') {
    filters.push({ key: 'voice', label: `语音: ${voiceFilterLabels[voiceFilter.value] || voiceFilter.value}` })
  }
  if (sourceTypeFilter.value) {
    const opt = sourceTypeOptions.find(o => o.value === sourceTypeFilter.value)
    filters.push({ key: 'source', label: `来源: ${opt?.label || sourceTypeFilter.value}` })
  }
  if (createdVersionFilter.value) {
    filters.push({ key: 'createdVersion', label: `创建版本: ${createdVersionFilter.value}` })
  }
  if (updatedVersionFilter.value) {
    filters.push({ key: 'updatedVersion', label: `更新版本: ${updatedVersionFilter.value}` })
  }
  return filters
})

const handleSearch = async () => {
  hasSearched.value = true
  await onQueryButtonClicked()
}

const clearFilter = (key) => {
  const map = {
    speaker: () => { speakerKeyword.value = '' },
    voice: () => { voiceFilter.value = 'all' },
    source: () => { sourceTypeFilter.value = '' },
    createdVersion: () => { createdVersionFilter.value = '' },
    updatedVersion: () => { updatedVersionFilter.value = '' },
  }
  map[key]?.()
  handleSearch()
}

const clearAllFilters = () => {
  speakerKeyword.value = ''
  voiceFilter.value = 'all'
  sourceTypeFilter.value = ''
  createdVersionFilter.value = ''
  updatedVersionFilter.value = ''
  handleSearch()
}

const onPageSizeChange = () => {
  currentPage.value = 1
  goToPage(1)
}

onBeforeMount(async () => {
    await loadVersionOptions()
})
</script>

<style scoped>
.languageSelector {
    width: 120px;
}

.languageSelector:deep(input) {
    text-align: center;
}

.speakerInput {
    width: 100%;
    min-width: 0;
    margin-top: 0;
}

.voiceFilter,
.sourceFilter {
    width: 100%;
    min-width: 0;
    margin-top: 0;
    margin-left: 0;
}

:deep(.versionFilterGroup) {
    grid-column: span 2;
}

.loading-container {
    padding: 12px 0;
}

.no-results {
    padding: 16px 0 8px;
    text-align: center;
}

@media (max-width: 680px) {
    :deep(.versionFilterGroup) {
        grid-column: span 2;
    }

    .activeFilters {
        margin-top: 8px;
    }

    .resultPagination {
        flex-wrap: wrap;
    }

    .loading-container {
        padding: 15px 0;
    }

    .no-results {
        padding: 30px 0;
    }
}
</style>
