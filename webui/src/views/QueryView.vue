<template>
    <div class="viewWrapper pageShell pageShell--compact">
        <h1 class="pageTitle">关键词搜索</h1>
        <div class="helpText">
            <p>支持关键词、说话人、来源类别、语音存在性、创建版本、更新版本组合筛选。</p>
            <p>当关键词留空时，只要填写了说话人、来源类别或版本也可以查询。</p>
        </div>

        <div class="stickySearchSection">
            <SearchBar
                v-model:keyword="keyword"
                v-model:selectedLanguage="selectedInputLanguage"
                :supportedLanguages="supportedInputLanguage"
                :summary="searchSummary"
                @search="onQueryButtonClicked"
            />

            <div class="searchBarAdditional">
                <div class="filterItem">
                    <span class="filterLabel">说话人</span>
                    <el-input
                        v-model="speakerKeyword"
                        placeholder="角色名..."
                        class="speakerInput"
                        @keyup.enter.native="onQueryButtonClicked"
                        clearable
                    />
                </div>

                <div class="filterItem">
                    <span class="filterLabel">语音</span>
                    <el-select
                        v-model="voiceFilter"
                        class="voiceFilter"
                        placeholder="语音筛选"
                        @change="onQueryButtonClicked"
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
                        @change="onQueryButtonClicked"
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
                    @search="onQueryButtonClicked"
                />
            </div>
        </div>

        <div class="resultControls" v-if="totalCount > 0">
            <span class="resultCount">共 {{ totalCount }} 条，当前 {{ currentPage }} / {{ totalPages }} 页</span>
            <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(1)">首页</el-button>
            <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(currentPage - 1)">上一页</el-button>
            <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(currentPage + 1)">下一页</el-button>
            <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(totalPages)">末页</el-button>
        </div>

        <div v-if="isLoading" class="loading-container">
            <el-skeleton :rows="5" animated />
        </div>

        <div v-else-if="queryResult.length === 0 && totalCount === 0" class="no-results">
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

        <div class="resultControls" v-if="totalCount > 0">
            <span class="resultCount">共 {{ totalCount }} 条，当前 {{ currentPage }} / {{ totalPages }} 页</span>
            <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(1)">首页</el-button>
            <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(currentPage - 1)">上一页</el-button>
            <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(currentPage + 1)">下一页</el-button>
            <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(totalPages)">末页</el-button>
        </div>
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
import { onBeforeMount } from 'vue'
import { Close } from '@element-plus/icons-vue'
import TranslateDisplay from '@/components/ResultEntry.vue'
import AudioPlayer from '@liripeng/vue-audio-player'
import SearchBar from '@/components/SearchBar.vue'
import VersionFilter from '@/components/VersionFilter.vue'
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
  searchSummary,
  currentPage,
  totalCount,
  totalPages,
  isLoading,
  loadVersionOptions,
  onQueryButtonClicked,
  goToPage
} = useSearch()

// 使用音频播放组合式API
const {
  voicePlayer,
  showPlayer,
  audio,
  onHidePlayerButtonClicked,
  onShowPlayerButtonClicked,
  onVoicePlay,
  pauseAudio
} = useAudioPlayer()

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

.searchBarAdditional {
    display: grid;
    grid-template-columns: minmax(0, 1.35fr) repeat(2, minmax(0, 1fr)) repeat(2, minmax(0, 0.85fr));
    align-items: end;
    gap: 12px;
    width: 100%;
}

.filterItem {
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
}

.filterLabel {
    font-size: 0.75rem;
    color: var(--theme-text-muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-weight: 600;
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

.resultControls {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    margin: 2px 0 4px;
    padding: 12px 14px;
    border-radius: 18px;
    border: 1px solid rgba(190, 164, 124, 0.28);
    background: rgba(255, 255, 255, 0.46);
    color: var(--theme-text-muted);
    font-size: 13px;
}

:global([data-theme="dark"]) .resultControls {
    background: rgba(30, 40, 37, 0.46);
    border-color: var(--theme-border);
}

.resultCount {
    margin-right: 4px;
    font-weight: 600;
    color: var(--theme-text);
}

.loading-container {
    padding: 12px 0;
}

.no-results {
    padding: 16px 0 8px;
    text-align: center;
}

@media (max-width: 860px) {
    .searchBarAdditional {
        grid-template-columns: repeat(5, minmax(0, 1fr));
    }
}

@media (max-width: 680px) {
    .searchBarAdditional {
        grid-template-columns: minmax(0, 1.2fr) repeat(2, minmax(0, 1fr));
    }

    .filterLabel {
        display: none;
    }

    .loading-container {
        padding: 15px 0;
    }

    .no-results {
        padding: 30px 0;
    }
}
</style>
