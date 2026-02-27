<template>
    <div class="viewWrapper">
        <h1 class="pageTitle">关键词搜索</h1>
        <div class="helpText">
            <p>支持关键词、说话人、语音存在性、创建版本、更新版本组合筛选。</p>
            <p>当关键词留空时，只要填写了版本或说话人也可以查询。</p>
        </div>

        <div class="searchBar">
            <el-input
                v-model="keyword"
                style="max-width: 600px;"
                placeholder="输入关键词"
                class="input-with-select"
                @keyup.enter.native="onQueryButtonClicked"
                clearable
            >
                <template #prepend>
                    <el-select v-model="selectedInputLanguage" placeholder="Select" class="languageSelector">
                        <el-option v-for="(v, k) in supportedInputLanguage" :label="v" :value="k" :key="k" />
                    </el-select>
                </template>
                <template #append>
                    <el-button :icon="Search" @click="onQueryButtonClicked" />
                </template>
            </el-input>

            <el-input
                v-model="speakerKeyword"
                placeholder="说话人（可选）"
                class="speakerInput"
                @keyup.enter.native="onQueryButtonClicked"
                clearable
            />

            <el-select
                v-model="voiceFilter"
                class="voiceFilter"
                placeholder="语音筛选"
                @change="onQueryButtonClicked"
            >
                <el-option label="全部" value="all" />
                <el-option label="有语音" value="with" />
                <el-option label="无语音" value="without" />
            </el-select>

            <el-select
                v-model="createdVersionFilter"
                class="versionFilter"
                placeholder="创建版本"
                clearable
                filterable
            >
                <el-option v-for="version in versionOptions" :key="`created-${version}`" :label="version" :value="version" />
            </el-select>

            <el-select
                v-model="updatedVersionFilter"
                class="versionFilter"
                placeholder="更新版本"
                clearable
                filterable
            >
                <el-option v-for="version in versionOptions" :key="`updated-${version}`" :label="version" :value="version" />
            </el-select>

            <span class="searchSummary">{{ searchSummary }}</span>
        </div>

        <div class="searchSpacer"></div>

        <div class="resultControls" v-if="totalCount > 0">
            <span class="resultCount">共 {{ totalCount }} 条，当前 {{ currentPage }} / {{ totalPages }} 页</span>
            <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(1)">首页</el-button>
            <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(currentPage - 1)">上一页</el-button>
            <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(currentPage + 1)">下一页</el-button>
            <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(totalPages)">末页</el-button>
        </div>

        <div>
            <TranslateDisplay
                v-for="translate in queryResult"
                :key="`${translate.hash}-${translate.origin || ''}`"
                :translate-obj="translate"
                class="translate"
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

    <div class="viewWrapper voicePlayerContainer" v-show="showPlayer && queryResult.length > 0">
        <span class="hideIcon" @click="onHidePlayerButtonClicked">
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

    <div class="showPlayerButton" @click="onShowPlayerButtonClicked" v-show="!showPlayer && queryResult.length > 0">
        <i class="fi fi-sr-waveform-path"></i>
    </div>
</template>

<script setup>
import { onBeforeMount, ref, computed } from 'vue'
import { Close, Search } from '@element-plus/icons-vue'
import global from '@/global/global'
import api from '@/api/keywordQuery'
import basicInfoApi from '@/api/basicInfo'
import TranslateDisplay from '@/components/ResultEntry.vue'
import AudioPlayer from '@liripeng/vue-audio-player'

const queryResult = ref([])

const selectedInputLanguage = ref(global.config.defaultSearchLanguage + '')
const keyword = ref('')
const keywordLast = ref('')
const speakerLast = ref('')
const speakerKeyword = ref('')
const searchLangLast = ref(0)
const voiceFilter = ref('all')
const voiceFilterLast = ref('all')
const createdVersionFilter = ref('')
const updatedVersionFilter = ref('')
const createdVersionLast = ref('')
const updatedVersionLast = ref('')
const versionOptions = ref([])
const supportedInputLanguage = ref({})
const searchSummary = ref('')
const pageSize = ref(50)
const currentPage = ref(1)
const totalCount = ref(0)
const totalPages = computed(() => Math.max(1, Math.ceil(totalCount.value / pageSize.value)))

onBeforeMount(async () => {
    supportedInputLanguage.value = global.languages
    try {
        const ans = await basicInfoApi.getAvailableVersions()
        versionOptions.value = ans.json || []
    } catch (_) {
        versionOptions.value = []
    }
})

const normalizeText = (value) => {
    if (!value) return ''
    return String(value).trim().toLowerCase()
}

const normalizeVersion = (value) => normalizeText(value)

const getNormalizedEntryVersion = (entry, kind) => {
    if (kind === 'created') return normalizeVersion(entry.createdVersion || entry.createdVersionRaw || '')
    return normalizeVersion(entry.updatedVersion || entry.updatedVersionRaw || '')
}

const shouldKeepByVersionFilter = (entry, updatedFilterRaw) => {
    const updatedFilter = normalizeVersion(updatedFilterRaw)
    if (!updatedFilter) return true

    const updatedValue = getNormalizedEntryVersion(entry, 'updated')
    if (!updatedValue.includes(updatedFilter)) return false

    const createdValue = getNormalizedEntryVersion(entry, 'created')
    if (!createdValue || !updatedValue) return true
    return createdValue !== updatedValue
}

const voicePlayer = ref()
const showPlayer = ref(false)
let firstShowPlayer = true

const fetchPage = async (page, useLast = false) => {
    const params = useLast
        ? {
            keyword: keywordLast.value,
            speaker: speakerLast.value,
            langCode: searchLangLast.value,
            voiceFilter: voiceFilterLast.value,
            createdVersion: createdVersionLast.value,
            updatedVersion: updatedVersionLast.value,
        }
        : {
            keyword: keyword.value,
            speaker: speakerKeyword.value,
            langCode: parseInt(selectedInputLanguage.value),
            voiceFilter: voiceFilter.value,
            createdVersion: createdVersionFilter.value,
            updatedVersion: updatedVersionFilter.value,
        }

    const ans = (
        await api.queryByKeyword(
            params.keyword,
            params.langCode,
            params.speaker,
            page,
            pageSize.value,
            params.voiceFilter,
            params.createdVersion,
            params.updatedVersion
        )
    ).json

    if (voicePlayer.value) {
        voicePlayer.value.pause()
    }

    const timeMs = typeof ans.time === 'number' ? ans.time.toFixed(2) : '0.00'
    const total = ans.total || 0

    queryResult.value = (ans.contents || []).filter((entry) => {
        return shouldKeepByVersionFilter(entry, params.updatedVersion)
    })
    totalCount.value = total
    currentPage.value = ans.page || page

    keywordLast.value = params.keyword || ''
    speakerLast.value = params.speaker || ''
    searchLangLast.value = params.langCode
    voiceFilterLast.value = params.voiceFilter || 'all'
    createdVersionLast.value = params.createdVersion || ''
    updatedVersionLast.value = params.updatedVersion || ''

    if (total > 0) {
        const filterText = [createdVersionLast.value, updatedVersionLast.value].filter(Boolean).join(' / ')
        searchSummary.value = filterText
            ? `查询耗时: ${timeMs}ms，总计 ${total} 条，版本筛选: ${filterText}`
            : `查询耗时: ${timeMs}ms，总计 ${total} 条`
    } else {
        searchSummary.value = `查询耗时: ${timeMs}ms，未找到结果`
    }
}

const onQueryButtonClicked = async () => {
    currentPage.value = 1
    await fetchPage(1, false)
}

const audio = ref([])

const onHidePlayerButtonClicked = () => {
    showPlayer.value = false
}

const onShowPlayerButtonClicked = () => {
    showPlayer.value = true
}

const goToPage = async (page) => {
    if (!keywordLast.value && !speakerLast.value && !createdVersionLast.value && !updatedVersionLast.value) {
        return
    }
    const safePage = Math.min(Math.max(1, page), totalPages.value)
    await fetchPage(safePage, true)
}

const onVoicePlay = (voiceUrl) => {
    if (firstShowPlayer) {
        showPlayer.value = true
        firstShowPlayer = false
    }

    if (audio.value.length > 0 && voiceUrl === audio.value[0]) {
        if (voicePlayer.value.isPlaying) {
            voicePlayer.value.pause()
        } else {
            voicePlayer.value.play()
        }
    } else {
        audio.value = [voiceUrl]
        setTimeout(() => {
            voicePlayer.value.play()
        }, 0)
    }
}
</script>

<style scoped>
.viewWrapper {
    position: relative;
    width: var(--page-width);
    margin: 0 auto;
    background-color: #fff;
    box-shadow: var(--page-shadow);
    border-radius: var(--page-radius);
    padding: var(--page-padding-compact);
    overflow: visible;
}

.languageSelector {
    width: 120px;
}

.languageSelector:deep(input) {
    text-align: center;
}

.translate:not(:last-child) {
    border-bottom: 1px solid #ccc;
}

.voicePlayerContainer {
    margin-top: 10px;
    bottom: 0;
    position: sticky !important;
    box-shadow: 0 0 5px 5px rgba(36, 37, 38, .05);
    z-index: 3;
    background-color: #fff;
}

.showPlayerButton {
    position: absolute;
    right: 7.5%;
    bottom: 80px;
    height: 70px;
    width: 70px;
    border-radius: 50%;
    background-color: var(--el-color-primary);
    color: #fff;
    font-size: 25px;
    box-shadow: 0 6px 15px rgba(36, 37, 38, .2);
    text-align: center;
    line-height: 75px;
    cursor: pointer;
    z-index: 3;
}

.showPlayerButton:hover {
    background-color: var(--el-color-primary-light-3);
}

.hideIcon {
    cursor: pointer;
    position: absolute;
    top: 10px;
    right: 10px;
}

.hideIcon:hover {
    color: #888;
}

.pageTitle {
    border-bottom: 1px #ccc solid;
    padding-bottom: 10px;
}

.helpText {
    margin: 8px 0 12px;
    color: #999;
}

.searchBar {
    position: sticky;
    top: 0;
    z-index: 3;
    background-color: #fff;
    padding-bottom: 8px;
    box-sizing: border-box;
}

.searchSummary {
    margin-left: 10px;
    color: var(--el-input-text-color, var(--el-text-color-regular));
    font-size: 14px;
}

.speakerInput {
    max-width: 320px;
    margin-top: 8px;
}

.voiceFilter {
    max-width: 160px;
    margin-top: 8px;
    margin-left: 8px;
}

.versionFilter {
    max-width: 180px;
    margin-top: 8px;
    margin-left: 8px;
}

.searchSpacer {
    display: none;
}

.resultControls {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    margin: 6px 0 12px;
    color: #666;
    font-size: 13px;
}

.resultCount {
    margin-right: 4px;
}

@media (max-width: 720px) {
    .searchSummary {
        display: block;
        margin-left: 0;
        margin-top: 8px;
    }

    .voiceFilter,
    .versionFilter {
        margin-left: 0;
        display: block;
    }

    .searchSpacer {
        display: none;
        height: 0;
    }

    .voicePlayerContainer {
        position: fixed !important;
        left: 8px;
        right: 8px;
        bottom: 0;
        width: auto;
        margin-top: 0;
        border-radius: 0;
        z-index: 3;
        box-sizing: border-box;
        box-shadow: none;
    }

    .showPlayerButton {
        position: fixed;
        right: 16px;
        bottom: 24px;
        width: 56px;
        height: 56px;
        line-height: 60px;
        z-index: 3;
        box-shadow: none;
    }
}
</style>
