<script setup>
import { onBeforeMount, ref, computed } from 'vue'
import { Search } from '@element-plus/icons-vue'
import { useRouter } from 'vue-router'
import global from '@/global/global'
import api from '@/api/keywordQuery'
import basicInfoApi from '@/api/basicInfo'
import StylizedText from '@/components/StylizedText.vue'

const router = useRouter()
const keyword = ref('')
const keywordLast = ref('')
const selectedInputLanguage = ref(global.config.defaultSearchLanguage + '')
const supportedInputLanguage = ref({})
const searchSummary = ref('')

const questResults = ref([])
const readableResults = ref([])
const createdVersionFilter = ref('')
const updatedVersionFilter = ref('')
const versionOptions = ref([])

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

const isSameCreatedUpdatedVersion = (entry) => {
    const createdValue = getNormalizedEntryVersion(entry, 'created')
    const updatedValue = getNormalizedEntryVersion(entry, 'updated')
    if (!createdValue || !updatedValue) return false
    return createdValue === updatedValue
}

const matchVersionFilter = (entry) => {
    const createdFilter = normalizeVersion(createdVersionFilter.value)
    const updatedFilter = normalizeVersion(updatedVersionFilter.value)
    const createdValue = getNormalizedEntryVersion(entry, 'created')
    const updatedValue = getNormalizedEntryVersion(entry, 'updated')
    if (createdFilter && !createdValue.includes(createdFilter)) return false
    if (updatedFilter) {
        if (!updatedValue.includes(updatedFilter)) return false
        if (isSameCreatedUpdatedVersion(entry)) return false
    }
    return true
}

const filteredQuestResults = computed(() => {
    return questResults.value.filter(matchVersionFilter)
})

const filteredReadableResults = computed(() => {
    return readableResults.value.filter(matchVersionFilter)
})

const displayVersion = (entry, kind) => {
    if (kind === 'created') return entry.createdVersion || entry.createdVersionRaw || '未知'
    return entry.updatedVersion || entry.updatedVersionRaw || '未知'
}

const showUpdatedVersionTag = (entry) => {
    return displayVersion(entry, 'created') !== displayVersion(entry, 'updated')
}

const onSearchClicked = async () => {
    const keywordText = keyword.value.trim()
    const createdText = createdVersionFilter.value.trim()
    const updatedText = updatedVersionFilter.value.trim()

    if (!keywordText && !createdText && !updatedText) {
        searchSummary.value = '请输入关键词或版本'
        questResults.value = []
        readableResults.value = []
        return
    }

    const ans = (await api.searchByName(
        keyword.value,
        selectedInputLanguage.value,
        createdVersionFilter.value,
        updatedVersionFilter.value,
    )).json
    const contents = ans.contents
    keywordLast.value = keyword.value
    questResults.value = contents.quests || []
    readableResults.value = contents.readables || []

    const questCount = questResults.value.length
    const readableCount = readableResults.value.length
    if (!keywordText && (createdText || updatedText)) {
        searchSummary.value = `查询耗时: ${ans.time.toFixed(2)}ms，按版本筛选。任务 ${questCount} 条，阅读物 ${readableCount} 条`
    } else {
        searchSummary.value = `查询耗时: ${ans.time.toFixed(2)}ms，任务 ${questCount} 条，阅读物 ${readableCount} 条`
    }
}

const gotoQuest = (questId) => {
    router.push({
        path: '/talk',
        query: {
            questId,
            keyword: keywordLast.value,
            searchLang: selectedInputLanguage.value,
        },
    })
}

const gotoReadable = (entry) => {
    router.push({
        path: '/talk',
        query: {
            readableId: entry.readableId,
            fileName: entry.fileName,
            keyword: keywordLast.value,
            searchLang: selectedInputLanguage.value,
        },
    })
}
</script>

<template>
    <div class="viewWrapper">
        <h1 class="pageTitle">任务/阅读物搜索</h1>
        <div class="helpText">
            <p>按任务标题、章节名、任务 ID 或阅读物标题/文件名检索。</p>
        </div>

        <div class="searchBar">
            <el-input
                v-model="keyword"
                style="max-width: 600px;"
                placeholder="输入关键词"
                class="input-with-select"
                @keyup.enter.native="onSearchClicked"
                clearable
            >
                <template #prepend>
                    <el-select v-model="selectedInputLanguage" placeholder="Select" class="languageSelector">
                        <el-option v-for="(v, k) in supportedInputLanguage" :label="v" :value="k" :key="k" />
                    </el-select>
                </template>
                <template #append>
                    <el-button :icon="Search" @click="onSearchClicked" />
                </template>
            </el-input>
            <span class="searchSummary">{{ searchSummary }}</span>
        </div>

        <div class="filterBar">
            <el-select v-model="createdVersionFilter" placeholder="创建版本" class="versionInput" clearable filterable>
                <el-option v-for="version in versionOptions" :key="`created-${version}`" :label="version" :value="version" />
            </el-select>
            <el-select v-model="updatedVersionFilter" placeholder="更新版本" class="versionInput" clearable filterable>
                <el-option v-for="version in versionOptions" :key="`updated-${version}`" :label="version" :value="version" />
            </el-select>
        </div>

        <div class="searchSpacer"></div>

        <div class="resultSection">
            <h2>任务结果</h2>
            <el-empty v-if="filteredQuestResults.length === 0" description="没有任务结果" />
            <div v-else class="resultGrid">
                <el-card v-for="quest in filteredQuestResults" :key="quest.questId" class="resultCard">
                    <div class="cardTitle">
                        <StylizedText :text="quest.title" :keyword="keywordLast" />
                    </div>
                    <div class="cardMeta" v-if="quest.chapterName">章节: {{ quest.chapterName }}</div>
                    <div class="cardMeta">任务 ID: {{ quest.questId }}</div>
                    <div class="versionTags">
                        <el-tag size="small" effect="plain">创建: {{ displayVersion(quest, 'created') }}</el-tag>
                        <el-tag v-if="showUpdatedVersionTag(quest)" size="small" effect="plain">更新: {{ displayVersion(quest, 'updated') }}</el-tag>
                    </div>
                    <el-button size="small" type="primary" @click="gotoQuest(quest.questId)">查看对话</el-button>
                </el-card>
            </div>
        </div>

        <div class="resultSection">
            <h2>阅读物结果</h2>
            <el-empty v-if="filteredReadableResults.length === 0" description="没有阅读物结果" />
            <div v-else class="resultGrid">
                <el-card v-for="readable in filteredReadableResults" :key="`${readable.readableId}-${readable.fileName}`" class="resultCard">
                    <div class="cardTitle">
                        <StylizedText :text="readable.title" :keyword="keywordLast" />
                    </div>
                    <div class="versionTags">
                        <el-tag size="small" effect="plain">创建: {{ displayVersion(readable, 'created') }}</el-tag>
                        <el-tag v-if="showUpdatedVersionTag(readable)" size="small" effect="plain">更新: {{ displayVersion(readable, 'updated') }}</el-tag>
                    </div>
                    <el-button size="small" type="primary" @click="gotoReadable(readable)">查看内容</el-button>
                </el-card>
            </div>
        </div>
    </div>
</template>

<style scoped>
.viewWrapper {
    position: relative;
    width: var(--page-width);
    margin: 0 auto;
    background-color: #fff;
    box-shadow: var(--page-shadow);
    border-radius: var(--page-radius);
    padding: var(--page-padding);
    overflow: visible;
}

.pageTitle {
    border-bottom: 1px #ccc solid;
    padding-bottom: 10px;
}

.helpText {
    margin: 20px 0;
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

.searchSpacer {
    display: none;
}

.filterBar {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin: 10px 0 6px;
}

.versionInput {
    width: 180px;
}

.resultSection {
    margin-top: 20px;
}

.resultSection h2 {
    margin-bottom: 12px;
}

.resultGrid {
    display: grid;
    gap: 12px;
    grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
}

.resultCard {
    display: flex;
    flex-direction: column;
    gap: 8px;
}

.cardTitle {
    font-weight: 600;
}

.cardTitle :deep(p) {
    margin: 0;
}

.cardMeta {
    color: #888;
    font-size: 13px;
}

.versionTags {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
}

@media (max-width: 720px) {
    .searchSummary {
        display: block;
        margin-left: 0;
        margin-top: 8px;
    }

    .searchSpacer {
        display: none;
        height: 0;
    }
}
</style>
