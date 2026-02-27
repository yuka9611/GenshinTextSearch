<script setup>
import { onBeforeMount, ref, computed } from 'vue';
import { Search } from '@element-plus/icons-vue';
import global from "@/global/global";
import api from "@/api/keywordQuery";
import basicInfoApi from "@/api/basicInfo";
import TranslateDisplay from "@/components/ResultEntry.vue";

const keyword = ref("")
const selectedInputLanguage = ref(global.config.defaultSearchLanguage + '')
const supportedInputLanguage = ref({})
const searchSummary = ref("")
const storySummary = ref("")

const avatarResults = ref([])
const storyEntries = ref([])
const globalStoryEntries = ref([])
const useGlobalStoryEntries = ref(false)
const selectedAvatar = ref(null)
const loadingStories = ref(false)
const textFilter = ref("")
const createdVersionFilter = ref("")
const updatedVersionFilter = ref("")
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
    if (!value) return ""
    return String(value).trim().toLowerCase()
}

const normalizeVersion = (value) => normalizeText(value)

const getNormalizedEntryVersion = (entry, kind) => {
    if (kind === "created") return normalizeVersion(entry.createdVersion || entry.createdVersionRaw || "")
    return normalizeVersion(entry.updatedVersion || entry.updatedVersionRaw || "")
}

const isSameCreatedUpdatedVersion = (entry) => {
    const createdVersion = getNormalizedEntryVersion(entry, "created")
    const updatedVersion = getNormalizedEntryVersion(entry, "updated")
    if (!createdVersion || !updatedVersion) return false
    return createdVersion === updatedVersion
}

const filteredStories = computed(() => {
    const text = normalizeText(textFilter.value)
    const createdFilter = normalizeVersion(createdVersionFilter.value)
    const updatedFilter = normalizeVersion(updatedVersionFilter.value)
    return storyEntries.value.filter((entry) => {
        const createdVersion = getNormalizedEntryVersion(entry, "created")
        const updatedVersion = getNormalizedEntryVersion(entry, "updated")
        if (createdFilter && !createdVersion.includes(createdFilter)) return false
        if (updatedFilter) {
            if (!updatedVersion.includes(updatedFilter)) return false
            if (isSameCreatedUpdatedVersion(entry)) return false
        }

        if (!text) return true
        const title = normalizeText(entry.storyTitle || entry.origin || "")
        if (title.includes(text)) return true
        const translates = entry.translates || {}
        for (const key of Object.keys(translates)) {
            const content = normalizeText(translates[key] || "")
            if (content.includes(text)) return true
        }
        return false
    })
})

const highlightKeyword = computed(() => textFilter.value.trim())

const onSearchClicked = async () => {
    const avatarKeyword = keyword.value.trim()
    const titleKeyword = textFilter.value.trim()
    const createdKeyword = createdVersionFilter.value.trim()
    const updatedKeyword = updatedVersionFilter.value.trim()
    const hasGlobalFilter = titleKeyword || createdKeyword || updatedKeyword

    if (!avatarKeyword && !hasGlobalFilter) {
        searchSummary.value = "请输入角色名进行查询"
        storySummary.value = ""
        avatarResults.value = []
        storyEntries.value = []
        globalStoryEntries.value = []
        useGlobalStoryEntries.value = false
        selectedAvatar.value = null
        return
    }

    if (avatarKeyword) {
        try {
            const ans = (await api.searchAvatar(keyword.value, selectedInputLanguage.value)).json
            const contents = ans.contents
            avatarResults.value = contents.avatars || []
            const avatarCount = avatarResults.value.length
            searchSummary.value = `查询耗时: ${ans.time.toFixed(2)}ms，找到 ${avatarCount} 个角色`
            storyEntries.value = []
            globalStoryEntries.value = []
            useGlobalStoryEntries.value = false
            storySummary.value = ""
            selectedAvatar.value = null
        } catch (err) {
            searchSummary.value = "查询失败，请稍后重试"
            avatarResults.value = []
            storyEntries.value = []
            globalStoryEntries.value = []
            useGlobalStoryEntries.value = false
            storySummary.value = ""
            selectedAvatar.value = null
            if (!err?.network) err?.defaultHandler?.()
        }
        return
    }

    loadingStories.value = true
    avatarResults.value = []
    selectedAvatar.value = { avatarId: null, name: "全角色（按筛选）" }

    try {
        const ans = (await api.searchAvatarStories(
            titleKeyword,
            createdVersionFilter.value,
            updatedVersionFilter.value,
            selectedInputLanguage.value,
        )).json
        const contents = ans.contents
        storyEntries.value = contents.stories || []
        globalStoryEntries.value = storyEntries.value
        useGlobalStoryEntries.value = true
        const avatarMap = new Map()
        for (const entry of storyEntries.value) {
            if (entry.avatarId === null || entry.avatarId === undefined) continue
            if (avatarMap.has(entry.avatarId)) continue
            avatarMap.set(entry.avatarId, {
                avatarId: entry.avatarId,
                name: (entry.avatarName || "").trim() || `角色 ${entry.avatarId}`
            })
        }
        avatarResults.value = Array.from(avatarMap.values()).sort((a, b) => a.name.localeCompare(b.name))
        const avatarCount = avatarResults.value.length
        const storyCount = storyEntries.value.length
        searchSummary.value = `查询耗时: ${ans.time.toFixed(2)}ms，按标题/版本检索，涉及 ${avatarCount} 个角色`
        storySummary.value = `查询耗时: ${ans.time.toFixed(2)}ms，找到 ${storyCount} 条角色故事`
    } catch (err) {
        searchSummary.value = "查询失败，请稍后重试"
        storySummary.value = ""
        avatarResults.value = []
        storyEntries.value = []
        globalStoryEntries.value = []
        useGlobalStoryEntries.value = false
        selectedAvatar.value = null
        if (!err?.network) err?.defaultHandler?.()
    } finally {
        loadingStories.value = false
    }
}

const onAvatarClicked = async (avatar) => {
    const avatarId = Number.parseInt(avatar?.avatarId, 10)
    if (Number.isNaN(avatarId)) {
        storySummary.value = "角色 ID 无效"
        return
    }
    selectedAvatar.value = { ...avatar, avatarId }
    storySummary.value = ""

    if (useGlobalStoryEntries.value) {
        storyEntries.value = globalStoryEntries.value.filter((entry) => Number(entry.avatarId) === avatarId)
        const storyCount = storyEntries.value.length
        storySummary.value = `当前筛选下，找到 ${storyCount} 条角色故事`
        return
    }

    storyEntries.value = []
    loadingStories.value = true

    try {
        const ans = (await api.getAvatarStories(avatarId, selectedInputLanguage.value)).json
        const contents = ans.contents
        storyEntries.value = contents.stories || []
        const storyCount = storyEntries.value.length
        storySummary.value = `查询耗时: ${ans.time.toFixed(2)}ms，找到 ${storyCount} 条角色故事`
    } catch (err) {
        storyEntries.value = []
        storySummary.value = "加载故事失败，请稍后重试"
        if (!err?.network) err?.defaultHandler?.()
    } finally {
        loadingStories.value = false
    }
}
</script>

<template>
    <div class="viewWrapper">
        <h1 class="pageTitle">角色故事查询</h1>
        <div class="helpText">
            <p>输入角色名进行检索，然后点击角色查看该角色的故事内容。</p>
        </div>

        <div class="searchBar">
            <el-input
                v-model="keyword"
                style="max-width: 600px;"
                placeholder="请输入角色名"
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
            <span class="searchSummary">
                {{ searchSummary }}
            </span>
        </div>

        <div class="filterBar topFilterBar">
            <el-input v-model="textFilter" placeholder="筛选标题或正文" class="filterInput" clearable @keyup.enter.native="onSearchClicked" />
            <el-select v-model="createdVersionFilter" placeholder="创建版本" class="versionInput" clearable filterable>
                <el-option v-for="version in versionOptions" :key="`created-${version}`" :label="version" :value="version" />
            </el-select>
            <el-select v-model="updatedVersionFilter" placeholder="更新版本" class="versionInput" clearable filterable>
                <el-option v-for="version in versionOptions" :key="`updated-${version}`" :label="version" :value="version" />
            </el-select>
        </div>

        <div class="searchSpacer"></div>

        <div class="resultSection" v-if="keyword.trim() || textFilter.trim() || createdVersionFilter.trim() || updatedVersionFilter.trim()">
            <h2>角色列表</h2>
            <el-empty v-if="avatarResults.length === 0" description="未找到角色" />
            <div v-else class="resultGrid">
                <el-card v-for="avatar in avatarResults" :key="avatar.avatarId" class="resultCard">
                    <div class="cardTitle">{{ avatar.name }}</div>
                    <div class="cardMeta">角色 ID: {{ avatar.avatarId }}</div>
                    <el-button size="small" type="primary" @click="onAvatarClicked(avatar)">
                        查看故事
                    </el-button>
                </el-card>
            </div>
        </div>

        <div class="resultSection">
            <h2 v-if="selectedAvatar">角色故事 - {{ selectedAvatar.name }}</h2>
            <div class="storySummary" v-if="storySummary">{{ storySummary }}</div>

            <el-empty v-if="!loadingStories && filteredStories.length === 0" description="未找到角色故事" />
            <div v-else>
                <TranslateDisplay
                    v-for="story in filteredStories"
                    :key="story.hash"
                    :translate-obj="story"
                    :keyword="highlightKeyword"
                    :search-lang="selectedInputLanguage"
                    class="translate"
                />
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

.cardMeta {
    color: #888;
    font-size: 13px;
}

.storySummary {
    margin: 8px 0 12px;
    color: #666;
    font-size: 13px;
}

.filterBar {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
    margin-bottom: 12px;
}

.topFilterBar {
    margin-top: 8px;
}

.filterInput {
    max-width: 260px;
}

.versionInput {
    width: 180px;
}

.translate:not(:last-child) {
    border-bottom: 1px solid #ccc;
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
