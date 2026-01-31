<script setup>
import { onBeforeMount, ref } from 'vue';
import { Search } from '@element-plus/icons-vue';
import { useRouter } from "vue-router";
import global from "@/global/global";
import api from "@/api/keywordQuery";

const router = useRouter()
const keyword = ref("")
const keywordLast = ref("")
const selectedInputLanguage = ref(global.config.defaultSearchLanguage + '')
const supportedInputLanguage = ref({})
const searchSummary = ref("")

const questResults = ref([])
const readableResults = ref([])

onBeforeMount(async () => {
    supportedInputLanguage.value = global.languages
})

const onSearchClicked = async () => {
    if (!keyword.value.trim()) {
        searchSummary.value = "请输入关键词，中文支持模糊搜索。"
        questResults.value = []
        readableResults.value = []
        return
    }

    const ans = (await api.searchByName(keyword.value, selectedInputLanguage.value)).json
    const contents = ans.contents
    keywordLast.value = keyword.value
    questResults.value = contents.quests || []
    readableResults.value = contents.readables || []

    const questCount = questResults.value.length
    const readableCount = readableResults.value.length
    searchSummary.value = `查询用时: ${ans.time.toFixed(2)}ms，共 ${questCount} 条任务结果，${readableCount} 条阅读物结果。`
}

const gotoQuest = (questId) => {
    router.push({
        path: "/talk",
        query: {
            questId: questId,
            keyword: keywordLast.value,
            searchLang: selectedInputLanguage.value
        }
    })
}

const gotoReadable = (entry) => {
    router.push({
        path: "/talk",
        query: {
            readableId: entry.readableId,
            fileName: entry.fileName,
            keyword: keywordLast.value,
            searchLang: selectedInputLanguage.value
        }
    })
}
</script>

<template>
    <div class="viewWrapper">
        <h1 class="pageTitle">任务/阅读物查询</h1>
        <div class="helpText">
            <p>支持检索任务名称与阅读物名称。</p>
            <p>搜索结果可跳转到对应页面查看详细内容。</p>
        </div>

        <div class="searchBar">
            <el-input
                v-model="keyword"
                style="max-width: 600px;"
                placeholder="请输入关键词，中文支持模糊搜索"
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

        <div class="searchSpacer"></div>

        <div class="resultSection">
            <h2>任务名称</h2>
            <el-empty v-if="questResults.length === 0" description="没有匹配的任务名称" />
            <div v-else class="resultGrid">
                <el-card v-for="quest in questResults" :key="quest.questId" class="resultCard">
                    <div class="cardTitle">{{ quest.title }}</div>
                    <div class="cardMeta" v-if="quest.chapterName">章节: {{ quest.chapterName }}</div>
                    <div class="cardMeta">任务 ID: {{ quest.questId }}</div>
                    <el-button size="small" type="primary" @click="gotoQuest(quest.questId)">
                        查看剧情
                    </el-button>
                </el-card>
            </div>
        </div>

        <div class="resultSection">
            <h2>阅读物名称</h2>
            <el-empty v-if="readableResults.length === 0" description="没有匹配的阅读物名称" />
            <div v-else class="resultGrid">
                <el-card v-for="readable in readableResults" :key="readable.readableId" class="resultCard">
                    <div class="cardTitle">{{ readable.title }}</div>
                    <div class="cardMeta">文件名: {{ readable.fileName }}</div>
                    <el-button size="small" type="primary" @click="gotoReadable(readable)">
                        查看阅读物
                    </el-button>
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
