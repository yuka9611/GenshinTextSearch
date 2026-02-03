<template>
    <div class="viewWrapper">
        <h1 class="pageTitle">关键词检索</h1>
        <div class="helpText">
            <p>使用关键词对游戏的指定语言的文本进行检索。</p>
            <p>检索结果以精准匹配优先；非精准匹配中，带配音的结果会更靠前。</p>
        </div>


        <div class="searchBar">
            <el-input
                v-model="keyword"
                style="max-width: 600px;"
                placeholder="请输入关键词，中文支持模糊搜索"
                class="input-with-select"
                @keyup.enter.native="onQueryButtonClicked"
                clearable
            >
                <template #prepend>
                    <el-select v-model="selectedInputLanguage" placeholder="Select" class="languageSelector" >
                        <el-option v-for="(v,k) in supportedInputLanguage" :label="v" :value="k" :key="k"/>
                    </el-select>
                </template>
                <template #append>
                    <el-button :icon="Search" @click="onQueryButtonClicked"/>
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
                placeholder="配音筛选"
                @change="onQueryButtonClicked"
            >
                <el-option label="全部" value="all" />
                <el-option label="仅有配音" value="with" />
                <el-option label="仅无配音" value="without" />
            </el-select>
            <span class="searchSummary">
                {{ searchSummary }}
            </span>
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
            <TranslateDisplay v-for="translate in queryResult" :translate-obj="translate" class="translate" @onVoicePlay="onVoicePlay" :keyword="keywordLast" :search-lang="searchLangLast" />
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
            theme-color="var(--el-color-primary)">

        </AudioPlayer>
    </div>

    <div class="showPlayerButton" @click="onShowPlayerButtonClicked" v-show="!showPlayer && queryResult.length > 0">
        <i class="fi fi-sr-waveform-path"></i>
    </div>

</template>

<script setup>
import {onBeforeMount, ref, computed} from 'vue';
import {Close, Delete, Download, Plus, ZoomIn} from '@element-plus/icons-vue';
import { Search } from '@element-plus/icons-vue'
import global from "@/global/global"
import api from "@/api/keywordQuery"
import TranslateDisplay from "@/components/ResultEntry.vue";
import AudioPlayer from "@liripeng/vue-audio-player";

const queryLanguages = [1,4]

const queryResult = ref([])


const selectedInputLanguage = ref(global.config.defaultSearchLanguage + '')
const keyword = ref("")
const keywordLast = ref("")
const speakerLast = ref("")
const speakerKeyword = ref("")
const searchLangLast = ref(0)
const voiceFilter = ref("all")
const voiceFilterLast = ref("all")
const supportedInputLanguage = ref({})
const searchSummary = ref("")
const pageSize = ref(50)
const currentPage = ref(1)
const totalCount = ref(0)
const totalPages = computed(() => Math.max(1, Math.ceil(totalCount.value / pageSize.value)))

onBeforeMount(async ()=>{
    supportedInputLanguage.value = global.languages
})

/**
 *
 * @type {Ref<AudioPlayer>}
 */
const voicePlayer = ref()
const showPlayer = ref(false)
let firstShowPlayer = true

const fetchPage = async (page, useLast = false) => {
    const params = useLast ? {
        keyword: keywordLast.value,
        speaker: speakerLast.value,
        langCode: searchLangLast.value,
        voiceFilter: voiceFilterLast.value
    } : {
        keyword: keyword.value,
        speaker: speakerKeyword.value,
        langCode: parseInt(selectedInputLanguage.value),
        voiceFilter: voiceFilter.value
    }

    let ans = (await api.queryByKeyword(
        params.keyword,
        params.langCode,
        params.speaker,
        page,
        pageSize.value,
        params.voiceFilter
    )).json

    if (voicePlayer.value) {
        voicePlayer.value.pause()
    }

    const timeMs = typeof ans.time === "number" ? ans.time.toFixed(2) : "0.00"
    const total = ans.total || 0

    queryResult.value = ans.contents || []
    totalCount.value = total
    currentPage.value = ans.page || page

    keywordLast.value = params.keyword
    speakerLast.value = params.speaker || ""
    searchLangLast.value = params.langCode
    voiceFilterLast.value = params.voiceFilter

    if (total > 0) {
        searchSummary.value = `查询用时: ${timeMs}ms，共 ${total} 条结果。`
    } else {
        searchSummary.value = `查询用时: ${timeMs}ms，没有找到结果。`
    }
}

const onQueryButtonClicked = async () =>{
    currentPage.value = 1
    await fetchPage(1, false)
}

// 播放器相关开始
const audio = ref([])



const onHidePlayerButtonClicked = () => {
    showPlayer.value = false
}

const onShowPlayerButtonClicked = () => {
    showPlayer.value = true
}

const goToPage = async (page) => {
    if (!keywordLast.value && !speakerLast.value) {
        return
    }
    const safePage = Math.min(Math.max(1, page), totalPages.value)
    await fetchPage(safePage, true)
}

const onVoicePlay = (voiceUrl) => {
    if(firstShowPlayer){
        showPlayer.value = true;
        firstShowPlayer = false
    }

    if(audio.value.length > 0 && voiceUrl === audio.value[0]){
        if(voicePlayer.value.isPlaying){
            voicePlayer.value.pause()
        }else{
            voicePlayer.value.play()
        }

    }else{
        audio.value = [voiceUrl]
        // 要等一会才能播放
        setTimeout(()=>{
            voicePlayer.value.play()
        }, 0)

    }

}


</script>

<style scoped>
.viewWrapper{
    position: relative;
    width: var(--page-width);
    margin: 0 auto;
    background-color: #fff;
    box-shadow: var(--page-shadow);
    border-radius: var(--page-radius);
    padding: var(--page-padding-compact);
    overflow: visible;
}

.languageSelector{
    width: 120px;
}

.languageSelector:deep(input){
    text-align: center;
}
.translate:not(:last-child){
    border-bottom: 1px solid #ccc;
}

.voicePlayerContainer {
    margin-top: 10px;
    bottom: 0;
    position: sticky !important;
    box-shadow: 0 0 5px 5px rgba(36,37,38,.05);
    z-index: 3;
    background-color: #fff;
}

.showPlayerButton{
    position: absolute;
    right: 7.5%;
    bottom: 80px;
    height: 70px;
    width: 70px;
    border-radius: 50%;
    background-color: var(--el-color-primary);
    color: #fff;
    font-size: 25px;
    box-shadow: 0 6px 15px rgba(36,37,38,.2);
    text-align: center;
    line-height: 75px;
    cursor: pointer;
    z-index: 3;
}

.showPlayerButton:hover{
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

.searchSummary{
    margin-left: 10px;
    color: var(--el-input-text-color, var(--el-text-color-regular));
    font-size: 14px;
}
.speakerInput{
    max-width: 320px;
    margin-top: 8px;
}
.voiceFilter{
    max-width: 160px;
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

    .voiceFilter{
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

