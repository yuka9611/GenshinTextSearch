<template>
    <div class="viewWrapper">
        <h1 class="pageTitle">关键词检索</h1>
        <div class="helpText">
            <p>使用关键词对游戏的指定语言的文本进行检索。</p>
            <p>检索结果中，可能有对应配音的结果会被排序在前面。</p>
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
            <span class="searchSummary">
                {{ searchSummary }}
            </span>
        </div>
        <div class="searchSpacer"></div>

        <div>
            <TranslateDisplay v-for="translate in visibleResults" :translate-obj="translate" class="translate" @onVoicePlay="onVoicePlay" :keyword="keywordLast" :search-lang="searchLangLast" />
        </div>

        <div class="resultControls" v-if="queryResult.length > 0">
            <span class="resultCount">已显示 {{ Math.min(renderLimit, queryResult.length) }} / {{ queryResult.length }}</span>
            <el-button v-if="hasMoreResults" size="small" @click="showMoreResults">显示更多</el-button>
            <el-button v-if="hasMoreResults" size="small" @click="showAllResults">显示全部</el-button>
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
const searchLangLast = ref(0)
const supportedInputLanguage = ref({})
const searchSummary = ref("")
const renderStep = 50
const renderLimit = ref(renderStep)
const visibleResults = computed(() => queryResult.value.slice(0, renderLimit.value))
const hasMoreResults = computed(() => queryResult.value.length > renderLimit.value)

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

const onQueryButtonClicked = async () =>{
    let ans = (await api.queryByKeyword(keyword.value, selectedInputLanguage.value)).json
    // 停止语音播放
    voicePlayer.value.pause()

    let searchSummaryTmp = `查询用时: ${ans.time.toFixed(2)}ms，`
    if(ans.contents.length > 0){
        if(ans.contents.length >= 200){
            searchSummaryTmp += `共 ≥200 条结果`
        }else{
            searchSummaryTmp += `共 ${ans.contents.length} 条结果`
        }

    }else{
        searchSummaryTmp += `没有找到结果。`
        searchSummary.value = searchSummaryTmp
        queryResult.value = []
        return
    }

    // let mergedCount = 0
    // // 去重，合并相同的语音条目
    // let resultMap = new Map()
    // for(let item of ans.contents){
    //     let key = item.translates[queryLanguages[0]]
    //     if(!resultMap.has(key)){
    //         resultMap.set(key, item)
    //         continue
    //     }
    //     mergedCount++;
    //
    //     let oldItem = resultMap.get(key)
    //     let voicePathsToAdd = []
    //     for(let newVoicePath of item.voicePaths){
    //         let found = false
    //         for(let oldVoicePath of oldItem.voicePaths){
    //             if(oldVoicePath === newVoicePath){
    //                 found = false
    //                 break
    //             }
    //         }
    //         if(!found){
    //             voicePathsToAdd.push(newVoicePath)
    //         }
    //     }
    //     if(voicePathsToAdd.length > 0){
    //         oldItem.voicePaths.push(...voicePathsToAdd)
    //
    //     }
    // }
    // // 重排序，把有语音的条目拉到上面
    // queryResult.value.length = 0
    // let noVoiceEntries = []
    //
    // resultMap.forEach((item, key, _)=>{
    //     if(item.voicePaths.length > 0){
    //         queryResult.value.push(item)
    //     }else{
    //         noVoiceEntries.push(item)
    //     }
    // })
    //
    //
    // queryResult.value.push(...noVoiceEntries)

    // 不合并了
    queryResult.value = ans.contents;
    renderLimit.value = Math.min(renderStep, queryResult.value.length)

    keywordLast.value = keyword.value
    searchLangLast.value = parseInt(selectedInputLanguage.value)

    // if(mergedCount > 0){
    //     searchSummaryTmp += `，已合并 ${mergedCount} 条重复结果。`
    // }else{
    //     searchSummaryTmp += '。'
    // }

    // 不合并了
    searchSummaryTmp += '。'
    searchSummary.value = searchSummaryTmp
}

// 播放器相关开始
const audio = ref([])



const onHidePlayerButtonClicked = () => {
    showPlayer.value = false
}

const onShowPlayerButtonClicked = () => {
    showPlayer.value = true
}

const showMoreResults = () => {
    renderLimit.value = Math.min(renderLimit.value + renderStep, queryResult.value.length)
}

const showAllResults = () => {
    renderLimit.value = queryResult.value.length
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
.searchSpacer {
    display: none;
}

.resultControls {
    display: flex;
    align-items: center;
    gap: 8px;
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

