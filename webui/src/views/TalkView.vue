<script setup>
import global from "@/global/global"
import api from "@/api/keywordQuery";

import {useRoute} from "vue-router";
import {onActivated, onDeactivated, reactive, ref, watch, computed} from "vue";
import PlayVoiceButton from "@/components/PlayVoiceButton.vue";
import StylizedText from "@/components/StylizedText.vue";
import AudioPlayer from "@liripeng/vue-audio-player";
import {Close, CopyDocument, VideoPlay} from "@element-plus/icons-vue";
import {ElMessage} from "element-plus";

const UI_TEXT = Object.freeze({
    taskText: "任务文本",
    readableFallback: "阅读物",
    noTextToCopy: "没有可复制的文本",
    copied: "已复制",
    copyFailed: "复制失败，请手动选择文本",
    pageTitleReadable: "阅读物内容",
    pageTitleDialogue: "任务对话内容",
    source: "来源",
    queryTime: "查询耗时",
    copy: "复制",
    showTextVersion: "显示文本版本",
    total: "共",
    currentPage: "条，当前第",
    page: "页",
    first: "首页",
    prev: "上一页",
    next: "下一页",
    last: "末页",
    speaker: "说话人",
    playAll: "播放全部",
    voice: "语音",
    autoLoop: "自动循环播放",
    autoScroll: "自动滚动",
    loadingVoices: "正在加载语音",
    created: "创建",
    updated: "更新",
    unknown: "未知",
    version: "版本",
})

const route = useRoute()
const keyword = ref("")
const questName = ref(UI_TEXT.taskText)
const textHash = ref(0)
const queryTime = ref("0")
const dialogues = ref([])
const isReadable = ref(false)
const readableTitle = ref("")
const readableFileName = ref("")
const readableTranslates = ref({})
const readableCreatedVersion = ref("")
const readableUpdatedVersion = ref("")
const readableCreatedVersionRaw = ref("")
const readableUpdatedVersionRaw = ref("")
const subtitleCreatedVersion = ref("")
const subtitleUpdatedVersion = ref("")
const subtitleCreatedVersionRaw = ref("")
const subtitleUpdatedVersionRaw = ref("")
const pageSize = ref(200)
const currentPage = ref(1)
const totalCount = ref(0)
const totalPages = computed(() => Math.max(1, Math.ceil(totalCount.value / pageSize.value)))

let playVoiceButtonDict = {}
let playableDialogueIdList = []

const updateContentScrollClass = () => {
    const content = document.querySelector(".content")
    if (!content) return
    if (isReadable.value) {
        content.classList.remove("dialogueContent")
    } else {
        content.classList.add("dialogueContent")
    }
}

const reloadPage = () => {
    keyword.value = route.query.keyword
    playVoiceButtonDict = {}
    playableDialogueIdList = []
    totalCount.value = 0
    const isSubtitle = !!route.query.isSubtitle
    const readableId = route.query.readableId
    const fileName = route.query.fileName
    const questId = route.query.questId
    if ((readableId || fileName) && !isSubtitle) {
        isReadable.value = true
        subtitleCreatedVersion.value = ""
        subtitleUpdatedVersion.value = ""
        subtitleCreatedVersionRaw.value = ""
        subtitleUpdatedVersionRaw.value = ""
        showPlayer.value = false
        updateContentScrollClass()
        reloadReadable()
        return
    }
    if (questId) {
        isReadable.value = false
        readableCreatedVersion.value = ""
        readableUpdatedVersion.value = ""
        readableCreatedVersionRaw.value = ""
        readableUpdatedVersionRaw.value = ""
        subtitleCreatedVersion.value = ""
        subtitleUpdatedVersion.value = ""
        subtitleCreatedVersionRaw.value = ""
        subtitleUpdatedVersionRaw.value = ""
        showPlayer.value = false
        updateContentScrollClass()
        currentPage.value = 1
        reloadQuest()
        return
    }
    isReadable.value = false
    readableCreatedVersion.value = ""
    readableUpdatedVersion.value = ""
    readableCreatedVersionRaw.value = ""
    readableUpdatedVersionRaw.value = ""
    subtitleCreatedVersion.value = ""
    subtitleUpdatedVersion.value = ""
    subtitleCreatedVersionRaw.value = ""
    subtitleUpdatedVersionRaw.value = ""
    textHash.value = parseInt(route.query.textHash)
    updateContentScrollClass()
    reloadTalk()
}


const reloadTalk = () => {
    if (route.query.isSubtitle) {
        api.getSubtitleContext(route.query.fileName, route.query.subtitleId, route.query.searchLang).then(res => {
            let resJson = res.json
            queryTime.value = resJson.time.toFixed(2)
            let talkContents = resJson.contents
            questName.value = talkContents.talkQuestName
            dialogues.value = talkContents.dialogues
            subtitleCreatedVersion.value = talkContents.createdVersion || ""
            subtitleUpdatedVersion.value = talkContents.updatedVersion || ""
            subtitleCreatedVersionRaw.value = talkContents.createdVersionRaw || ""
            subtitleUpdatedVersionRaw.value = talkContents.updatedVersionRaw || ""
        }).catch(err => {
            if(!err.network) err.defaultHandler()
        })
        return
    }

    subtitleCreatedVersion.value = ""
    subtitleUpdatedVersion.value = ""
    subtitleCreatedVersionRaw.value = ""
    subtitleUpdatedVersionRaw.value = ""

    api.getTalkFromHash(textHash.value, route.query.searchLang).then(res => {
        let resJson = res.json
        queryTime.value = resJson.time.toFixed(2)
        let talkContents = resJson.contents
        questName.value = talkContents.talkQuestName
        dialogues.value = talkContents.dialogues

    }).catch(err => {
        if(!err.network) err.defaultHandler()
    })
}

const reloadReadable = () => {
    api.getReadableContent(route.query.readableId, route.query.fileName, route.query.searchLang).then(res => {
        let resJson = res.json
        queryTime.value = resJson.time.toFixed(2)
        let readableContents = resJson.contents
        readableTitle.value = readableContents.readableTitle || UI_TEXT.readableFallback
        readableFileName.value = readableContents.fileName || ""
        readableTranslates.value = readableContents.translates || {}
        readableCreatedVersion.value = readableContents.createdVersion || ""
        readableUpdatedVersion.value = readableContents.updatedVersion || ""
        readableCreatedVersionRaw.value = readableContents.createdVersionRaw || ""
        readableUpdatedVersionRaw.value = readableContents.updatedVersionRaw || ""
    }).catch(err => {
        if(!err.network) err.defaultHandler()
    })
}

const reloadQuest = () => {
    api.getQuestDialogues(
        route.query.questId,
        route.query.searchLang,
        currentPage.value,
        pageSize.value
    ).then(res => {
        let resJson = res.json
        queryTime.value = resJson.time.toFixed(2)
        let talkContents = resJson.contents
        questName.value = talkContents.talkQuestName
        dialogues.value = talkContents.dialogues
        totalCount.value = resJson.total || 0
        currentPage.value = resJson.page || currentPage.value
    }).catch(err => {
        if(!err.network) err.defaultHandler()
    })
}

const goToPage = (page) => {
    if (!route.query.questId) return
    if (page < 1) {
        page = 1
    } else if (page > totalPages.value) {
        page = totalPages.value
    }
    currentPage.value = page
    reloadQuest()
}

const normalizeCopyText = (text) => {
    if (!text) return ""
    const normalized = text.replace(/\\n/g, "\n")
    return normalized.replace(/\r\n/g, "\n").replace(/\r/g, "\n")
}

const copyReadableContent = async (langCode) => {
    const rawText = readableTranslates.value?.[langCode] || ""
    const text = normalizeCopyText(rawText)
    if (!text) {
        ElMessage.warning(UI_TEXT.noTextToCopy)
        return
    }
    try {
        if (navigator.clipboard?.writeText) {
            await navigator.clipboard.writeText(text)
            ElMessage.success(UI_TEXT.copied)
            return
        }

        const textarea = document.createElement('textarea')
        textarea.value = text
        textarea.setAttribute('readonly', '')
        textarea.style.position = 'absolute'
        textarea.style.left = '-9999px'
        document.body.appendChild(textarea)
        textarea.select()
        document.execCommand('copy')
        document.body.removeChild(textarea)
        ElMessage.success(UI_TEXT.copied)
    } catch (error) {
        console.error(error)
        ElMessage.error(UI_TEXT.copyFailed)
    }
}

const copyDialogueText = async (text) => {
    const normalized = normalizeCopyText(text)
    if (!normalized) {
        ElMessage.warning(UI_TEXT.noTextToCopy)
        return
    }
    try {
        if (navigator.clipboard?.writeText) {
            await navigator.clipboard.writeText(normalized)
            ElMessage.success(UI_TEXT.copied)
            return
        }

        const textarea = document.createElement('textarea')
        textarea.value = normalized
        textarea.setAttribute('readonly', '')
        textarea.style.position = 'absolute'
        textarea.style.left = '-9999px'
        document.body.appendChild(textarea)
        textarea.select()
        document.execCommand('copy')
        document.body.removeChild(textarea)
        ElMessage.success(UI_TEXT.copied)
    } catch (error) {
        console.error(error)
        ElMessage.error(UI_TEXT.copyFailed)
    }
}

const isCopyableText = (text) => {
    const normalized = normalizeCopyText(text)
    return normalized.trim().length > 0
}

const formatVersionTag = (versionTag, rawVersion) => {
    if (versionTag) return versionTag
    if (rawVersion) return String(rawVersion)
    return UI_TEXT.unknown
}

const shouldShowUpdatedVersionTag = (createdTag, createdRaw, updatedTag, updatedRaw) => {
    return formatVersionTag(createdTag, createdRaw) !== formatVersionTag(updatedTag, updatedRaw)
}

const displayLanguages = computed(() => {
    let langs = [...global.config.resultLanguages]
    let searchLang = parseInt(route.query.searchLang)
    if (searchLang && !langs.includes(searchLang)) {
        langs.push(searchLang)
    }
    return langs
})

const dialogueGroups = computed(() => {
    if (!dialogues.value || dialogues.value.length === 0) {
        return []
    }
    const hasTalkId = dialogues.value.some(item => item.talkId)
    if (!hasTalkId) {
        return [{ talkId: null, rows: dialogues.value }]
    }
    const groups = []
    let currentTalkId = null
    let currentRows = []
    dialogues.value.forEach((item) => {
        if (item.talkId !== currentTalkId) {
            if (currentRows.length > 0) {
                groups.push({ talkId: currentTalkId, rows: currentRows })
            }
            currentTalkId = item.talkId
            currentRows = [item]
        } else {
            currentRows.push(item)
        }
    })
    if (currentRows.length > 0) {
        groups.push({ talkId: currentTalkId, rows: currentRows })
    }
    return groups
})

// Audio player behavior
/**
 *
 * @type {Ref<AudioPlayer>}
 */
const voicePlayer = ref()
const showPlayer = ref(false)
const autoLoop = ref(true)
const showDialogueVersions = ref(false)
let firstShowPlayer = true
const audio = ref([])
const currentPlayingIndex = ref(-1)
const autoScroll = ref(true)
const voiceListLoadingInfo = reactive(({
    showLoadingDialogue: false,
    total: 1,
    current: 0,
    percentage: 0,
    audioLoaded: false
}))



const onHidePlayerButtonClicked = () => {
    showPlayer.value = false
}

const onShowPlayerButtonClicked = () => {
    showPlayer.value = true
}

const onVoicePlay = (voiceUrl, dialogueId) => {
    currentPlayingIndex.value = 0
    voicePlayer.value.currentPlayIndex = 0
    playableDialogueIdList = [dialogueId]
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
        // Audio player behavior
        setTimeout(()=>{
            voicePlayer.value.play()
        }, 100)

    }

}

const playAllLangVoice = async (langCode) => {
    let newAudios = []
    playableDialogueIdList = []
    voiceListLoadingInfo.total = dialogues.value.length
    voiceListLoadingInfo.current = 0
    voiceListLoadingInfo.percentage = 0
    if (!voiceListLoadingInfo.audioLoaded)
        voiceListLoadingInfo.showLoadingDialogue = true
    for(let dialogue of dialogues.value) {
        if(!playVoiceButtonDict[langCode][dialogue.dialogueId]) {
            voiceListLoadingInfo.current += 1;
            voiceListLoadingInfo.percentage = 100 * voiceListLoadingInfo.current / voiceListLoadingInfo.total
            continue
        }
        let currentUrl = await playVoiceButtonDict[langCode][dialogue.dialogueId].getAudioUrl()
        newAudios.push(currentUrl)
        playableDialogueIdList.push(dialogue.dialogueId)
        voiceListLoadingInfo.current += 1;
        voiceListLoadingInfo.percentage = 100 * voiceListLoadingInfo.current / voiceListLoadingInfo.total
    }

    voiceListLoadingInfo.showLoadingDialogue = false
    voiceListLoadingInfo.audioLoaded = true


    if(newAudios.length === 0) return

    if(firstShowPlayer){
        showPlayer.value = true;
        firstShowPlayer = false
    }


    audio.value = newAudios
    voicePlayer.value.currentPlayIndex = 0
    // Audio player behavior
    setTimeout(()=>{
        voicePlayer.value.play()
    }, 100)


}

const registerVoicePlayButton = (buttonObj, langCode, dialogueId) => {
    if(!(langCode in playVoiceButtonDict)) {
        playVoiceButtonDict[langCode] = {}
    }
    playVoiceButtonDict[langCode][dialogueId] = buttonObj
}

const onBeforeNextAudio = (next) => {
    if(voicePlayer.value.currentPlayIndex < audio.value.length - 1) {
        next()
    }
}

const onPlay = () => {
    currentPlayingIndex.value = voicePlayer.value.currentPlayIndex
    if(autoScroll.value) {
        let dialogueId = playableDialogueIdList[currentPlayingIndex.value]
        let langCode = Object.getOwnPropertyNames(playVoiceButtonDict)[0]
        let voiceButton = playVoiceButtonDict[langCode][dialogueId]
        voiceButton.scrollTo()
    }


}

const tableRowClassName = ({row, rowIndex}) => {
    if(currentPlayingIndex.value >= 0 && row.dialogueId === playableDialogueIdList[currentPlayingIndex.value] ) {
        return 'playingDialogue'
    }
    return ''
}

onActivated(() => {
    reloadPage()
    voiceListLoadingInfo.audioLoaded = false
})

onDeactivated(() => {
    voicePlayer.value && voicePlayer.value.pause()
    const content = document.querySelector(".content")
    content && content.classList.remove("dialogueContent")
})

</script>

<template>
    <div class="viewWrapper" :class="{dialogueView: !isReadable}">
        <h1 class="pageTitle">{{ isReadable ? UI_TEXT.pageTitleReadable : UI_TEXT.pageTitleDialogue }}</h1>
        <div class="helpText">
            <p v-if="!isReadable">{{ UI_TEXT.source }}: {{ questName }}</p>
            <p v-else>{{ UI_TEXT.source }}: {{ readableTitle }}<span v-if="readableFileName"> ({{ readableFileName }})</span></p>
            <p>{{ UI_TEXT.queryTime }}: {{queryTime}} ms</p>

            <div v-if="isReadable" class="versionTags">
                <el-tag size="small" effect="plain" :title="readableCreatedVersionRaw">{{ UI_TEXT.created }}: {{ formatVersionTag(readableCreatedVersion, readableCreatedVersionRaw) }}</el-tag>
                <el-tag
                    v-if="shouldShowUpdatedVersionTag(readableCreatedVersion, readableCreatedVersionRaw, readableUpdatedVersion, readableUpdatedVersionRaw)"
                    size="small"
                    effect="plain"
                    :title="readableUpdatedVersionRaw"
                >
                    {{ UI_TEXT.updated }}: {{ formatVersionTag(readableUpdatedVersion, readableUpdatedVersionRaw) }}
                </el-tag>
            </div>
            <div v-else-if="route.query.isSubtitle" class="versionTags">
                <el-tag size="small" effect="plain" :title="subtitleCreatedVersionRaw">{{ UI_TEXT.created }}: {{ formatVersionTag(subtitleCreatedVersion, subtitleCreatedVersionRaw) }}</el-tag>
                <el-tag
                    v-if="shouldShowUpdatedVersionTag(subtitleCreatedVersion, subtitleCreatedVersionRaw, subtitleUpdatedVersion, subtitleUpdatedVersionRaw)"
                    size="small"
                    effect="plain"
                    :title="subtitleUpdatedVersionRaw"
                >
                    {{ UI_TEXT.updated }}: {{ formatVersionTag(subtitleUpdatedVersion, subtitleUpdatedVersionRaw) }}
                </el-tag>
            </div>
        </div>

        <div v-if="isReadable" class="readableContent">
            <div v-for="langCode in displayLanguages" :key="langCode" class="readableBlock">
                <div class="readableHeader">
                    <h3>{{ global.languages[langCode] }}</h3>
                    <el-button
                        size="small"
                        class="copyReadableButton"
                        :icon="CopyDocument"
                        @click="copyReadableContent(langCode)"
                    >
                        {{ UI_TEXT.copy }}
                    </el-button>
                </div>
                <StylizedText :text="readableTranslates[langCode]" :keyword="keyword" />
            </div>
        </div>

        <div v-else class="dialogueScroll">
            <div class="dialogueTopControls">
                <el-form :inline="true">
                    <el-form-item :label="UI_TEXT.showTextVersion">
                        <el-switch v-model="showDialogueVersions" />
                    </el-form-item>
                </el-form>
            </div>
            <div class="resultControls" v-if="totalCount > 0">
                <span class="resultCount">{{ UI_TEXT.total }} {{ totalCount }} {{ UI_TEXT.currentPage }} {{ currentPage }} / {{ totalPages }} {{ UI_TEXT.page }}</span>
                <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(1)">{{ UI_TEXT.first }}</el-button>
                <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(currentPage - 1)">{{ UI_TEXT.prev }}</el-button>
                <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(currentPage + 1)">{{ UI_TEXT.next }}</el-button>
                <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(totalPages)">{{ UI_TEXT.last }}</el-button>
            </div>
            <div class="dialogueGroups">
                <div v-for="group in dialogueGroups" :key="group.talkId || 'single'" class="dialogueGroup">
                    <h3 v-if="group.talkId" class="dialogueGroupTitle">Talk ID: {{ group.talkId }}</h3>
                    <el-table :data="group.rows" :row-class-name="tableRowClassName">
                        <el-table-column prop="talker" :label="UI_TEXT.speaker" width="110" />
                        <el-table-column v-if="showDialogueVersions" :label="UI_TEXT.version" width="110">
                            <template #default="scope">
                                <div class="rowVersionTags">
                                    <el-tag
                                        size="small"
                                        effect="plain"
                                        class="versionTag"
                                        :title="scope.row.createdVersionRaw || ''"
                                    >
                                        {{ UI_TEXT.created }}: {{ formatVersionTag(scope.row.createdVersion, scope.row.createdVersionRaw) }}
                                    </el-tag>
                                    <el-tag
                                        v-if="shouldShowUpdatedVersionTag(scope.row.createdVersion, scope.row.createdVersionRaw, scope.row.updatedVersion, scope.row.updatedVersionRaw)"
                                        size="small"
                                        effect="plain"
                                        class="versionTag"
                                        :title="scope.row.updatedVersionRaw || ''"
                                    >
                                        {{ UI_TEXT.updated }}: {{ formatVersionTag(scope.row.updatedVersion, scope.row.updatedVersionRaw) }}
                                    </el-tag>
                                </div>
                            </template>
                        </el-table-column>
                        <template v-for="langCode in displayLanguages">
                            <el-table-column width="40">
                                <template #header>
                                    <el-tooltip :content="UI_TEXT.playAll + global.languages[langCode] + UI_TEXT.voice">
                                        <el-icon @click="playAllLangVoice(langCode)"><VideoPlay /></el-icon>
                                    </el-tooltip>
                                </template>
                                <template #default="scope">
                                    <span v-if="global.voiceLanguages[langCode]">
                                        <PlayVoiceButton v-for="voice in scope.row.voicePaths"
                                                         :voice-path="voice" :lang-code="langCode"
                                                         @on-voice-play="(url) =>{ onVoicePlay(url, scope.row.dialogueId)}"
                                                         :ref = "(el) => {registerVoicePlayButton(el, langCode, scope.row.dialogueId)}"
                                        />
                                    </span>
                                </template>
                            </el-table-column>
                            <el-table-column :label="global.languages[langCode]" >
                                <template #default="scope">
                                    <div class="dialogueCell">
                                        <StylizedText :text="scope.row.translates[langCode]" :keyword="keyword"/>
                                        <el-button
                                            v-if="isCopyableText(scope.row.translates[langCode])"
                                            class="copyDialogueButton"
                                            :icon="CopyDocument"
                                            circle
                                            size="small"
                                            @click="copyDialogueText(scope.row.translates[langCode])"
                                        />
                                    </div>
                                </template>
                            </el-table-column>
                        </template>
                    </el-table>
                </div>
            </div>
            <div class="resultControls" v-if="totalCount > 0">
                <span class="resultCount">{{ UI_TEXT.total }} {{ totalCount }} {{ UI_TEXT.currentPage }} {{ currentPage }} / {{ totalPages }} {{ UI_TEXT.page }}</span>
                <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(1)">{{ UI_TEXT.first }}</el-button>
                <el-button size="small" :disabled="currentPage <= 1" @click="goToPage(currentPage - 1)">{{ UI_TEXT.prev }}</el-button>
                <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(currentPage + 1)">{{ UI_TEXT.next }}</el-button>
                <el-button size="small" :disabled="currentPage >= totalPages" @click="goToPage(totalPages)">{{ UI_TEXT.last }}</el-button>
            </div>
        </div>

        <el-form v-if="!isReadable" style="margin-top: 10px;" :inline="true">
            <el-form-item :label="UI_TEXT.autoLoop">
                <el-switch v-model="autoLoop" />
            </el-form-item>
            <el-form-item :label="UI_TEXT.autoScroll">
                <el-switch v-model="autoScroll" />
            </el-form-item>
        </el-form>
    </div>

    <div class="viewWrapper voicePlayerContainer" v-show="showPlayer" v-if="!isReadable">
        <span class="hideIcon" @click="onHidePlayerButtonClicked">
            <el-icon>
                <Close />
            </el-icon>
        </span>

        <AudioPlayer
            ref="voicePlayer"
            :audio-list="audio"
            :is-loop="autoLoop"
            :progress-interval="25"
            theme-color="var(--el-color-primary)"
            :before-next="onBeforeNextAudio"
            @play="onPlay">
        </AudioPlayer>
    </div>

    <div class="showPlayerButton" @click="onShowPlayerButtonClicked" v-show="!showPlayer" v-if="!isReadable">
        <i class="fi fi-sr-waveform-path"></i>
    </div>

    <el-dialog
        v-model="voiceListLoadingInfo.showLoadingDialogue" :width="300"
        :show-close="false" :title="UI_TEXT.loadingVoices" :close-on-press-escape="false">
        <el-progress :percentage="voiceListLoadingInfo.percentage">
            {{voiceListLoadingInfo.current}} / {{voiceListLoadingInfo.total}}
        </el-progress>
    </el-dialog>
</template>
<style scoped>
.viewWrapper{
    position: relative;
    width: 85%;
    margin: 0 auto;
    background-color: #fff;
    box-shadow: 0 3px 3px rgba(36,37,38,.05);
    border-radius: 3px;
    padding: 20px;
}

.pageTitle {
    border-bottom: 1px #ccc solid;
    padding-bottom: 10px;
}

.helpText {
    margin: 20px 0 20px 0;
    color: #999;
}

.readableContent {
    display: flex;
    flex-direction: column;
    gap: 16px;
}

.readableBlock {
    padding: 12px 0;
    border-bottom: 1px solid #eee;
}

.readableHeader {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
}

.copyReadableButton {
    flex-shrink: 0;
}

.readableBlock:last-child {
    border-bottom: none;
}

.dialogueGroups {
    display: flex;
    flex-direction: column;
    gap: 16px;
}



.dialogueScroll {
    overflow-x: visible;
    width: 100%;
}

.dialogueTopControls {
    margin-bottom: 8px;
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

.dialogueView {
    min-width: 1000px;
}

.dialogueCell {
    display: flex;
    align-items: flex-start;
    gap: 8px;
}

.copyDialogueButton {
    margin-top: 2px;
    flex-shrink: 0;
}

.dialogueGroupTitle {
    margin: 8px 0;
    font-weight: 600;
}

.voicePlayerContainer {
    margin-top: 10px;
    bottom: 0;
    position: sticky !important;
    box-shadow: 0 0 5px 5px rgba(36,37,38,.05);
    z-index: 9999;
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
    z-index: 9999;
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

:deep( .playingDialogue) {
    background-color: var(--el-color-primary-light-9);
}

@media (max-width: 720px) {
    .showPlayerButton {
        right: 16px;
        bottom: 24px;
        width: 56px;
        height: 56px;
        line-height: 60px;
    }
}

.versionTags {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 6px;
}
.rowVersionTags {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
}
</style>
