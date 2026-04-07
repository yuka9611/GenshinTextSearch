<script setup>
import global from "@/global/global"
import api from "@/api/keywordQuery";

import {useRoute} from "vue-router";
import {onDeactivated, reactive, ref, watch, computed} from "vue";
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
    pageTitleDialogue: "对话内容",
    source: "来源",
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
    talkId: "对话 ID",
    audioUnavailable: "当前语言暂无语音",
    audioMissing: "未找到语音文件",
    noPlayableAudio: "当前语言没有可播放的语音",
})

const route = useRoute()
const keyword = ref("")
const questName = ref(UI_TEXT.taskText)
const textHash = ref(0)
const dialogues = ref([])
const isReadable = ref(false)
const readableTitle = ref("")
const readableFileName = ref("")
const readableTranslates = ref({})
const questDescription = ref("")
const readableCreatedVersion = ref("")
const readableUpdatedVersion = ref("")
const readableCreatedVersionRaw = ref("")
const readableUpdatedVersionRaw = ref("")
const questCreatedVersion = ref("")
const questUpdatedVersion = ref("")
const questCreatedVersionRaw = ref("")
const questUpdatedVersionRaw = ref("")
const subtitleCreatedVersion = ref("")
const subtitleUpdatedVersion = ref("")
const subtitleCreatedVersionRaw = ref("")
const subtitleUpdatedVersionRaw = ref("")
const pageSize = ref(200)
const currentPage = ref(1)
const totalCount = ref(0)
const totalPages = computed(() => Math.max(1, Math.ceil(totalCount.value / pageSize.value)))
const currentTalkId = ref(null)
const currentQuestId = ref(null)
const currentDialogueId = ref(null)

let playVoiceButtonDict = {}
let playableDialogueIdList = []

const toDisplayId = (value) => {
    if (value === null || value === undefined) return null
    const text = String(value).trim()
    if (!text || text === "0" || text === "NaN") return null
    return text
}

const displayMetaIds = computed(() => {
    const items = []
    const talkIdValue = route.query.groupMode === 'npc'
        ? (currentTalkId.value === null || currentTalkId.value === undefined ? null : String(currentTalkId.value))
        : toDisplayId(currentTalkId.value)
    const questIdValue = toDisplayId(currentQuestId.value)
    const dialogueIdValue = toDisplayId(currentDialogueId.value)
    if (talkIdValue) {
        items.push({ label: "Talk ID", value: talkIdValue })
    }
    if (questIdValue) {
        items.push({ label: "Quest ID", value: questIdValue })
    }
    if (dialogueIdValue) {
        items.push({ label: "Dialogue ID", value: dialogueIdValue })
    }
    return items
})

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
    questName.value = UI_TEXT.taskText
    dialogues.value = []
    readableTitle.value = ""
    readableFileName.value = ""
    readableTranslates.value = {}
    audio.value = []
    currentPlayingIndex.value = -1
    totalCount.value = 0
    questDescription.value = ""
    currentTalkId.value = null
    currentQuestId.value = null
    currentDialogueId.value = null
    readableCreatedVersion.value = ""
    readableUpdatedVersion.value = ""
    readableCreatedVersionRaw.value = ""
    readableUpdatedVersionRaw.value = ""
    questCreatedVersion.value = ""
    questUpdatedVersion.value = ""
    questCreatedVersionRaw.value = ""
    questUpdatedVersionRaw.value = ""
    subtitleCreatedVersion.value = ""
    subtitleUpdatedVersion.value = ""
    subtitleCreatedVersionRaw.value = ""
    subtitleUpdatedVersionRaw.value = ""
    voiceListLoadingInfo.showLoadingDialogue = false
    voiceListLoadingInfo.total = 1
    voiceListLoadingInfo.current = 0
    voiceListLoadingInfo.percentage = 0
    voiceListLoadingInfo.audioLoaded = false
    const isSubtitle = !!route.query.isSubtitle
    const readableId = route.query.readableId
    const fileName = route.query.fileName
    const questId = route.query.questId
    const isDialogueGroup = route.query.groupMode === 'npc'
    if ((readableId || fileName) && !isSubtitle) {
        isReadable.value = true
        totalCount.value = 0
        currentPage.value = 1
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
        currentQuestId.value = questId
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
    if (isDialogueGroup) {
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
        reloadDialogueGroup()
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
    currentPage.value = 1
    updateContentScrollClass()
    reloadTalk(false)
}

const reloadDialogueGroup = () => {
    api.getDialogueGroup(
        route.query.talkId,
        route.query.coopQuestId,
        route.query.dialogueIdFallback,
        route.query.searchLang,
        currentPage.value,
        pageSize.value,
    ).then(res => {
        let resJson = res.json
        let talkContents = resJson.contents
        questName.value = talkContents.talkQuestName
        dialogues.value = talkContents.dialogues
        questDescription.value = ""
        currentTalkId.value = talkContents.talkId ?? route.query.talkId ?? null
        currentQuestId.value = null
        currentDialogueId.value = talkContents.dialogueIdFallback ?? route.query.dialogueIdFallback ?? null
        totalCount.value = resJson.total || talkContents.total || talkContents.dialogues?.length || 0
        currentPage.value = resJson.page || talkContents.page || currentPage.value
    }).catch(err => {
        if(!err.network) err.defaultHandler()
    })
}


const reloadTalk = (useCurrentPage = true) => {
    if (route.query.isSubtitle) {
        api.getSubtitleContext(route.query.fileName, route.query.subtitleId, route.query.searchLang).then(res => {
            let resJson = res.json
            let talkContents = resJson.contents
            questName.value = talkContents.talkQuestName
            dialogues.value = talkContents.dialogues
            questDescription.value = ""
            currentTalkId.value = null
            currentQuestId.value = null
            totalCount.value = talkContents.dialogues?.length || 0
            currentPage.value = 1
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

    api.getTalkFromHash(
        textHash.value,
        route.query.searchLang,
        useCurrentPage ? currentPage.value : null,
        pageSize.value,
    ).then(res => {
        let resJson = res.json
        let talkContents = resJson.contents
        questName.value = talkContents.talkQuestName
        dialogues.value = talkContents.dialogues
        questDescription.value = ""
        currentTalkId.value = talkContents.talkId ?? null
        currentQuestId.value = talkContents.questId ?? null
        currentDialogueId.value = null
        totalCount.value = resJson.total || talkContents.total || talkContents.dialogues?.length || 0
        currentPage.value = resJson.page || talkContents.page || 1

    }).catch(err => {
        if(!err.network) err.defaultHandler()
    })
}

const reloadReadable = () => {
    api.getReadableContent(route.query.readableId, route.query.fileName, route.query.searchLang).then(res => {
        let resJson = res.json
        let readableContents = resJson.contents
        questDescription.value = ""
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
        let talkContents = resJson.contents
        questName.value = talkContents.talkQuestName
        questDescription.value = talkContents.questDescription || ""
        dialogues.value = talkContents.dialogues
        currentTalkId.value = talkContents.talkId ?? null
        currentQuestId.value = talkContents.questId ?? route.query.questId ?? null
        currentDialogueId.value = null
        questCreatedVersion.value = talkContents.createdVersion || ""
        questUpdatedVersion.value = talkContents.updatedVersion || ""
        questCreatedVersionRaw.value = talkContents.createdVersionRaw || ""
        questUpdatedVersionRaw.value = talkContents.updatedVersionRaw || ""
        totalCount.value = resJson.total || 0
        currentPage.value = resJson.page || currentPage.value
    }).catch(err => {
        if(!err.network) err.defaultHandler()
    })
}

const goToPage = (page) => {
    if (page < 1) {
        page = 1
    } else if (page > totalPages.value) {
        page = totalPages.value
    }
    currentPage.value = page
    if (route.query.questId) {
        reloadQuest()
        return
    }
    if (route.query.groupMode === 'npc') {
        reloadDialogueGroup()
        return
    }
    if (route.query.textHash && !route.query.isSubtitle) {
        reloadTalk(true)
    }
}

const onPageSizeChange = () => {
    currentPage.value = 1
    goToPage(1)
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

const resolveVersionValue = (versionTag, rawVersion) => {
    if (versionTag) return String(versionTag).trim()
    if (rawVersion) return String(rawVersion).trim()
    return ''
}

const formatVersionTag = (versionTag, rawVersion) => {
    return resolveVersionValue(versionTag, rawVersion) || UI_TEXT.unknown
}

const hasVersionTag = (versionTag, rawVersion) => {
    return !!resolveVersionValue(versionTag, rawVersion)
}

const shouldShowUpdatedVersionTag = (createdTag, createdRaw, updatedTag, updatedRaw) => {
    const updatedValue = resolveVersionValue(updatedTag, updatedRaw)
    if (!updatedValue) return false
    const createdValue = resolveVersionValue(createdTag, createdRaw)
    return createdValue !== updatedValue
}

const displayLanguages = computed(() => {
    let langs = [...global.config.resultLanguages]
    let searchLang = parseInt(route.query.searchLang)
    if (searchLang && !langs.includes(searchLang)) {
        langs.push(searchLang)
    }
    return langs
})

const isVoiceAvailableForLang = (entry, langCode) => {
    const availableVoiceLangs = entry?.availableVoiceLangs || []
    return entry?.voicePaths?.length > 0 && availableVoiceLangs.includes(Number(langCode))
}

const shouldShowVoiceButton = (entry, langCode) => {
    if (!entry?.voicePaths?.length) {
        return false
    }
    if (isVoiceAvailableForLang(entry, langCode)) {
        return true
    }
    return !(entry?.availableVoiceLangs?.length > 0)
}

const hasPlayableVoicesForLang = (langCode) => {
    return dialogues.value.some((entry) => isVoiceAvailableForLang(entry, langCode))
}

const dialogueGroups = computed(() => {
    if (!dialogues.value || dialogues.value.length === 0) {
        return []
    }
    const hasTalkId = dialogues.value.some(item => item.talkId)
    if (!hasTalkId) {
        return [{ talkId: null, stepTitle: '', rows: dialogues.value }]
    }
    const groups = []
    let currentTalkId = null
    let currentRows = []
    dialogues.value.forEach((item) => {
        if (item.talkId !== currentTalkId) {
            if (currentRows.length > 0) {
                groups.push({
                    talkId: currentTalkId,
                    stepTitle: currentRows[0]?.stepTitle || '',
                    rows: currentRows
                })
            }
            currentTalkId = item.talkId
            currentRows = [item]
        } else {
            currentRows.push(item)
        }
    })
    if (currentRows.length > 0) {
        groups.push({
            talkId: currentTalkId,
            stepTitle: currentRows[0]?.stepTitle || '',
            rows: currentRows
        })
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
    if (!hasPlayableVoicesForLang(langCode)) {
        ElMessage.warning(UI_TEXT.noPlayableAudio)
        return
    }

    let newAudios = []
    playableDialogueIdList = []
    voiceListLoadingInfo.total = dialogues.value.length
    voiceListLoadingInfo.current = 0
    voiceListLoadingInfo.percentage = 0
    if (!voiceListLoadingInfo.audioLoaded)
        voiceListLoadingInfo.showLoadingDialogue = true
    for(let dialogue of dialogues.value) {
        const voiceButton = playVoiceButtonDict[langCode]?.[dialogue.dialogueId]
        if(!voiceButton) {
            voiceListLoadingInfo.current += 1;
            voiceListLoadingInfo.percentage = 100 * voiceListLoadingInfo.current / voiceListLoadingInfo.total
            continue
        }
        let currentUrl = await voiceButton.getAudioUrl(false)
        if (currentUrl) {
            newAudios.push(currentUrl)
            playableDialogueIdList.push(dialogue.dialogueId)
        }
        voiceListLoadingInfo.current += 1;
        voiceListLoadingInfo.percentage = 100 * voiceListLoadingInfo.current / voiceListLoadingInfo.total
    }

    voiceListLoadingInfo.showLoadingDialogue = false
    voiceListLoadingInfo.audioLoaded = true


    if(newAudios.length === 0) {
        ElMessage.warning(UI_TEXT.noPlayableAudio)
        return
    }

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
        voiceButton && voiceButton.scrollTo()
    }


}

const tableRowClassName = ({row, rowIndex}) => {
    const classNames = []
    if (row.isSelectedHash) {
        classNames.push('selectedHashDialogue')
    }
    if(currentPlayingIndex.value >= 0 && row.dialogueId === playableDialogueIdList[currentPlayingIndex.value] ) {
        classNames.push('playingDialogue')
    }
    return classNames.join(' ')
}

watch(() => route.fullPath, () => {
    if (route.name !== 'talkView') {
        return
    }
    reloadPage()
}, { immediate: true })

onDeactivated(() => {
    voicePlayer.value && voicePlayer.value.pause()
    const content = document.querySelector(".content")
    content && content.classList.remove("dialogueContent")
})

</script>

<template>
    <div class="viewWrapper detailShell" :class="{ pageShell: isReadable, dialogueView: !isReadable }">
        <div class="detailHero">
            <div class="detailHeroMain">
                <h1 class="pageTitle">{{ isReadable ? UI_TEXT.pageTitleReadable : UI_TEXT.pageTitleDialogue }}</h1>
                <p v-if="!isReadable" class="detailSourceLine">{{ UI_TEXT.source }}: {{ questName }}</p>
                <p v-else class="detailSourceLine">
                    {{ UI_TEXT.source }}: {{ readableTitle }}
                    <span v-if="readableFileName" class="detailSourceFile">({{ readableFileName }})</span>
                </p>
                <div
                    v-if="(!isReadable && displayMetaIds.length) || isReadable || route.query.questId || route.query.isSubtitle"
                    class="detailMetaRow"
                >
                    <div v-if="!isReadable && displayMetaIds.length" class="metaIdTags tagRow">
                        <el-tag
                            v-for="item in displayMetaIds"
                            :key="item.label"
                            size="small"
                            effect="plain"
                        >
                            {{ item.label }}: {{ item.value }}
                        </el-tag>
                    </div>
                    <div v-if="isReadable" class="versionTags tagRow">
                        <el-tag size="small" effect="plain" :title="readableCreatedVersionRaw">{{ UI_TEXT.created }}: {{ formatVersionTag(readableCreatedVersion, readableCreatedVersionRaw) }}</el-tag>
                        <el-tag
                            v-if="hasVersionTag(readableUpdatedVersion, readableUpdatedVersionRaw)"
                            size="small"
                            effect="plain"
                            :title="readableUpdatedVersionRaw"
                        >
                            {{ UI_TEXT.updated }}: {{ formatVersionTag(readableUpdatedVersion, readableUpdatedVersionRaw) }}
                        </el-tag>
                    </div>
                    <div v-else-if="route.query.questId" class="versionTags tagRow">
                        <el-tag size="small" effect="plain" :title="questCreatedVersionRaw">{{ UI_TEXT.created }}: {{ formatVersionTag(questCreatedVersion, questCreatedVersionRaw) }}</el-tag>
                        <el-tag
                            v-if="hasVersionTag(questUpdatedVersion, questUpdatedVersionRaw)"
                            size="small"
                            effect="plain"
                            :title="questUpdatedVersionRaw"
                        >
                            {{ UI_TEXT.updated }}: {{ formatVersionTag(questUpdatedVersion, questUpdatedVersionRaw) }}
                        </el-tag>
                    </div>
                    <div v-else-if="route.query.isSubtitle" class="versionTags tagRow">
                        <el-tag size="small" effect="plain" :title="subtitleCreatedVersionRaw">{{ UI_TEXT.created }}: {{ formatVersionTag(subtitleCreatedVersion, subtitleCreatedVersionRaw) }}</el-tag>
                        <el-tag
                            v-if="hasVersionTag(subtitleUpdatedVersion, subtitleUpdatedVersionRaw)"
                            size="small"
                            effect="plain"
                            :title="subtitleUpdatedVersionRaw"
                        >
                            {{ UI_TEXT.updated }}: {{ formatVersionTag(subtitleUpdatedVersion, subtitleUpdatedVersionRaw) }}
                        </el-tag>
                    </div>
                </div>
            </div>
        </div>

        <div v-if="!isReadable && questDescription" class="questDescriptionBlock">
            <div class="questDescriptionLabel">任务描述</div>
            <StylizedText :text="questDescription" :keyword="keyword" />
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
            <div class="dialogueToolbar">
                <el-form :inline="true" class="dialogueControlForm">
                    <el-form-item :label="UI_TEXT.showTextVersion">
                        <el-switch v-model="showDialogueVersions" />
                    </el-form-item>
                    <el-form-item :label="UI_TEXT.autoLoop">
                        <el-switch v-model="autoLoop" />
                    </el-form-item>
                    <el-form-item :label="UI_TEXT.autoScroll">
                        <el-switch v-model="autoScroll" />
                    </el-form-item>
                </el-form>
            </div>
            <div v-if="totalCount > 0" class="resultSummary">
                <span class="resultCount">共 <strong>{{ totalCount }}</strong> 条对话，当前 <strong>{{ currentPage }}</strong> / {{ totalPages }} 页</span>
            </div>
            <div class="dialogueGroups">
                <div v-for="group in dialogueGroups" :key="group.talkId || 'single'" class="dialogueGroup">
                    <h3 v-if="group.talkId" class="dialogueGroupTitle">
                        {{ group.stepTitle || `${UI_TEXT.talkId}: ${group.talkId}` }}
                    </h3>
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
                                        <el-icon :class="{ disabledPlayIcon: !hasPlayableVoicesForLang(langCode) }" @click="playAllLangVoice(langCode)"><VideoPlay /></el-icon>
                                    </el-tooltip>
                                </template>
                                <template #default="scope">
                                    <span v-if="shouldShowVoiceButton(scope.row, langCode)">
                                        <PlayVoiceButton v-for="voice in scope.row.voicePaths"
                                                          :voice-path="voice" :lang-code="langCode"
                                                          :disabled="!isVoiceAvailableForLang(scope.row, langCode)"
                                                          :disabled-tooltip="UI_TEXT.audioUnavailable"
                                                          :unavailable-message="UI_TEXT.audioMissing"
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
    </div>

    <div class="viewWrapper pageShell voicePlayerContainer audioDock" v-show="showPlayer" v-if="!isReadable">
        <span class="hideIcon audioDockClose" @click="onHidePlayerButtonClicked">
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

    <div class="showPlayerButton audioDockToggle" @click="onShowPlayerButtonClicked" v-show="!showPlayer" v-if="!isReadable">
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
.detailShell {
    display: flex;
    flex-direction: column;
    width: var(--page-width);
    margin: 0 auto;
    gap: 14px;
}

.detailHero {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 16px;
    flex-wrap: wrap;
}

.detailHeroMain {
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-width: 0;
}

.detailSourceLine {
    margin: 0;
    color: var(--theme-text-muted);
    line-height: 1.7;
}

.detailSourceFile {
    color: var(--theme-text-soft);
}

.detailMetaRow {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 8px 12px;
}

.metaIdTags {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 0;
}

.questDescriptionBlock {
    padding: 16px 18px;
    border-radius: 20px;
    border: 1px solid var(--theme-border);
    background: var(--search-section-muted-bg);
    color: var(--theme-text);
}

.questDescriptionLabel {
    margin-bottom: 6px;
    font-weight: 600;
    color: var(--theme-ink);
}

.readableContent {
    display: flex;
    flex-direction: column;
    gap: 14px;
}

.readableBlock {
    padding: 16px 18px;
    border-radius: 22px;
    border: 1px solid var(--theme-border);
    background: linear-gradient(180deg, var(--theme-card-strong), var(--theme-surface));
    box-shadow: 0 10px 24px rgba(44, 57, 54, 0.06);
}

.readableHeader {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 12px;
}

.readableHeader h3 {
    margin: 0;
    color: var(--theme-ink);
    font-size: 1rem;
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
    gap: 14px;
}

.dialogueGroup {
    padding: 0;
}

.dialogueScroll {
    overflow-x: auto;
    width: 100%;
}

.dialogueToolbar {
    margin-bottom: 10px;
    padding: 14px 16px;
    border-radius: 18px;
    border: 1px solid rgba(190, 164, 124, 0.24);
    background: rgba(47, 105, 101, 0.05);
}

.dialogueControlForm :deep(.el-form-item) {
    margin-bottom: 0;
    margin-right: 20px;
}

.resultSummary {
    margin: 0 0 12px;
    padding: 0;
}

.resultPagination {
    justify-content: center;
    margin-top: 24px;
    padding: 16px 0;
}

.dialogueView {
    width: 100%;
    max-width: none;
    min-width: 0;
}

.dialogueCell {
    display: flex;
    align-items: flex-start;
    gap: 10px;
}

.copyDialogueButton {
    flex-shrink: 0;
}

.dialogueGroupTitle {
    margin: 0 0 10px;
    padding: 0 4px;
    font-weight: 600;
    color: var(--theme-ink);
    font-family: var(--font-title);
}

.voicePlayerContainer {
    z-index: 9999;
}

.showPlayerButton{
    z-index: 9999;
}

:deep( .playingDialogue) {
    background-color: rgba(47, 105, 101, 0.10);
}

:deep(.selectedHashDialogue:not(.playingDialogue)) {
    background-color: rgba(183, 140, 79, 0.12);
}

.disabledPlayIcon {
    opacity: 0.35;
}

.versionTags {
    margin-top: 0;
}

.rowVersionTags {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
}

:deep(.el-table .cell) {
    color: var(--theme-text);
}

@media (max-width: 860px) {
    .dialogueToolbar {
        padding: 12px 14px;
    }

    .dialogueControlForm {
        display: flex;
        flex-wrap: wrap;
        gap: 10px 14px;
    }

    .dialogueControlForm :deep(.el-form-item) {
        margin-right: 0;
    }
}

</style>

<style>
[data-theme="dark"] .readableBlock {
    box-shadow: 0 16px 30px rgba(0, 0, 0, 0.18);
}
</style>
