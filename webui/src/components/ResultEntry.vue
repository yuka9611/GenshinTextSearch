<script setup>
import global from "@/global/global.js"
import PlayVoiceButton from "@/components/PlayVoiceButton.vue"
import StylizedText from "@/components/StylizedText.vue"
import { useRouter } from "vue-router"
import { ElMessage } from "element-plus"
import { CopyDocument } from "@element-plus/icons-vue"

const UI_TEXT = Object.freeze({
    noTextToCopy: "没有可复制的文本",
    copied: "已复制",
    copyFailed: "复制失败，请手动复制",
    unknown: "未知",
    created: "创建",
    updated: "更新",
    source: "来源",
})

const props = defineProps(["translateObj", "keyword", "searchLang"])
const emit = defineEmits(["onVoicePlay"])
const router = useRouter()

const onVoicePlay = (voiceUrl) => {
    emit("onVoicePlay", voiceUrl)
}

const canOpenDetail = () => {
    if (props.translateObj.disableDetail) return false
    return Boolean(
        props.translateObj.isTalk ||
        props.translateObj.isSubtitle ||
        props.translateObj.viewAsTextHash
    )
}

const normalizeCopyText = (text) => {
    if (!text) return ""
    const normalized = text.replace(/\\n/g, "\n")
    return normalized.replace(/\r\n/g, "\n").replace(/\r/g, "\n")
}

const copyToClipboard = async (text) => {
    const sanitizedText = normalizeCopyText(text)
    if (!sanitizedText) {
        ElMessage.warning(UI_TEXT.noTextToCopy)
        return
    }
    try {
        if (navigator.clipboard?.writeText) {
            await navigator.clipboard.writeText(sanitizedText)
            ElMessage.success(UI_TEXT.copied)
            return
        }

        const textarea = document.createElement("textarea")
        textarea.value = sanitizedText
        textarea.setAttribute("readonly", "")
        textarea.style.position = "absolute"
        textarea.style.left = "-9999px"
        document.body.appendChild(textarea)
        textarea.select()
        document.execCommand("copy")
        document.body.removeChild(textarea)
        ElMessage.success(UI_TEXT.copied)
    } catch (error) {
        console.error(error)
        ElMessage.error(UI_TEXT.copyFailed)
    }
}

const gotoTalk = () => {
    if (props.translateObj.isSubtitle) {
        const query = {
            fileName: props.translateObj.fileName,
            keyword: props.keyword,
            isSubtitle: 1,
            searchLang: props.searchLang,
        }
        if (props.translateObj.subtitleId) {
            query.subtitleId = props.translateObj.subtitleId
        }
        router.push({ path: "/talk", query })
        return
    }
    if (!(props.translateObj.isTalk || props.translateObj.viewAsTextHash)) return
    router.push({
        path: "/talk",
        query: {
            textHash: props.translateObj.hash,
            keyword: props.keyword,
            searchLang: props.searchLang,
        },
    })
}

const formatVersion = (versionTag, rawVersion) => {
    if (versionTag) return versionTag
    if (rawVersion) return String(rawVersion)
    return UI_TEXT.unknown
}

const showUpdatedVersionTag = () => {
    const created = formatVersion(props.translateObj.createdVersion, props.translateObj.createdVersionRaw)
    const updated = formatVersion(props.translateObj.updatedVersion, props.translateObj.updatedVersionRaw)
    return created !== updated
}
</script>

<template>
    <div class="entry">
        <div class="translate" v-for="(translate, translateKey) in props.translateObj.translates" :key="translateKey">
            <p class="info">
                {{ global.languages[translateKey] }}:
                <span v-if="global.voiceLanguages[translateKey]">
                    <PlayVoiceButton
                        v-for="voice in props.translateObj.voicePaths"
                        :key="`${voice}-${translateKey}`"
                        :voice-path="voice"
                        :lang-code="translateKey"
                        @on-voice-play="onVoicePlay"
                    />
                </span>
                <el-button
                    class="copyButton"
                    :icon="CopyDocument"
                    circle
                    size="small"
                    @click="copyToClipboard(translate)"
                />
            </p>
            <StylizedText :text="translate" :keyword="$props.keyword" />
        </div>

        <div class="versionTags">
            <el-tag
                size="small"
                effect="plain"
                class="versionTag"
                :title="props.translateObj.createdVersionRaw || ''"
            >
                {{ UI_TEXT.created }}: {{ formatVersion(props.translateObj.createdVersion, props.translateObj.createdVersionRaw) }}
            </el-tag>
            <el-tag
                v-if="showUpdatedVersionTag()"
                size="small"
                effect="plain"
                class="versionTag"
                :title="props.translateObj.updatedVersionRaw || ''"
            >
                {{ UI_TEXT.updated }}: {{ formatVersion(props.translateObj.updatedVersion, props.translateObj.updatedVersionRaw) }}
            </el-tag>
        </div>

        <p class="info">
            <span class="origin" :class="{ talkOrigin: canOpenDetail() }" @click="gotoTalk">
                {{ UI_TEXT.source }}: {{ props.translateObj.origin }}
                <span class="gotoIcon" v-if="canOpenDetail()">&gt;</span>
            </span>
        </p>
    </div>
</template>

<style scoped>
.translate {
    margin-bottom: 10px;
}

.entry {
    padding-bottom: 20px;
    padding-top: 20px;
    line-height: 30px;
}

.info {
    font-size: 14px;
}

.copyButton {
    margin-left: 8px;
    vertical-align: middle;
}

.origin {
    color: #ab9d96;
}

.talkOrigin {
    cursor: pointer;
    transition: 0.3s;
}

.talkOrigin:hover {
    opacity: 0.8;
}

.talkOrigin > .gotoIcon {
    transition: 0.3s;
    margin-left: 0;
}

.talkOrigin:hover > .gotoIcon {
    padding-left: 5px;
}

.versionTags {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin: 6px 0;
}

.versionTag {
    border-radius: 999px;
}
</style>
