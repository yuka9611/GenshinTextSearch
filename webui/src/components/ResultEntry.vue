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
    <div class="entry" :class="{ 'entry-with-voice': props.translateObj.voicePaths && props.translateObj.voicePaths.length > 0 }">
        <div class="translate" v-for="(translate, translateKey) in props.translateObj.translates" :key="translateKey">
            <p class="info">
                <span class="language-label">{{ global.languages[translateKey] }}:</span>
                <span v-if="global.voiceLanguages[translateKey]" class="voice-buttons">
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
                    :title="UI_TEXT.noTextToCopy"
                />
            </p>
            <StylizedText :text="translate" :keyword="$props.keyword" />
        </div>

        <div class="versionTags" v-if="props.translateObj.createdVersion || props.translateObj.updatedVersion">
            <el-tag
                size="small"
                effect="plain"
                class="versionTag"
                :title="props.translateObj.createdVersionRaw || ''"
                type="info"
            >
                {{ UI_TEXT.created }}: {{ formatVersion(props.translateObj.createdVersion, props.translateObj.createdVersionRaw) }}
            </el-tag>
            <el-tag
                v-if="showUpdatedVersionTag()"
                size="small"
                effect="plain"
                class="versionTag"
                :title="props.translateObj.updatedVersionRaw || ''"
                type="warning"
            >
                {{ UI_TEXT.updated }}: {{ formatVersion(props.translateObj.updatedVersion, props.translateObj.updatedVersionRaw) }}
            </el-tag>
        </div>

        <p class="info">
            <span class="origin" :class="{ talkOrigin: canOpenDetail() }" @click="gotoTalk">
                <span class="origin-label">{{ UI_TEXT.source }}:</span> <span class="origin-value">{{ props.translateObj.origin }}</span>
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
    transition: all 0.3s ease;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 12px;
    background-color: #f9f9f9;
}

.entry:hover {
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    transform: translateY(-2px);
}

.entry-with-voice {
    border-left: 4px solid #409eff;
}

.info {
    font-size: 14px;
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
}

.language-label {
    font-weight: 500;
    color: #606266;
}

.voice-buttons {
    display: inline-flex;
    gap: 4px;
}

.copyButton {
    margin-left: auto;
    vertical-align: middle;
    transition: all 0.3s ease;
}

.copyButton:hover {
    transform: scale(1.1);
}

.origin {
    color: #ab9d96;
    display: flex;
    align-items: center;
    gap: 4px;
}

.origin-label {
    font-size: 13px;
    color: #909399;
}

.origin-value {
    font-size: 13px;
    color: #606266;
}

.talkOrigin {
    cursor: pointer;
    transition: all 0.3s ease;
}

.talkOrigin:hover {
    opacity: 0.8;
}

.talkOrigin:hover .origin-value {
    color: #409eff;
}

.talkOrigin > .gotoIcon {
    transition: all 0.3s ease;
    margin-left: 0;
    font-size: 12px;
    color: #909399;
}

.talkOrigin:hover > .gotoIcon {
    padding-left: 5px;
    color: #409eff;
}

.versionTags {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin: 10px 0;
}

.versionTag {
    border-radius: 999px;
    font-size: 12px;
    padding: 2px 10px;
}

/* 响应式设计 */
@media (max-width: 720px) {
    .entry {
        padding: 12px;
        margin-bottom: 10px;
    }
    
    .info {
        flex-wrap: wrap;
        gap: 6px;
    }
    
    .copyButton {
        margin-left: 0;
    }
    
    .versionTags {
        margin: 8px 0;
    }
}
</style>
