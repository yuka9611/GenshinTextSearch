<script setup>
import global from "@/global/global.js";
import PlayVoiceButton from "@/components/PlayVoiceButton.vue";
import StylizedText from "@/components/StylizedText.vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import { CopyDocument } from "@element-plus/icons-vue";

const UI_TEXT = Object.freeze({
    noTextToCopy: "没有可复制的文本",
    copied: "已复制",
    copyFailed: "复制失败",
    unknown: "未知",
    created: "创建",
    updated: "更新",
    source: "来源",
    copy: "复制",
    audioUnavailable: "当前语言暂无语音",
    audioMissing: "未找到语音文件",
});

const props = defineProps(["translateObj", "keyword", "searchLang"]);
const emit = defineEmits(["onVoicePlay"]);
const router = useRouter();

const onVoicePlay = (voiceUrl) => {
    emit("onVoicePlay", voiceUrl);
};

const canOpenDetail = () => {
    if (props.translateObj.disableDetail) return false;
    return Boolean(
        props.translateObj.isTalk ||
        props.translateObj.isSubtitle ||
        props.translateObj.viewAsTextHash ||
        props.translateObj.isReadable
    );
};

const hasVoicePaths = () => {
    return Boolean(props.translateObj.voicePaths && props.translateObj.voicePaths.length > 0);
};

const hasAvailableVoice = () => {
    return Boolean(props.translateObj.availableVoiceLangs && props.translateObj.availableVoiceLangs.length > 0);
};

const isVoiceAvailableForLang = (langCode) => {
    const voiceLangs = props.translateObj.availableVoiceLangs || [];
    return voiceLangs.includes(Number(langCode));
};

const shouldShowVoiceButton = (langCode) => {
    if (!hasVoicePaths()) return false;
    if (isVoiceAvailableForLang(langCode)) return true;
    return !hasAvailableVoice();
};

const normalizeCopyText = (text) => {
    if (!text) return "";
    const normalized = text.replace(/\\n/g, "\n");
    return normalized.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
};

const copyToClipboard = async (text) => {
    const sanitizedText = normalizeCopyText(text);
    if (!sanitizedText) {
        ElMessage.warning(UI_TEXT.noTextToCopy);
        return;
    }
    try {
        if (navigator.clipboard?.writeText) {
            await navigator.clipboard.writeText(sanitizedText);
            ElMessage.success(UI_TEXT.copied);
            return;
        }

        const textarea = document.createElement("textarea");
        textarea.value = sanitizedText;
        textarea.setAttribute("readonly", "");
        textarea.style.position = "absolute";
        textarea.style.left = "-9999px";
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand("copy");
        document.body.removeChild(textarea);
        ElMessage.success(UI_TEXT.copied);
    } catch (error) {
        console.error(error);
        ElMessage.error(UI_TEXT.copyFailed);
    }
};

const gotoTalk = () => {
    if (props.translateObj.isSubtitle) {
        const query = {
            fileName: props.translateObj.fileName,
            keyword: props.keyword,
            isSubtitle: 1,
            searchLang: props.searchLang,
        };
        if (props.translateObj.subtitleId) {
            query.subtitleId = props.translateObj.subtitleId;
        }
        router.push({ path: "/talk", query });
        return;
    }
    if (props.translateObj.isReadable) {
        const query = {
            readableId: props.translateObj.readableId,
            fileName: props.translateObj.fileName,
            keyword: props.keyword,
            searchLang: props.searchLang,
        };
        router.push({ path: "/talk", query });
        return;
    }
    if (!(props.translateObj.isTalk || props.translateObj.viewAsTextHash)) return;
    router.push({
        path: "/talk",
        query: {
            textHash: props.translateObj.hash,
            keyword: props.keyword,
            searchLang: props.searchLang,
        },
    });
};

const resolveVersionValue = (versionTag, rawVersion) => {
    if (versionTag) return String(versionTag).trim();
    if (rawVersion) return String(rawVersion).trim();
    return '';
};

const formatVersion = (versionTag, rawVersion) => {
    return resolveVersionValue(versionTag, rawVersion) || UI_TEXT.unknown;
};

const hasCreatedVersionTag = () => {
    return !!resolveVersionValue(props.translateObj.createdVersion, props.translateObj.createdVersionRaw);
};

const hasAnyVersionInfo = () => {
    return hasCreatedVersionTag() || !!resolveVersionValue(props.translateObj.updatedVersion, props.translateObj.updatedVersionRaw);
};

const showUpdatedVersionTag = () => {
    const updated = resolveVersionValue(props.translateObj.updatedVersion, props.translateObj.updatedVersionRaw);
    if (!updated) return false;
    const created = resolveVersionValue(props.translateObj.createdVersion, props.translateObj.createdVersionRaw);
    return created !== updated;
};
</script>

<template>
    <div class="entry" :class="{ 'entry-with-voice': hasVoicePaths() }">
        <div class="translate" v-for="(translate, translateKey) in props.translateObj.translates" :key="translateKey">
            <p class="info">
                <span class="language-label">{{ global.languages[translateKey] }}:</span>
                <span v-if="shouldShowVoiceButton(translateKey)" class="voice-buttons">
                    <PlayVoiceButton
                        v-for="voice in props.translateObj.voicePaths"
                        :key="`${voice}-${translateKey}`"
                        :voice-path="voice"
                        :lang-code="translateKey"
                        :disabled="!isVoiceAvailableForLang(translateKey)"
                        :disabled-tooltip="UI_TEXT.audioUnavailable"
                        :unavailable-message="UI_TEXT.audioMissing"
                        @on-voice-play="onVoicePlay"
                    />
                </span>
                <el-button
                    class="copyButton"
                    :icon="CopyDocument"
                    circle
                    size="small"
                    @click="copyToClipboard(translate)"
                    :title="UI_TEXT.copy"
                />
            </p>
            <StylizedText :text="translate" :keyword="$props.keyword" />
        </div>

        <div class="versionTags" v-if="hasAnyVersionInfo()">
            <el-tag
                v-if="hasCreatedVersionTag()"
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
    margin-bottom: 14px;
}

.translate:last-child {
    margin-bottom: 0;
}

.entry {
    position: relative;
    line-height: 30px;
    transition: all 0.3s ease;
    border-radius: 22px;
    padding: 20px 20px 18px;
    margin-bottom: 14px;
    background:
        linear-gradient(180deg, rgba(255, 253, 248, 0.98), rgba(249, 243, 232, 0.94));
    border: 1px solid rgba(190, 164, 124, 0.32);
    box-shadow: 0 12px 26px rgba(44, 57, 54, 0.07);
}

.entry:hover {
    box-shadow: 0 18px 32px rgba(44, 57, 54, 0.10);
    transform: translateY(-2px);
}

.entry-with-voice::before {
    content: "";
    position: absolute;
    left: 0;
    top: 18px;
    bottom: 18px;
    width: 4px;
    border-radius: 999px;
    background: linear-gradient(180deg, var(--theme-primary), var(--theme-accent));
}

.info {
    font-size: 14px;
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 8px;
    margin-bottom: 10px;
}

.language-label {
    display: inline-flex;
    align-items: center;
    min-height: 28px;
    padding: 0 10px;
    border-radius: 999px;
    font-weight: 700;
    color: var(--theme-ink);
    background: rgba(183, 140, 79, 0.12);
    font-family: var(--font-title);
}

.voice-buttons {
    display: inline-flex;
    gap: 6px;
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
    color: var(--theme-text-muted);
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 4px;
    padding: 10px 12px;
    border-radius: 14px;
    background: rgba(47, 105, 101, 0.06);
    border: 1px solid rgba(47, 105, 101, 0.1);
}

.origin-label {
    font-size: 13px;
    color: var(--theme-text-soft);
}

.origin-value {
    font-size: 13px;
    color: var(--theme-text);
}

.talkOrigin {
    cursor: pointer;
    transition: all 0.3s ease;
}

.talkOrigin:hover {
    border-color: rgba(47, 105, 101, 0.24);
    background: rgba(47, 105, 101, 0.10);
}

.talkOrigin:hover .origin-value {
    color: var(--theme-primary);
}

.talkOrigin > .gotoIcon {
    transition: all 0.3s ease;
    margin-left: 0;
    font-size: 12px;
    color: var(--theme-text-soft);
}

.talkOrigin:hover > .gotoIcon {
    padding-left: 5px;
    color: var(--theme-primary);
}

.versionTags {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin: 12px 0 10px;
}

.versionTag {
    font-size: 12px;
    padding: 2px 10px;
}

.translate :deep(p) {
    margin: 0;
}

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
