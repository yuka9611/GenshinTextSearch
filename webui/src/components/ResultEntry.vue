<script setup>
import global from "@/global/global.js";
import PlayVoiceButton from "@/components/PlayVoiceButton.vue";
import StylizedText from "@/components/StylizedText.vue";
import { computed } from "vue";
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
    sourceDetail: "来源详情",
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

const primarySource = computed(() => props.translateObj?.primarySource || null);
const sourceCount = computed(() => {
    const raw = props.translateObj?.sourceCount;
    const value = Number(raw);
    return Number.isFinite(value) ? value : 0;
});

const sourceTypeLabel = computed(() => {
    const sourceType = String(primarySource.value?.sourceType || "").trim();
    const map = {
        dialogue: "对话",
        voice: "角色语音",
        quest: "任务",
        readable: "阅读物",
        subtitle: "字幕",
        item: "道具",
        material: "材料",
        food: "食物",
        blueprint: "图纸",
        gcg: "七圣召唤",
        namecard: "名片",
        performance: "表演诀窍",
        avatar_intro: "角色",
        dressing: "装扮",
        music_theme: "演奏主题",
        avatar_mat: "角色突破素材",
        other_mat: "其他",
        weapon: "武器",
        reliquary: "圣遗物",
        furnishing: "摆设",
        gadget: "小道具",
        monster: "怪物",
        creature: "生物",
        costume: "千星奇域",
        suit: "千星奇域",
        achievement: "成就",
        viewpoint: "观景点",
        dungeon: "秘境",
        loading_tip: "过场提示",
        unknown: "未归类",
    };
    return map[sourceType] || UI_TEXT.source;
});

const sourceTitle = computed(() => {
    const title = primarySource.value?.title;
    if (title !== undefined && title !== null && String(title).trim() !== "") {
        return String(title);
    }
    const origin = props.translateObj?.origin;
    if (origin !== undefined && origin !== null && String(origin).trim() !== "") {
        return String(origin);
    }
    return UI_TEXT.unknown;
});

const sourceSubtitle = computed(() => {
    const subtitle = primarySource.value?.subtitle;
    if (subtitle === undefined || subtitle === null) return "";
    return String(subtitle).trim();
});

const showSourceCount = computed(() => sourceCount.value > 1);
const showSourcePanel = computed(() => {
    const sourceType = String(primarySource.value?.sourceType || "").trim();
    return Boolean(primarySource.value) && sourceType !== "unknown";
});

const openSourceDetail = () => {
    const detail = primarySource.value?.detailQuery;
    if (!detail || typeof detail !== "object") {
        gotoTalk();
        return;
    }
    const kind = String(detail.kind || "").trim();
    if (kind === "readable") {
        router.push({
            path: "/talk",
            query: {
                readableId: detail.readableId ?? props.translateObj.readableId,
                fileName: detail.fileName ?? props.translateObj.fileName,
                keyword: props.keyword,
                searchLang: props.searchLang,
            },
        });
        return;
    }
    if (kind === "subtitle") {
        const query = {
            fileName: detail.fileName ?? props.translateObj.fileName,
            keyword: props.keyword,
            isSubtitle: 1,
            searchLang: props.searchLang,
        };
        if (detail.subtitleId ?? props.translateObj.subtitleId) {
            query.subtitleId = detail.subtitleId ?? props.translateObj.subtitleId;
        }
        router.push({ path: "/talk", query });
        return;
    }
    if (kind === "quest") {
        router.push({
            path: "/talk",
            query: {
                questId: detail.questId,
                keyword: props.keyword,
                searchLang: props.searchLang,
            },
        });
        return;
    }
    if (kind === "entity") {
        router.push({
            path: "/entity",
            query: {
                sourceTypeCode: detail.sourceTypeCode,
                entityId: detail.entityId,
                keyword: props.keyword,
                searchLang: props.searchLang,
            },
        });
        return;
    }
    if (kind === "talk" || kind === "text") {
        router.push({
            path: "/talk",
            query: {
                textHash: detail.textHash ?? props.translateObj.hash,
                keyword: props.keyword,
                searchLang: props.searchLang,
            },
        });
        return;
    }
    gotoTalk();
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
        <div v-if="showSourcePanel" class="sourcePanel">
            <div class="sourceText">
                <span class="sourceType">{{ sourceTypeLabel }}</span>
                <StylizedText :text="sourceTitle" :keyword="$props.keyword" class="sourceTitle" />
                <StylizedText v-if="sourceSubtitle" :text="sourceSubtitle" :keyword="$props.keyword" class="sourceSubtitle" />
            </div>
            <div class="sourceActions">
                <el-button v-if="canOpenDetail()" size="small" @click="openSourceDetail">{{ UI_TEXT.sourceDetail }}</el-button>
                <span v-if="showSourceCount" class="sourceCount">{{ sourceCount }} 个来源</span>
            </div>
        </div>

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

.sourcePanel {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
    padding: 14px 14px 12px;
    border-radius: 18px;
    background: rgba(47, 105, 101, 0.06);
    border: 1px solid rgba(47, 105, 101, 0.10);
    margin-bottom: 16px;
}

.sourceText {
    min-width: 0;
}

.sourceType {
    display: inline-flex;
    align-items: center;
    padding: 2px 10px;
    border-radius: 999px;
    background: rgba(183, 140, 79, 0.12);
    color: var(--theme-ink);
    font-size: 12px;
    font-family: var(--font-title);
    font-weight: 700;
    margin-bottom: 8px;
}

.sourceTitle {
    font-size: 18px;
    font-weight: 700;
    color: var(--theme-text);
}

.sourceTitle:deep(p),
.sourceSubtitle:deep(p) {
    margin: 0;
}

.sourceSubtitle {
    margin-top: 6px;
    color: var(--theme-text-muted);
    font-size: 13px;
}

.sourceActions {
    display: inline-flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
}

.sourceCount {
    padding: 4px 10px;
    border-radius: 999px;
    font-size: 12px;
    color: var(--theme-text-muted);
    background: rgba(183, 140, 79, 0.10);
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

    .sourcePanel {
        padding: 12px 12px 10px;
        margin-bottom: 12px;
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
