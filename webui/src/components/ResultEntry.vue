<script setup>
import global from "@/global/global.js";
import PlayVoiceButton from "@/components/PlayVoiceButton.vue";
import StylizedText from "@/components/StylizedText.vue";
import { computed } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";

const UI_TEXT = Object.freeze({
    noTextToCopy: "没有可复制的文本",
    copied: "已复制",
    copyFailed: "复制失败",
    unknown: "未知",
    created: "✦ 创建",
    updated: "↻ 更新",
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

const showColorStrip = computed(() => showSourcePanel.value || hasVoicePaths());

const showUpdatedVersionTag = () => {
    const updated = resolveVersionValue(props.translateObj.updatedVersion, props.translateObj.updatedVersionRaw);
    if (!updated) return false;
    const created = resolveVersionValue(props.translateObj.createdVersion, props.translateObj.createdVersionRaw);
    return created !== updated;
};
</script>

<template>
    <div class="entry" :class="{ 'entry-with-strip': showColorStrip }" :data-source-type="primarySource?.sourceType || undefined">
        <div v-if="showSourcePanel" class="sourcePanel">
            <span class="sourceType">{{ sourceTypeLabel }}</span>
            <div class="sourceText">
                <StylizedText :text="sourceTitle" :keyword="$props.keyword" class="sourceTitle" />
                <StylizedText v-if="sourceSubtitle" :text="sourceSubtitle" :keyword="$props.keyword" class="sourceSubtitle" />
            </div>
            <div class="sourceActions">
                <button v-if="canOpenDetail()" class="source-detail-btn" @click="openSourceDetail">
                    <span class="sourceDetailArrow" aria-hidden="true">→</span>
                    {{ UI_TEXT.sourceDetail }}
                </button>
                <span v-if="showSourceCount" class="sourceCount">{{ sourceCount }} 个来源</span>
            </div>
        </div>

        <div class="translate" v-for="(translate, translateKey) in props.translateObj.translates" :key="translateKey">
            <div class="info">
                <span class="language-label">{{ global.languages[translateKey] }}:</span>
                <div class="translateActions">
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
                    <button type="button" class="copyButton" @click="copyToClipboard(translate)" :title="UI_TEXT.copy">
                        <i class="fi fi-rr-copy"></i>
                    </button>
                </div>
            </div>
            <StylizedText :text="translate" :keyword="$props.keyword" />
        </div>

        <div class="versionTags" v-if="hasAnyVersionInfo()">
            <span
                v-if="hasCreatedVersionTag()"
                class="versionTag created"
                :title="props.translateObj.createdVersionRaw || ''"
            >
                {{ UI_TEXT.created }} {{ formatVersion(props.translateObj.createdVersion, props.translateObj.createdVersionRaw) }}
            </span>
            <span
                v-if="showUpdatedVersionTag()"
                class="versionTag updated"
                :title="props.translateObj.updatedVersionRaw || ''"
            >
                {{ UI_TEXT.updated }} {{ formatVersion(props.translateObj.updatedVersion, props.translateObj.updatedVersionRaw) }}
            </span>
        </div>
    </div>
</template>

<style scoped>
.translate {
    padding: 12px 0 12px 8px;
}

.translate + .translate {
    border-top: 1px solid rgba(190, 164, 124, 0.18);
}


.translate:last-child {
    border-bottom: none;
}

.entry {
    position: relative;
    overflow: hidden;
    line-height: 1.6;
    transition: transform 0.25s ease, box-shadow 0.25s ease, background 0.25s ease, border-color 0.25s ease;
    border-radius: 22px;
    padding: 20px 20px 18px;
    margin-bottom: 14px;
    background:
        linear-gradient(180deg, rgba(255, 253, 248, 0.98), rgba(249, 243, 232, 0.94));
    border: 1px solid rgba(190, 164, 124, 0.32);
    box-shadow: 0 12px 26px rgba(44, 57, 54, 0.07);
}


.entry:hover {
    box-shadow: 0 22px 42px rgba(44, 57, 54, 0.16);
    transform: translateY(-3px);
    border-color: rgba(var(--theme-primary-rgb), 0.3);
}


/* left color strip — only shown when entry has source panel or voice */
.entry-with-strip::before {
    content: "";
    position: absolute;
    left: 0;
    top: 18px;
    bottom: 18px;
    width: 4px;
    border-radius: 0 4px 4px 0;
    background: linear-gradient(180deg, var(--theme-primary), var(--theme-accent));
}

.entry-with-strip[data-source-type="dialogue"]::before,
.entry-with-strip[data-source-type="voice"]::before { background: var(--theme-primary); }
.entry-with-strip[data-source-type="quest"]::before { background: #4a7ab5; }
.entry-with-strip[data-source-type="readable"]::before { background: var(--theme-accent); }
.entry-with-strip[data-source-type="subtitle"]::before { background: #5c7f58; }
.entry-with-strip[data-source-type="weapon"]::before,
.entry-with-strip[data-source-type="reliquary"]::before { background: #7a5cb5; }
.entry-with-strip[data-source-type="item"]::before,
.entry-with-strip[data-source-type="material"]::before { background: var(--theme-accent); }

.info {
    font-size: 14px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    margin-bottom: 10px;
}

.language-label {
    display: inline-flex;
    align-items: center;
    min-height: 28px;
    padding: 0 11px;
    border-radius: 999px;
    font-weight: 600;
    font-size: 12px;
    line-height: 1.2;
    color: var(--theme-text-muted);
    background: rgba(183, 140, 79, 0.12);
}

.translateActions {
    display: flex;
    align-items: center;
    gap: 8px;
}

.voice-buttons {
    display: inline-flex;
    gap: 6px;
}

.copyButton {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    padding: 0;
    border-radius: 50%;
    border: 1px solid rgba(190, 164, 124, 0.32);
    background: rgba(255, 253, 248, 0.94);
    color: var(--theme-text-muted);
    font-size: 13px;
    cursor: pointer;
    transition: all 0.18s ease;
    line-height: 1;
}

.copyButton i {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 14px;
    height: 14px;
    line-height: 1;
}

.copyButton:hover {
    color: var(--theme-accent);
    border-color: var(--theme-accent);
    transform: scale(1.08);
}

.sourcePanel {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    margin: -20px -20px 16px;
    padding: 12px 14px 12px 28px;
    background: rgba(47, 105, 101, 0.05);
    border-bottom: 1px solid rgba(190, 164, 124, 0.18);
}


.sourceText {
    flex: 1;
    min-width: 0;
}

.sourceType {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 28px;
    padding: 0 11px;
    border-radius: 999px;
    background: rgba(183, 140, 79, 0.12);
    color: var(--theme-accent);
    font-size: 12px;
    font-weight: 600;
    line-height: 1.2;
    white-space: nowrap;
    flex-shrink: 0;
    align-self: flex-start;
}

.entry[data-source-type="dialogue"] .sourceType,
.entry[data-source-type="voice"] .sourceType {
    background: rgba(47, 105, 101, 0.12);
    color: var(--theme-primary);
}
.entry[data-source-type="quest"] .sourceType {
    background: rgba(74, 122, 181, 0.12);
    color: #4a7ab5;
}
.entry[data-source-type="weapon"] .sourceType,
.entry[data-source-type="reliquary"] .sourceType {
    background: rgba(122, 92, 181, 0.12);
    color: #7a5cb5;
}
.entry[data-source-type="subtitle"] .sourceType {
    background: rgba(92, 127, 88, 0.12);
    color: #5c7f58;
}


.sourceTitle {
    font-size: 13px;
    font-weight: 600;
    color: var(--theme-text);
    line-height: 1.4;
}

.sourceTitle:deep(p),
.sourceSubtitle:deep(p) {
    margin: 0;
}

.sourceSubtitle {
    margin-top: 4px;
    color: var(--theme-text-muted);
    font-size: 13px;
}

.sourceActions {
    display: inline-flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
    flex-shrink: 0;
}

.source-detail-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    min-height: 28px;
    padding: 0 12px;
    border-radius: 999px;
    border: 1px solid var(--theme-border);
    background: transparent;
    color: var(--theme-text-muted);
    font-size: 0.75rem;
    cursor: pointer;
    transition: all 0.18s ease;
    white-space: nowrap;
}

.source-detail-btn:hover {
    border-color: var(--theme-primary);
    color: var(--theme-primary);
    background: var(--theme-primary-soft);
}

.sourceDetailArrow {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 12px;
    line-height: 1;
}

.sourceCount {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 28px;
    padding: 0 11px;
    border-radius: 999px;
    font-size: 12px;
    line-height: 1.2;
    color: var(--theme-text-muted);
    background: rgba(47, 105, 101, 0.06);
}

.versionTags {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin: 14px 0 4px 8px;
}

.versionTag {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 28px;
    padding: 0 11px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 500;
    line-height: 1.2;
    border: 1px solid var(--theme-border);
    color: var(--theme-text-muted);
    background: transparent;
}

.versionTag.created {
    border-color: rgba(92, 127, 88, 0.35);
    color: #5c7f58;
}

.versionTag.updated {
    border-color: rgba(74, 122, 181, 0.35);
    color: #4a7ab5;
}


.translate :deep(p) {
    margin: 0;
}

@media (max-width: 680px) {
    .entry {
        padding: 14px 14px 12px;
        margin-bottom: 10px;
    }

    .sourcePanel {
        margin: -14px -14px 12px;
        padding: 10px 12px 10px 22px;
    }

    .info {
        flex-wrap: wrap;
        gap: 6px;
    }

    .versionTags {
        margin: 10px 0 2px 8px;
    }
}
</style>

<style>
/* Dark-mode overrides — unscoped to avoid Vue scoped CSS :global() compilation bug */
[data-theme="dark"] .translate + .translate {
    border-top-color: var(--theme-border);
}
[data-theme="dark"] .entry {
    background: linear-gradient(180deg, rgba(30, 40, 37, 0.98), rgba(24, 34, 31, 0.94));
    border-color: var(--theme-border);
    box-shadow: 0 12px 26px rgba(0, 0, 0, 0.14);
}
[data-theme="dark"] .entry:hover {
    box-shadow: 0 22px 42px rgba(0, 0, 0, 0.3);
    background: rgba(42, 56, 52, 0.96);
}
[data-theme="dark"] .language-label {
    background: rgba(212, 168, 98, 0.12);
}
[data-theme="dark"] .sourcePanel {
    background: rgba(74, 154, 149, 0.07);
    border-bottom-color: rgba(74, 154, 149, 0.14);
}
[data-theme="dark"] .sourceCount {
    background: rgba(74, 154, 149, 0.08);
}
[data-theme="dark"] .sourceType {
    background: rgba(183, 140, 79, 0.18);
}
[data-theme="dark"] .entry[data-source-type="dialogue"] .sourceType,
[data-theme="dark"] .entry[data-source-type="voice"] .sourceType {
    background: rgba(74, 154, 149, 0.18);
}
[data-theme="dark"] .entry[data-source-type="quest"] .sourceType {
    background: rgba(74, 122, 181, 0.18);
    color: #6a9fd4;
}
[data-theme="dark"] .entry[data-source-type="weapon"] .sourceType,
[data-theme="dark"] .entry[data-source-type="reliquary"] .sourceType {
    background: rgba(122, 92, 181, 0.18);
    color: #9a7ed4;
}
[data-theme="dark"] .entry[data-source-type="subtitle"] .sourceType {
    background: rgba(92, 127, 88, 0.18);
    color: #7aa075;
}
[data-theme="dark"] .versionTag.created {
    color: var(--theme-success);
}
[data-theme="dark"] .versionTag.updated {
    color: #6aa0d8;
    border-color: rgba(74, 122, 181, 0.35);
}
</style>
