<script setup>
import { VideoPlay } from "@element-plus/icons-vue";
import { ElMessage } from "element-plus";
import api from "@/api/keywordQuery";
import * as converter from "@/assets/wem2wav";
import { computed, ref, watch } from "vue";

const props = defineProps({
    voicePath: {
        type: String,
        required: true,
    },
    langCode: {
        type: [String, Number],
        required: true,
    },
    disabled: {
        type: Boolean,
        default: false,
    },
    disabledTooltip: {
        type: String,
        default: "当前语言暂无语音",
    },
    unavailableMessage: {
        type: String,
        default: "未找到语音文件",
    },
});
const emit = defineEmits(["onVoicePlay"]);

let audioUrl = undefined;

const icon = ref();
const tooltipText = computed(() => (props.disabled ? props.disabledTooltip : props.voicePath));

const notifyUnavailable = (message, showError) => {
    if (!showError) return;
    ElMessage.warning(message || props.unavailableMessage);
};

const getAudioUrl = async (showError = true) => {
    if (props.disabled) {
        notifyUnavailable(props.disabledTooltip, showError);
        return null;
    }

    if (audioUrl !== undefined) {
        return audioUrl;
    }

    const buffer = await api.getVoiceOver(props.voicePath, props.langCode);
    if (!buffer) {
        audioUrl = null;
        notifyUnavailable(props.unavailableMessage, showError);
        return null;
    }

    try {
        audioUrl = await converter.convertBufferedArray(buffer);
    } catch (error) {
        console.error(error);
        audioUrl = null;
        notifyUnavailable(props.unavailableMessage, showError);
        return null;
    }

    return audioUrl;
};

const scrollTo = () => {
    icon.value?.scrollIntoView({ behavior: "smooth", block: "center" });
};

const playVoice = async () => {
    const url = await getAudioUrl();
    if (!url) return;
    emit("onVoicePlay", url);
};

watch(() => [props.voicePath, props.langCode, props.disabled], () => {
    audioUrl = undefined;
});

defineExpose({ getAudioUrl, scrollTo });
</script>

<template>
    <el-tooltip :content="tooltipText">
        <span ref="icon" class="voiceButtonWrapper" :class="{ isDisabled: disabled }">
            <el-icon @click="playVoice"><VideoPlay /></el-icon>
        </span>
    </el-tooltip>
</template>

<style scoped>
.voiceButtonWrapper {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    border-radius: 999px;
    background: rgba(47, 105, 101, 0.08);
    border: 1px solid rgba(47, 105, 101, 0.16);
    color: var(--theme-primary);
    transition: transform 0.18s ease, background-color 0.18s ease, border-color 0.18s ease, color 0.18s ease;
}

.voiceButtonWrapper:not(.isDisabled):hover {
    transform: translateY(-1px);
    background: rgba(47, 105, 101, 0.14);
    border-color: rgba(47, 105, 101, 0.24);
}

.voiceButtonWrapper :deep(.el-icon) {
    font-size: 14px;
    cursor: pointer;
}

.voiceButtonWrapper.isDisabled {
    opacity: 0.5;
    color: var(--theme-text-soft);
    background: rgba(233, 225, 210, 0.8);
    border-color: rgba(190, 164, 124, 0.22);
}
</style>
