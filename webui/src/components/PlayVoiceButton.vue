<script setup>
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
    size: {
        type: String,
        default: "default",
        validator: (value) => ["default", "small"].includes(value),
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
        <span
            ref="icon"
            class="voiceButtonWrapper"
            :class="{ isDisabled: disabled, isSmall: size === 'small' }"
            role="button"
            @click="playVoice"
        >
            <i class="fi fi-rr-play"></i>
        </span>
    </el-tooltip>
</template>

<style scoped>
.voiceButtonWrapper {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    border-radius: 50%;
    background: var(--voice-button-bg);
    border: 1px solid var(--voice-button-border);
    color: var(--voice-button-color);
    cursor: pointer;
    position: relative;
    overflow: hidden;
    box-shadow: var(--voice-button-shadow);
    transition: transform 0.15s ease, background-color 0.15s ease, border-color 0.15s ease, color 0.15s ease, box-shadow 0.15s ease;
}

.voiceButtonWrapper .fi {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    height: 100%;
    font-size: 13px;
    line-height: 1;
}

.voiceButtonWrapper::after {
    content: "";
    position: absolute;
    inset: 0;
    border-radius: 50%;
    background: var(--voice-button-ripple);
    transform: scale(0);
    transition: transform 0.4s ease;
}

.voiceButtonWrapper:active::after {
    transform: scale(2.5);
    transition-duration: 0s;
}

.voiceButtonWrapper:not(.isDisabled):hover {
    background: var(--voice-button-hover-bg);
    border-color: var(--voice-button-hover-border);
    color: var(--voice-button-hover-color);
    box-shadow: var(--voice-button-hover-shadow);
    transform: scale(1.08);
}

.voiceButtonWrapper.isSmall {
    width: 28px;
    height: 28px;
}

.voiceButtonWrapper.isSmall .fi {
    font-size: 11px;
}

.voiceButtonWrapper.isDisabled {
    opacity: 0.45;
    cursor: not-allowed;
    box-shadow: none;
}
</style>
