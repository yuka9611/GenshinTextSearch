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
}

.voiceButtonWrapper.isDisabled {
    opacity: 0.45;
}
</style>
