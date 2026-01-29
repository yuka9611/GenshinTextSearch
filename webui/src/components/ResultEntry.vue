<script setup>
// 显示多语言翻译的组件
import global from '@/global/global.js'
import PlayVoiceButton from "@/components/PlayVoiceButton.vue";
import StylizedText from "@/components/StylizedText.vue";
import {useRouter} from "vue-router";
import {ElMessage} from "element-plus";
import {CopyDocument} from "@element-plus/icons-vue";
/**
 *         {
 *             "type": "Dialogue",
 *             "origin": "TASK NAME etc.",
 *             "voicePaths": [],
 *             "translates":{
 *                 1: "TRANSLATE_CHINESE",
 *                 4: "TRANSLATE_ENGLISH"
 *             }
 *         },
 */
const props = defineProps(['translateObj', 'keyword', 'searchLang'])
const emit = defineEmits(['onVoicePlay'])
const router = useRouter()


const onVoicePlay = (voiceUrl) => {
    emit('onVoicePlay', voiceUrl)
}

const normalizeCopyText = (text) => {
    if (!text) return ""
    const normalized = text.replace(/\\n/g, "\n")
    return normalized.replace(/\r\n/g, "\n").replace(/\r/g, "\n")
}

const copyToClipboard = async (text) => {
    const sanitizedText = normalizeCopyText(text)
    if (!sanitizedText) {
        ElMessage.warning("没有可复制的文本")
        return
    }
    try {
        if (navigator.clipboard?.writeText) {
            await navigator.clipboard.writeText(sanitizedText)
            ElMessage.success("已复制")
            return
        }

        const textarea = document.createElement('textarea')
        textarea.value = sanitizedText
        textarea.setAttribute('readonly', '')
        textarea.style.position = 'absolute'
        textarea.style.left = '-9999px'
        document.body.appendChild(textarea)
        textarea.select()
        document.execCommand('copy')
        document.body.removeChild(textarea)
        ElMessage.success("已复制")
    } catch (error) {
        console.error(error)
        ElMessage.error("复制失败，请手动选择文本")
    }
}

const gotoTalk = () => {
    if (props.translateObj.isSubtitle) {
        let query = {
            fileName: props.translateObj.fileName,
            keyword: props.keyword,
            isSubtitle: 1,
            searchLang: props.searchLang
        }
        if (props.translateObj.subtitleId) {
            query.subtitleId = props.translateObj.subtitleId
        }
        router.push({path: '/talk', query: query})
        return
    }
    if(!props.translateObj.isTalk) return
    router.push(`/talk?textHash=${props.translateObj.hash}&keyword=${props.keyword}`)
}

</script>

<template>

    <div class="entry">

        <div class="translate" v-for="(translate, translateKey) in props.translateObj.translates">
            <p class="info">{{global.languages[translateKey]}}:
                <span v-if="global.voiceLanguages[translateKey]">
                    <PlayVoiceButton v-for="voice in props.translateObj.voicePaths"
                                     :voice-path="voice" :lang-code="translateKey"
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
            <StylizedText :text="translate" :keyword="$props.keyword"/>
        </div>
        <p class="info">
            <span class="origin" :class="{talkOrigin: props.translateObj.isTalk || props.translateObj.isSubtitle}" @click="gotoTalk">
                来源：{{props.translateObj.origin}}
                <span class="gotoIcon" v-if="props.translateObj.isTalk || props.translateObj.isSubtitle">&gt</span>
            </span>
        </p>
    </div>

</template>

<style scoped>
.translate{
    margin-bottom: 10px;
}

.entry{
    padding-bottom: 20px;
    padding-top: 20px;
    line-height: 30px;
}
.info{
    font-size: 14px;
}

.voice{
    margin-right: 10px;
}

.copyButton {
    margin-left: 8px;
    vertical-align: middle;
}

.origin{
    color: #ab9d96;
}

.talkOrigin {
    cursor: pointer;
    transition: 0.3s;
}

.talkOrigin:hover {
    opacity: 0.8;
}

.talkOrigin>.gotoIcon {
    transition: 0.3s;
    margin-left: 0;
}
.talkOrigin:hover>.gotoIcon {
    padding-left: 5px;
}
</style>
