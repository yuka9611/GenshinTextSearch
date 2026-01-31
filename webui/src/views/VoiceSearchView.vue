<script setup>
import { onBeforeMount, ref, computed, reactive, watch } from 'vue';
import { Search, Close } from '@element-plus/icons-vue';
import global from "@/global/global";
import api from "@/api/keywordQuery";
import TranslateDisplay from "@/components/ResultEntry.vue";
import AudioPlayer from "@liripeng/vue-audio-player";
import * as converter from "@/assets/wem2wav";

const keyword = ref("")
const selectedInputLanguage = ref(global.config.defaultSearchLanguage + '')
const supportedInputLanguage = ref({})
const searchSummary = ref("")
const voiceSummary = ref("")

const avatarResults = ref([])
const voiceEntries = ref([])
const selectedAvatar = ref(null)
const loadingVoices = ref(false)

const categoryFilter = ref("all")
const textFilter = ref("")
const selectedVoiceLanguage = ref("")

const showPlayer = ref(false)
const firstShowPlayer = ref(true)
const audio = ref([])
const voicePlayer = ref()

const playlistLoading = reactive({
    show: false,
    total: 0,
    current: 0,
    percentage: 0
})

onBeforeMount(async () => {
    supportedInputLanguage.value = global.languages
})

const availableVoiceLanguages = computed(() => global.voiceLanguages || {})
const voiceLanguageOptions = computed(() => {
    return Object.keys(availableVoiceLanguages.value).map((key) => ({
        id: key,
        name: availableVoiceLanguages.value[key]
    }))
})

watch(voiceLanguageOptions, (options) => {
    if (!selectedVoiceLanguage.value && options.length > 0) {
        selectedVoiceLanguage.value = options[0].id
    }
}, { immediate: true })

const normalizeText = (value) => {
    if (!value) return ""
    return String(value).trim().toLowerCase()
}

const deriveCategory = (title) => {
    const raw = (title || "").trim()
    if (!raw) return "未分类"
    const separators = ["·", "・", "：", ":", "-", "—"]
    for (const sep of separators) {
        if (raw.includes(sep)) {
            const head = raw.split(sep)[0].trim()
            return head || raw
        }
    }
    return raw
}

const categories = computed(() => {
    const set = new Set()
    for (const entry of voiceEntries.value) {
        const title = entry.voiceTitle || ""
        set.add(deriveCategory(title))
    }
    return ["all", ...Array.from(set)]
})

const filteredVoices = computed(() => {
    const category = categoryFilter.value
    const text = normalizeText(textFilter.value)
    return voiceEntries.value.filter((entry) => {
        if (category !== "all") {
            const title = entry.voiceTitle || ""
            const entryCategory = deriveCategory(title)
            if (entryCategory !== category) return false
        }
        if (text) {
            const title = normalizeText(entry.voiceTitle || "")
            if (title.includes(text)) return true
            const translates = entry.translates || {}
            for (const key of Object.keys(translates)) {
                const content = normalizeText(translates[key] || "")
                if (content.includes(text)) return true
            }
            return false
        }
        return true
    })
})

const onSearchClicked = async () => {
    if (!keyword.value.trim()) {
        searchSummary.value = "请输入角色名进行查询。"
        voiceSummary.value = ""
        avatarResults.value = []
        voiceEntries.value = []
        selectedAvatar.value = null
        return
    }

    const ans = (await api.searchAvatar(keyword.value, selectedInputLanguage.value)).json
    const contents = ans.contents
    avatarResults.value = contents.avatars || []
    const avatarCount = avatarResults.value.length
    searchSummary.value = `查询用时: ${ans.time.toFixed(2)}ms，共 ${avatarCount} 个角色结果。`
    voiceEntries.value = []
    voiceSummary.value = ""
    selectedAvatar.value = null
}

const onAvatarClicked = async (avatar) => {
    selectedAvatar.value = avatar
    voiceEntries.value = []
    voiceSummary.value = ""
    loadingVoices.value = true
    categoryFilter.value = "all"
    textFilter.value = ""

    const ans = (await api.getAvatarVoices(avatar.avatarId, selectedInputLanguage.value)).json
    const contents = ans.contents
    voiceEntries.value = contents.voices || []
    const voiceCount = voiceEntries.value.length
    voiceSummary.value = `查询用时: ${ans.time.toFixed(2)}ms，共 ${voiceCount} 条语音结果。`
    loadingVoices.value = false
}

const onVoicePlay = (voiceUrl) => {
    if (!voiceUrl) return
    if (firstShowPlayer.value) {
        showPlayer.value = true
        firstShowPlayer.value = false
    }
    if (audio.value.length > 0 && voiceUrl === audio.value[0]) {
        if (voicePlayer.value.isPlaying) {
            voicePlayer.value.pause()
        } else {
            voicePlayer.value.play()
        }
        return
    }
    audio.value = [voiceUrl]
    voicePlayer.value.currentPlayIndex = 0
    setTimeout(() => {
        voicePlayer.value.play()
    }, 100)
}

const getAudioUrlForEntry = async (entry) => {
    const voicePath = entry.voicePaths?.[0]
    const langCode = selectedVoiceLanguage.value
    if (!voicePath || !langCode) return null
    try {
        const buffer = await api.getVoiceOver(voicePath, langCode)
        return await converter.convertBufferedArray(buffer)
    } catch (error) {
        console.error(error)
        return null
    }
}

const loadPlaylist = async () => {
    const items = filteredVoices.value
    playlistLoading.total = items.length
    playlistLoading.current = 0
    playlistLoading.percentage = 0
    playlistLoading.show = true

    const urls = []
    for (const entry of items) {
        const url = await getAudioUrlForEntry(entry)
        if (url) {
            urls.push(url)
        }
        playlistLoading.current += 1
        playlistLoading.percentage = playlistLoading.total
            ? Math.round(100 * playlistLoading.current / playlistLoading.total)
            : 0
    }

    playlistLoading.show = false
    if (urls.length === 0) return

    if (firstShowPlayer.value) {
        showPlayer.value = true
        firstShowPlayer.value = false
    }
    audio.value = urls
    voicePlayer.value.currentPlayIndex = 0
    setTimeout(() => {
        voicePlayer.value.play()
    }, 100)
}

const clearPlaylist = () => {
    audio.value = []
}

const onHidePlayerButtonClicked = () => {
    showPlayer.value = false
}

const onShowPlayerButtonClicked = () => {
    showPlayer.value = true
}
</script>

<template>
    <div class="viewWrapper">
        <h1 class="pageTitle">角色语音查询</h1>
        <div class="helpText">
            <p>输入角色名（支持模糊搜索），点击搜索后选择角色查看语音。</p>
            <p>支持语音分类、筛选与播放列表。</p>
        </div>

        <div class="searchBar">
            <el-input
                v-model="keyword"
                style="max-width: 600px;"
                placeholder="请输入角色名"
                class="input-with-select"
                @keyup.enter.native="onSearchClicked"
                clearable
            >
                <template #prepend>
                    <el-select v-model="selectedInputLanguage" placeholder="Select" class="languageSelector">
                        <el-option v-for="(v, k) in supportedInputLanguage" :label="v" :value="k" :key="k" />
                    </el-select>
                </template>
                <template #append>
                    <el-button :icon="Search" @click="onSearchClicked" />
                </template>
            </el-input>
            <span class="searchSummary">
                {{ searchSummary }}
            </span>
        </div>

        <div class="searchSpacer"></div>

        <div class="resultSection">
            <h2>角色列表</h2>
            <el-empty v-if="avatarResults.length === 0" description="暂无角色结果" />
            <div v-else class="resultGrid">
                <el-card v-for="avatar in avatarResults" :key="avatar.avatarId" class="resultCard">
                    <div class="cardTitle">{{ avatar.name }}</div>
                    <div class="cardMeta">角色 ID: {{ avatar.avatarId }}</div>
                    <el-button size="small" type="primary" @click="onAvatarClicked(avatar)">
                        查看语音
                    </el-button>
                </el-card>
            </div>
        </div>

        <div class="resultSection">
            <h2 v-if="selectedAvatar">语音结果 - {{ selectedAvatar.name }}</h2>
            <div class="voiceSummary" v-if="voiceSummary">{{ voiceSummary }}</div>

            <div class="filterBar" v-if="selectedAvatar">
                <el-select v-model="categoryFilter" placeholder="分类" class="filterSelect">
                    <el-option
                        v-for="item in categories"
                        :key="item"
                        :label="item === 'all' ? '全部' : item"
                        :value="item"
                    />
                </el-select>
                <el-input v-model="textFilter" placeholder="筛选关键词（标题/内容）" class="filterInput" clearable />
                <el-select v-model="selectedVoiceLanguage" placeholder="语音语言" class="filterSelect">
                    <el-option
                        v-for="item in voiceLanguageOptions"
                        :key="item.id"
                        :label="item.name"
                        :value="item.id"
                    />
                </el-select>
                <el-button size="small" type="primary" @click="loadPlaylist">播放筛选结果</el-button>
                <el-button size="small" @click="clearPlaylist">清空播放器</el-button>
            </div>

            <el-empty v-if="!loadingVoices && filteredVoices.length === 0" description="暂无语音结果" />
            <div v-else>
                <TranslateDisplay
                    v-for="voice in filteredVoices"
                    :key="voice.hash"
                    :translate-obj="voice"
                    :keyword="''"
                    :search-lang="selectedInputLanguage"
                    @onVoicePlay="onVoicePlay"
                    class="translate"
                />
            </div>
        </div>
    </div>

    <div class="viewWrapper voicePlayerContainer" v-show="showPlayer" v-if="audio.length > 0">
        <span class="hideIcon" @click="onHidePlayerButtonClicked">
            <el-icon>
                <Close />
            </el-icon>
        </span>

        <AudioPlayer
            ref="voicePlayer"
            :audio-list="audio"
            :show-prev-button="true"
            :show-next-button="true"
            :is-loop="false"
            :progress-interval="25"
            theme-color="var(--el-color-primary)">
        </AudioPlayer>
    </div>

    <div class="showPlayerButton" @click="onShowPlayerButtonClicked" v-show="!showPlayer && audio.length > 0">
        <i class="fi fi-sr-waveform-path"></i>
    </div>

    <el-dialog v-model="playlistLoading.show" :width="300" title="加载语音" align-center>
        <el-progress :percentage="playlistLoading.percentage">
            {{ playlistLoading.current }} / {{ playlistLoading.total }}
        </el-progress>
    </el-dialog>
</template>

<style scoped>
.viewWrapper {
    position: relative;
    width: var(--page-width);
    margin: 0 auto;
    background-color: #fff;
    box-shadow: var(--page-shadow);
    border-radius: var(--page-radius);
    padding: var(--page-padding);
    overflow: visible;
}

.pageTitle {
    border-bottom: 1px #ccc solid;
    padding-bottom: 10px;
}

.helpText {
    margin: 20px 0;
    color: #999;
}

.searchBar {
    position: sticky;
    top: 0;
    z-index: 3;
    background-color: #fff;
    padding-bottom: 8px;
    box-sizing: border-box;
}

.languageSelector {
    width: 120px;
}

.languageSelector:deep(input) {
    text-align: center;
}

.searchSummary {
    margin-left: 10px;
    color: var(--el-input-text-color, var(--el-text-color-regular));
    font-size: 14px;
}

.searchSpacer {
    display: none;
}

.resultSection {
    margin-top: 20px;
}

.resultSection h2 {
    margin-bottom: 12px;
}

.resultGrid {
    display: grid;
    gap: 12px;
    grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
}

.resultCard {
    display: flex;
    flex-direction: column;
    gap: 8px;
}

.cardTitle {
    font-weight: 600;
}

.cardMeta {
    color: #888;
    font-size: 13px;
}

.voiceSummary {
    margin: 8px 0 12px;
    color: #666;
    font-size: 13px;
}

.filterBar {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
    justify-content: flex-start;
    margin-bottom: 12px;
}

.filterSelect {
    min-width: 140px;
}

.filterInput {
    flex: 1 1 240px;
    max-width: 320px;
}

.translate:not(:last-child) {
    border-bottom: 1px solid #ccc;
}

.voicePlayerContainer {
    margin-top: 10px;
    bottom: 0;
    position: sticky !important;
    box-shadow: 0 0 5px 5px rgba(36, 37, 38, .05);
    z-index: 3;
    background-color: #fff;
}

.showPlayerButton {
    position: absolute;
    right: 7.5%;
    bottom: 80px;
    height: 70px;
    width: 70px;
    border-radius: 50%;
    background-color: var(--el-color-primary);
    color: #fff;
    font-size: 25px;
    box-shadow: 0 6px 15px rgba(36, 37, 38, .2);
    text-align: center;
    line-height: 75px;
    cursor: pointer;
    z-index: 3;
}

.showPlayerButton:hover {
    background-color: var(--el-color-primary-light-3);
}

.hideIcon {
    cursor: pointer;
    position: absolute;
    top: 10px;
    right: 10px;
}

.hideIcon:hover {
    color: #888;
}

@media (max-width: 720px) {
    .searchSummary {
        display: block;
        margin-left: 0;
        margin-top: 8px;
    }

    .searchSpacer {
        display: none;
        height: 0;
    }

    .voicePlayerContainer {
        position: fixed !important;
        left: 8px;
        right: 8px;
        bottom: 0;
        width: auto;
        margin-top: 0;
        border-radius: 0;
        z-index: 3;
        box-sizing: border-box;
        box-shadow: none;
    }

    .showPlayerButton {
        position: fixed;
        right: 16px;
        bottom: 24px;
        width: 56px;
        height: 56px;
        line-height: 60px;
        z-index: 3;
        box-shadow: none;
    }
}
</style>
