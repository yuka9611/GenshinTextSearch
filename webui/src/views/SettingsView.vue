<template>
  <div class="viewWrapper">
    <h1 class="pageTitle">设置</h1>

    <div class="helpText">
      <p>所有已经导入到数据库的文本语言会在此处显示。要导入新的语言，请关闭服务器，并使用导入工具。</p>
      <p>所有游戏已下载的语言包会在此处显示。请进入游戏来管理语音包。</p>
      <p>现在可以在这里直接选择游戏资源路径。</p>
    </div>

    <el-form :label-width="120" label-position="left">
      <el-form-item label="默认搜索语言">
        <el-select v-model="selectedInputLanguage" placeholder="Select" class="languageSelector">
          <el-option v-for="(v, k) in supportedInputLanguage" :label="v" :value="k" :key="k" />
        </el-select>
      </el-form-item>

      <el-form-item label="来源语言">
        <el-select v-model="selectedSourceLanguage" placeholder="Select" class="languageSelector">
          <el-option v-for="(v, k) in supportedInputLanguage" :label="v" :value="k" :key="k" />
        </el-select>
      </el-form-item>

      <el-form-item label="结果语言">
        <el-transfer
          v-model="transferComponentValue"
          :data="transferComponentData"
          :titles="['可选语言', '已选语言']"
        />
      </el-form-item>

      <el-form-item label="双子">
        <el-select v-model="selectedTwin" placeholder="Select" class="languageSelector">
          <el-option v-for="(v, k) in twinList" :label="v.label" :value="v.value" :key="k" />
        </el-select>
      </el-form-item>

      <el-form-item label="游戏资源路径">
        <div class="assetRow">
          <div class="assetInfo">
            <div class="assetPath">{{ global.config.assetDir || "(未设置)" }}</div>
            <div class="assetHint">
              <span v-if="assetDirValid" class="ok">✅ 路径有效</span>
              <span v-else class="bad">❌ 路径无效（请选择 GenshinImpact_Data 或包含 StreamingAssets 的目录）</span>
            </div>
          </div>

          <el-button @click="pickDir" :loading="picking">
            选择目录
          </el-button>
        </div>
      </el-form-item>

      <!-- ✅ 新版：已安装语音包 UI -->
      <el-form-item label="已安装语音包">
        <div class="voiceRow">
          <div class="voiceTop">
            <div class="voiceStatus">
              <span v-if="assetDirValid" class="ok">✅ 已检测到资源目录</span>
              <span v-else class="bad">❌ 资源目录无效（先选择目录）</span>
            </div>

            <div class="voiceActions">
              <el-button size="small" @click="refreshVoicePacks" :loading="refreshingVoice">
                刷新语音包列表
              </el-button>
            </div>
          </div>

          <div class="voiceTags" v-if="voiceTagList.length > 0">
            <el-tag
              v-for="v in voiceTagList"
              :key="v.code"
              effect="plain"
              class="langPackTag"
            >
              {{ v.name }}
            </el-tag>
          </div>

          <el-empty
            v-else
            description="未检测到语音包（请确认游戏里已下载语音，或目录层级是否正确）"
          />
        </div>
      </el-form-item>

      <el-form-item>
        <el-button type="primary" @click="save">
          保存
        </el-button>
      </el-form-item>
    </el-form>
  </div>
</template>

<script setup>
import global from "@/global/global"
import api from "@/api/basicInfo"
import { computed, onBeforeMount, ref } from "vue"
import { ElMessage } from "element-plus"

const selectedInputLanguage = ref(global.config.defaultSearchLanguage + "")
const selectedSourceLanguage = ref(global.config.sourceLanguage + "")
const supportedInputLanguage = ref({})

const twinList = ref([
  { value: false, label: "荧" },
  { value: true, label: "空" },
  { value: "both", label: "双子" },
])
const selectedTwin = ref(global.config.isMale)

const transferComponentData = ref([])
const transferComponentValue = ref([])

const picking = ref(false)
const refreshingVoice = ref(false)

// ✅ 用全局 config 的状态（后端 getConfig() / startupStatus 会带）
const assetDirValid = ref(!!global.config.assetDirValid)

// ✅ 把 global.voiceLanguages（对象）转成 tag 列表并排序显示
const voiceTagList = computed(() => {
  const obj = global.voiceLanguages || {}
  return Object.entries(obj)
    .map(([code, name]) => ({ code, name }))
    .sort((a, b) => Number(a.code) - Number(b.code))
})

onBeforeMount(async () => {
  supportedInputLanguage.value = global.languages

  // transfer 语言列表
  for (const [languageCode, languageName] of Object.entries(global.languages)) {
    transferComponentData.value.push({
      key: languageCode,
      label: languageName,
      disabled: false,
    })
  }

  // 已选语言
  for (let langCode of global.config.resultLanguages) {
    transferComponentValue.value.push(langCode + "")
  }

  // 初次进入设置页时也刷新一次语音包（如果目录有效）
  if (assetDirValid.value) {
    await refreshVoicePacks()
  }
})

const refreshVoicePacks = async () => {
  refreshingVoice.value = true
  try {
    const resp = await fetch("/api/getImportedVoiceLanguages")
    const payload = await resp.json()
    if (payload.code !== 200) {
      ElMessage({ type: "error", message: payload.msg || "刷新失败" })
      return
    }
    global.voiceLanguages = payload.data || {}
    ElMessage({ type: "success", message: "语音包列表已刷新" })
  } catch (e) {
    ElMessage({ type: "error", message: "刷新失败（后端未响应）" })
  } finally {
    refreshingVoice.value = false
  }
}

const pickDir = async () => {
  picking.value = true
  try {
    const resp = await fetch("/api/pickAssetDir", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    })
    const payload = await resp.json()
    if (payload.code !== 200) {
      ElMessage({ type: "error", message: payload.msg || "选择目录失败" })
      return
    }

    const data = payload.data || {}
    if (data.cancel) {
      ElMessage({ type: "info", message: "已取消选择" })
      return
    }

    // 更新全局 config
    global.config.assetDir = data.assetDir || ""
    global.config.assetDirValid = !!data.assetDirValid
    assetDirValid.value = !!data.assetDirValid

    // ✅ 选完目录立即刷新语音包 UI
    await refreshVoicePacks()

    ElMessage({
      type: assetDirValid.value ? "success" : "warning",
      message: assetDirValid.value ? "资源路径已设置 ✅" : "已选择目录，但校验未通过（请确认目录层级）",
    })
  } catch (e) {
    ElMessage({ type: "error", message: "选择目录失败（可能是后端不支持弹窗或运行环境不允许）" })
  } finally {
    picking.value = false
  }
}

const save = async () => {
  let newConfig = (await api.saveConfig(
    transferComponentValue.value,
    selectedInputLanguage.value,
    selectedSourceLanguage.value,
    selectedTwin.value
  )).json

  global.config.resultLanguages = newConfig.resultLanguages
  global.config.defaultSearchLanguage = newConfig.defaultSearchLanguage
  global.config.sourceLanguage = newConfig.sourceLanguage
  global.config.isMale = newConfig.isMale

  // 后端新版 getConfig() 会带 assetDirValid
  if (typeof newConfig.assetDirValid !== "undefined") {
    global.config.assetDirValid = !!newConfig.assetDirValid
    assetDirValid.value = !!newConfig.assetDirValid
  }

  ElMessage({ type: "success", message: "设置已保存" })
}
</script>

<style>
.viewWrapper {
  position: relative;
  width: 85%;
  margin: 0 auto;
  background-color: #fff;
  box-shadow: 0 3px 3px rgba(36, 37, 38, 0.05);
  border-radius: 3px;
  padding: 20px;
}

.pageTitle {
  border-bottom: 1px #ccc solid;
  padding-bottom: 10px;
}

.helpText {
  margin: 20px 0 20px 0;
  color: #999;
}

.languageSelector {
  width: 260px;
}

.assetRow {
  display: flex;
  gap: 12px;
  align-items: center;
  width: 100%;
}

.assetInfo {
  flex: 1;
  min-width: 0;
}

.assetPath {
  word-break: break-all;
}

.assetHint {
  margin-top: 4px;
  font-size: 12px;
}

.ok {
  color: #67c23a;
}
.bad {
  color: #f56c6c;
}

.voiceRow {
  width: 100%;
}

.voiceTop {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  margin-bottom: 10px;
}

.voiceStatus {
  font-size: 12px;
}

.voiceTags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.langPackTag {
  margin-right: 0 !important;
}
</style>
