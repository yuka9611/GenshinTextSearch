<template>
  <div class="viewWrapper pageShell settingsView">
    <h1 class="pageTitle">设置</h1>

    <div class="helpText settingsHelpText">
      <p>所有已经导入到数据库的文本语言会在此处显示。要导入新的语言，请关闭服务器，并使用导入工具。</p>
      <p>所有游戏已下载的语言包会在此处显示。请进入游戏来管理语音包。</p>
      <p>现在可以在这里直接选择游戏资源路径。</p>
    </div>

    <el-form :label-width="120" label-position="left" class="settingsForm">
      <section class="settingsSection">
        <div class="sectionHeader">
          <div class="sectionEyebrow">Search Defaults</div>
          <h2 class="sectionTitle">检索偏好</h2>
          <p class="sectionDescription">统一管理默认输入语言、来源语言、结果语言和双子设定。</p>
        </div>

        <el-form-item label="默认搜索语言">
          <el-select v-model="selectedInputLanguage" placeholder="选择语言" class="languageSelector">
            <el-option v-for="(v, k) in supportedInputLanguage" :label="v" :value="k" :key="k" />
          </el-select>
        </el-form-item>

        <el-form-item label="来源语言">
          <el-select v-model="selectedSourceLanguage" placeholder="选择语言" class="languageSelector">
            <el-option v-for="(v, k) in supportedInputLanguage" :label="v" :value="k" :key="k" />
          </el-select>
        </el-form-item>

        <el-form-item label="结果语言">
          <el-select
            v-model="transferComponentValue"
            multiple
            collapse-tags
            collapse-tags-tooltip
            filterable
            clearable
            placeholder="选择结果语言"
            class="languageSelector"
          >
            <el-option
              v-for="item in transferComponentData"
              :key="item.key"
              :label="item.label"
              :value="item.key"
            />
          </el-select>
        </el-form-item>

        <el-form-item label="双子">
          <el-select v-model="selectedTwin" placeholder="请选择" class="languageSelector">
            <el-option v-for="(v, k) in twinList" :label="v.label" :value="v.value" :key="k" />
          </el-select>
        </el-form-item>
      </section>

      <section class="settingsSection">
        <div class="sectionHeader">
          <div class="sectionEyebrow">Assets</div>
          <h2 class="sectionTitle">资源与语音包</h2>
          <p class="sectionDescription">校验游戏资源目录，并同步当前机器上已经安装的语音语言包。</p>
        </div>

        <el-form-item label="游戏资源路径">
          <div class="assetControls">
            <div class="assetPathRow">
              <el-input
                v-model="assetDirDraft"
                :title="assetDirDraft || '(未设置)'"
                placeholder="填写 GenshinImpact_Data 或包含 StreamingAssets 的目录"
                clearable
                class="assetPathInput"
                @keydown.enter.prevent="save"
              >
                <template #append>
                  <el-button class="assetPathPickerButton" @click="pickDir" :loading="picking" :disabled="savingAssetDir || saving">
                    选择目录
                  </el-button>
                </template>
              </el-input>
            </div>
            <div class="assetHint">
              <span v-if="assetDirValid" class="ok">✅ 路径有效</span>
              <span v-else class="bad">❌ 路径无效（请选择 GenshinImpact_Data 或包含 StreamingAssets 的目录）</span>
            </div>
          </div>
        </el-form-item>

        <el-form-item label="已安装语音包">
          <div class="voiceRow statusCard">
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
      </section>

      <div class="actionBar">
        <el-button type="primary" @click="save" :loading="saving" :disabled="picking || savingAssetDir">
          保存
        </el-button>
      </div>
    </el-form>
  </div>
</template>

<script setup>
import global from "@/global/global"
import api from "@/api/basicInfo"
import { computed, onBeforeMount, ref } from "vue"
import { ElMessage } from "element-plus"
import useLanguage from "@/composables/useLanguage"

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
const savingAssetDir = ref(false)
const saving = ref(false)
const assetDirDraft = ref(global.config.assetDir || "")

// 使用语言处理组合式API
const {
  selectedInputLanguage,
  selectedSourceLanguage: selectedSourceLanguageRef,
  supportedInputLanguage,
  loadLanguages
} = useLanguage()

// 由于SettingsView需要同时处理输入语言和源语言，我们需要单独处理源语言
const selectedSourceLanguage = ref(global.config.sourceLanguage + "")

// ✅ 用全局 config 的状态（后端 getConfig() / startupStatus 会带）
const assetDirValid = ref(!!global.config.assetDirValid)

// ✅ 把 global.voiceLanguages（对象）转成 tag 列表并排序显示
const voiceTagList = computed(() => {
  const obj = global.voiceLanguages || {}
  return Object.entries(obj)
    .map(([code, name]) => ({ code, name }))
    .sort((a, b) => Number(a.code) - Number(b.code))
})
const assetDirDirty = computed(() => assetDirDraft.value.trim() !== String(global.config.assetDir || "").trim())

const syncAssetDirState = (data = {}) => {
  global.config.assetDir = data.assetDir || ""
  global.config.assetDirValid = !!data.assetDirValid
  assetDirValid.value = !!data.assetDirValid
  assetDirDraft.value = global.config.assetDir || ""
}

onBeforeMount(async () => {
  await loadLanguages()
  // 获取完整的配置
  try {
    const configResp = await api.getConfig()
    if (configResp.code === 200) {
      // 更新全局配置
      Object.assign(global.config, configResp.data)
      // 更新本地状态
      selectedSourceLanguage.value = global.config.sourceLanguage + ""
      selectedTwin.value = global.config.isMale
      assetDirValid.value = !!global.config.assetDirValid
      assetDirDraft.value = global.config.assetDir || ""
    }
  } catch (e) {
    console.error("获取配置失败:", e)
  }
  const languages = supportedInputLanguage.value

  // transfer 语言列表
  for (const [languageCode, languageName] of Object.entries(languages)) {
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
    const data = payload.data || {}

    if (payload.code !== 200) {
      if (data.dialogUnavailable) {
        ElMessage({
          type: "warning",
          message: "当前运行环境不支持目录选择弹窗，请手动填写资源路径后点击下方“保存”。",
        })
      } else {
        ElMessage({ type: "error", message: payload.msg || "选择目录失败" })
      }
      return
    }

    if (data.cancel) {
      ElMessage({ type: "info", message: "已取消选择" })
      return
    }

    syncAssetDirState(data)

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

const saveAssetDir = async ({ silentSuccess = false } = {}) => {
  const nextPath = assetDirDraft.value.trim()
  if (!nextPath) {
    ElMessage({ type: "warning", message: "请先填写资源路径" })
    return false
  }

  savingAssetDir.value = true
  try {
    const resp = await fetch("/api/setAssetDir", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ assetDir: nextPath }),
    })
    const payload = await resp.json()
    if (payload.code !== 200) {
      ElMessage({
        type: "error",
        message: payload.msg === "Invalid directory"
          ? "路径无效，请填写 GenshinImpact_Data 或包含 StreamingAssets 的目录"
          : (payload.msg || "保存资源路径失败"),
      })
      return false
    }

    syncAssetDirState(payload.data || {})
    await refreshVoicePacks()
    if (!silentSuccess) {
      ElMessage({
        type: assetDirValid.value ? "success" : "warning",
        message: assetDirValid.value ? "资源路径已保存 ✅" : "路径已保存，但校验未通过（请确认目录层级）",
      })
    }
    return true
  } catch (error) {
    console.error("保存资源路径失败:", error)
    ElMessage({ type: "error", message: "保存资源路径失败（后端未响应）" })
    return false
  } finally {
    savingAssetDir.value = false
  }
}

const save = async () => {
  saving.value = true
  try {
    if (assetDirDirty.value) {
      const savedAssetDir = await saveAssetDir({ silentSuccess: true })
      if (!savedAssetDir) return
    }

    const response = await api.saveConfig(
      transferComponentValue.value,
      selectedInputLanguage.value,
      selectedSourceLanguage.value,
      selectedTwin.value
    )

    if (response.json) {
      const newConfig = response.json

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
    } else {
      ElMessage({ type: "error", message: "保存失败：返回数据格式错误" })
    }
  } catch (error) {
    console.error("保存配置失败:", error)
    ElMessage({ type: "error", message: "保存失败：网络错误" })
  } finally {
    saving.value = false
  }
}
</script>

<style>
.settingsView {
  gap: 18px;
}

.settingsHelpText {
  margin: 20px 0 20px 0;
}

.settingsForm {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.settingsSection {
  padding: 20px;
  border-radius: 24px;
  border: 1px solid var(--theme-border);
  background: rgba(255, 255, 255, 0.42);
}

.sectionHeader {
  margin-bottom: 18px;
}

.sectionEyebrow {
  color: var(--theme-primary);
  font-size: 12px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  margin-bottom: 8px;
}

.sectionTitle {
  color: var(--theme-ink);
  font-family: var(--font-title);
  font-size: 1.2rem;
  margin-bottom: 6px;
}

.sectionDescription {
  color: var(--theme-text-muted);
  line-height: 1.7;
}

.languageSelector {
  width: 260px;
}

.assetControls {
  display: flex;
  flex-direction: column;
  gap: 8px;
  width: 100%;
}

.assetPathInput {
  flex: 1 1 auto;
  min-width: 0;
  width: 100%;
}

.assetPathRow {
  display: flex;
  align-items: stretch;
  gap: 8px;
  width: 100%;
}

.statusCard {
  padding: 14px 16px;
  border-radius: 16px;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.66), rgba(243, 231, 211, 0.40));
  border: 1px solid var(--theme-border);
}

.assetPathInput :deep(.el-input__inner) {
  color: var(--theme-text);
  font-weight: 600;
}

.assetPathInput :deep(.el-input-group__append) {
  padding: 0;
  overflow: hidden;
  background: rgba(255, 250, 242, 0.88);
}

.assetPathPickerButton {
  margin: 0;
  height: 100%;
  padding: 0 18px;
  border: none;
  border-radius: 0;
  background: transparent;
  box-shadow: none;
  transform: none;
}

.assetPathPickerButton:hover,
.assetPathPickerButton:focus-visible {
  background: rgba(255, 255, 255, 0.42);
  color: var(--theme-primary);
  transform: none;
}

.assetHint {
  font-size: 12px;
}

.ok {
  color: var(--theme-success);
}
.bad {
  color: var(--theme-danger);
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

.actionBar {
  display: flex;
  justify-content: flex-end;
  padding-top: 4px;
}

@media (max-width: 680px) {
  .settingsSection {
    padding: 16px;
  }

  .voiceTop {
    flex-direction: column;
    align-items: flex-start;
  }

  .actionBar {
    justify-content: stretch;
  }

  .actionBar .el-button {
    width: 100%;
  }
}

/* Dark mode overrides */
[data-theme="dark"] .settingsSection {
    background: rgba(30, 40, 37, 0.42);
}

[data-theme="dark"] .statusCard {
    background: linear-gradient(180deg, rgba(30, 40, 37, 0.66), rgba(22, 32, 30, 0.40));
}

[data-theme="dark"] .assetPathInput :deep(.el-input-group__append) {
    background: rgba(30, 40, 37, 0.88);
}

[data-theme="dark"] .assetPathPickerButton:hover,
[data-theme="dark"] .assetPathPickerButton:focus-visible {
    background: rgba(42, 56, 52, 0.95);
}

</style>
