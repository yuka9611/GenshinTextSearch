<script setup>
import { RouterView, useRouter } from 'vue-router'
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import global from '@/global/global'
import loading from '@/global/loading'

const router = useRouter()
const startupChecked = ref(false)

async function jumpToSettings() {
  // 你的路由 name 是 settingsView :contentReference[oaicite:2]{index=2}
  try {
    await router.push({ name: 'settingsView' })
    return
  } catch (e) {
    // fallback
  }
  try {
    await router.push('/settings')
  } catch (e) {
    // ignore
  }
}

async function refreshStartupStatus() {
  const resp = await fetch('/api/startupStatus')
  const payload = await resp.json()
  const data = payload?.data || {}

  global.config.assetDir = data.assetDir || ''
  global.config.assetDirValid = !!data.assetDirValid

  return {
    assetDir: global.config.assetDir,
    assetDirValid: global.config.assetDirValid
  }
}

async function pickDirViaBackendDialog() {
  const resp = await fetch('/api/pickAssetDir', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({})
  })
  const payload = await resp.json()
  const data = payload?.data || {}

  if (data.cancel) {
    ElMessage({ type: 'info', message: '已取消选择目录' })
    return null
  }

  global.config.assetDir = data.assetDir || ''
  global.config.assetDirValid = !!data.assetDirValid

  return {
    assetDir: global.config.assetDir,
    assetDirValid: global.config.assetDirValid
  }
}

async function ensureAssetDirOnFirstRun() {
  const status = await refreshStartupStatus()
  if (status.assetDirValid) return

  try {
    await ElMessageBox.confirm(
      '检测到尚未设置有效的“游戏资源目录”（请选择 GenshinImpact_Data 或包含 StreamingAssets 的目录）。\n\n是否现在选择目录？',
      '首次启动设置',
      {
        confirmButtonText: '立即选择目录',
        cancelButtonText: '去设置页',
        type: 'warning',
        distinguishCancelAndClose: true
      }
    )

    // 立即选择目录
    const picked = await pickDirViaBackendDialog()
    if (!picked) return

    if (picked.assetDirValid) {
      ElMessage({ type: 'success', message: '资源目录已设置 ✅' })
    } else {
      ElMessage({ type: 'warning', message: '已选择目录，但校验未通过（请确认目录层级）' })
      await jumpToSettings()
    }
  } catch (e) {
    // 去设置页 或关闭弹窗
    await jumpToSettings()
  }
}

onMounted(async () => {
  if (startupChecked.value) return
  startupChecked.value = true

  loading.startLoading() // :contentReference[oaicite:3]{index=3}
  try {
    await ensureAssetDirOnFirstRun()
  } finally {
    loading.endLoading()
  }
})
</script>

<template>
  <RouterView />
</template>

<style scoped></style>
