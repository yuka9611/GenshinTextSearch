<script setup>
import { RouterView, useRouter } from 'vue-router'
import { onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

import global from '@/global/global'
import loading from '@/global/loading'

const router = useRouter()
const startupChecked = ref(false)
let disposeBrowserSessionMonitor = () => {}

const BROWSER_SESSION_HEARTBEAT_MS = 20000
const BROWSER_SESSION_STORAGE_KEY = 'gts-browser-session-id'

function getBrowserSessionId() {
  try {
    const existingId = window.sessionStorage.getItem(BROWSER_SESSION_STORAGE_KEY)
    if (existingId) {
      return existingId
    }

    const nextId = typeof crypto !== 'undefined' && crypto.randomUUID
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(16).slice(2)}`

    window.sessionStorage.setItem(BROWSER_SESSION_STORAGE_KEY, nextId)
    return nextId
  } catch (e) {
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`
  }
}

function createBrowserSessionMonitor() {
  if (typeof window === 'undefined') {
    return () => {}
  }

  const clientId = getBrowserSessionId()
  let timerId = 0
  let closed = false

  const postJson = async (url, keepalive = false) => {
    try {
      await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ clientId }),
        keepalive
      })
    } catch (e) {
      // The server may already be stopping; silence network noise.
    }
  }

  const sendHeartbeat = () => {
    void postJson('/api/browser-session/heartbeat')
  }

  const sendDisconnect = () => {
    if (closed) {
      return
    }
    closed = true

    const payload = JSON.stringify({ clientId })
    try {
      if (navigator.sendBeacon) {
        const blob = new Blob([payload], { type: 'application/json' })
        if (navigator.sendBeacon('/api/browser-session/disconnect', blob)) {
          return
        }
      }
    } catch (e) {
      // Fall through to keepalive fetch.
    }

    void postJson('/api/browser-session/disconnect', true)
  }

  const handlePageHide = (event) => {
    if (event.persisted) {
      return
    }
    sendDisconnect()
  }

  const handleVisibilityChange = () => {
    if (document.visibilityState === 'visible' && !closed) {
      sendHeartbeat()
    }
  }

  sendHeartbeat()
  timerId = window.setInterval(sendHeartbeat, BROWSER_SESSION_HEARTBEAT_MS)
  window.addEventListener('pagehide', handlePageHide)
  window.addEventListener('beforeunload', sendDisconnect)
  document.addEventListener('visibilitychange', handleVisibilityChange)

  return () => {
    window.clearInterval(timerId)
    window.removeEventListener('pagehide', handlePageHide)
    window.removeEventListener('beforeunload', sendDisconnect)
    document.removeEventListener('visibilitychange', handleVisibilityChange)
  }
}

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

  if (payload.code !== 200) {
    if (data.dialogUnavailable) {
      ElMessage({
        type: 'warning',
        message: '当前运行环境不支持目录选择弹窗，请前往设置页手动填写资源路径。'
      })
      return { status: 'unavailable' }
    }
    ElMessage({ type: 'error', message: payload.msg || '选择目录失败' })
    return { status: 'error' }
  }

  if (data.cancel) {
    ElMessage({ type: 'info', message: '已取消选择目录' })
    return { status: 'cancel' }
  }

  global.config.assetDir = data.assetDir || ''
  global.config.assetDirValid = !!data.assetDirValid

  return {
    status: 'picked',
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
    if (!picked || picked.status !== 'picked') {
      if (picked?.status === 'unavailable') {
        await jumpToSettings()
      }
      return
    }

    if (picked.assetDirValid) {
      ElMessage({ type: 'success', message: '资源目录已设置 ✅' })
    } else {
      ElMessage({ type: 'warning', message: '已选择目录，但校验未通过（请确认目录层级）' })
      await jumpToSettings()
    }
  } catch (action) {
    if (action === 'cancel') {
      await jumpToSettings()
    }
  }
}

onMounted(async () => {
  disposeBrowserSessionMonitor = createBrowserSessionMonitor()

  if (startupChecked.value) return
  startupChecked.value = true

  loading.startLoading() // :contentReference[oaicite:3]{index=3}
  try {
    await ensureAssetDirOnFirstRun()
  } finally {
    loading.endLoading()
  }
})

onBeforeUnmount(() => {
  disposeBrowserSessionMonitor()
})
</script>

<template>
  <RouterView v-slot="{ Component }">
    <Transition name="page-fade" mode="out-in">
      <component :is="Component" />
    </Transition>
  </RouterView>
</template>

<style>
.page-fade-enter-active,
.page-fade-leave-active {
  transition: opacity 0.18s ease, transform 0.18s ease;
}

.page-fade-enter-from {
  opacity: 0;
  transform: translateY(8px);
}

.page-fade-leave-to {
  opacity: 0;
  transform: translateY(-4px);
}
</style>
