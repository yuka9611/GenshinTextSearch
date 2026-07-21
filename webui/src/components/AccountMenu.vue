<template>
  <el-popover placement="bottom-end" :width="300" trigger="click" popper-class="accountPopover">
    <template #reference>
      <button class="accountButton" :title="accountTitle">
        <img v-if="avatarUrl" :src="avatarUrl" alt="" class="accountAvatar" referrerpolicy="no-referrer" />
        <i v-else class="fi fi-rr-user"></i>
        <span class="accountLabel">{{ accountLabel }}</span>
        <i class="fi fi-rr-angle-small-down accountChevron"></i>
      </button>
    </template>

    <div v-if="isSignedIn" class="accountPanel">
      <div class="accountIdentity">
        <img v-if="avatarUrl" :src="avatarUrl" alt="" class="accountPanelAvatar" referrerpolicy="no-referrer" />
        <span v-else class="accountPanelAvatar accountPanelAvatar--fallback">
          <i class="fi fi-rr-user"></i>
        </span>
        <div>
          <div class="accountName">{{ displayName }}</div>
          <div class="accountHint">偏好与搜索记录正在同步</div>
        </div>
      </div>
      <el-button class="accountAction" @click="goToHistory">查看搜索记录</el-button>
      <el-button class="accountAction" text @click="handleSignOut">退出登录</el-button>
    </div>

    <div v-else class="accountPanel">
      <div class="accountIntro">
        <strong>访客模式</strong>
        <span>当前记录保存在本机。登录后可跨设备同步偏好与搜索记录。</span>
      </div>
      <el-button
        v-if="providers.includes('github')"
        class="accountAction providerButton"
        @click="handleProvider('github')"
      >
        <i class="fi fi-brands-github"></i>
        使用 GitHub 登录
      </el-button>
      <el-button
        v-if="providers.includes('google')"
        class="accountAction providerButton"
        @click="handleProvider('google')"
      >
        <span class="googleMark">G</span>
        使用 Google 登录
      </el-button>
      <p v-if="!isConfigured" class="accountWarning">账号同步服务尚未配置。</p>
      <p v-else-if="!providers.length" class="accountWarning">GitHub / Google 登录方式正在配置中。</p>
    </div>
  </el-popover>
</template>

<script setup>
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { useAccount } from '@/composables/useAccount'

const router = useRouter()
const {
  isConfigured,
  isSignedIn,
  displayName,
  avatarUrl,
  providers,
  signInWithProvider,
  signOut,
} = useAccount()

const accountLabel = computed(() => isSignedIn.value ? displayName.value : '访客模式')
const accountTitle = computed(() => isSignedIn.value ? '账号与同步状态' : '登录后同步偏好与搜索记录')

const handleProvider = async (provider) => {
  try {
    await signInWithProvider(provider)
  } catch (error) {
    ElMessage.error(error?.message || '登录失败')
  }
}

const handleSignOut = async () => {
  try {
    await signOut()
    ElMessage.success('已退出登录')
  } catch (error) {
    ElMessage.error(error?.message || '退出失败')
  }
}

const goToHistory = () => router.push('/history')
</script>

<style scoped>
.accountButton {
  min-width: 124px;
  height: 38px;
  padding: 0 12px;
  border-radius: 12px;
  border: 1px solid rgba(255, 255, 255, 0.2);
  background: rgba(255, 255, 255, 0.08);
  color: rgba(255, 255, 255, 0.94);
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  cursor: pointer;
  font: inherit;
  font-size: 13px;
  transition: background 0.15s ease, transform 0.15s ease;
}

.accountButton:hover,
.accountButton:focus-visible {
  background: rgba(255, 255, 255, 0.16);
  transform: translateY(-1px);
  outline: none;
}

.accountAvatar {
  width: 24px;
  height: 24px;
  border-radius: 50%;
  object-fit: cover;
}

.accountLabel {
  max-width: 132px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.accountChevron {
  font-size: 12px;
  opacity: 0.72;
}

.accountPanel {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.accountIntro {
  display: flex;
  flex-direction: column;
  gap: 5px;
  color: var(--theme-text);
}

.accountIntro span,
.accountHint {
  color: var(--theme-text-muted);
  font-size: 12px;
  line-height: 1.55;
}

.accountIdentity {
  display: flex;
  align-items: center;
  gap: 12px;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--theme-border);
}

.accountPanelAvatar {
  width: 42px;
  height: 42px;
  border-radius: 50%;
  object-fit: cover;
}

.accountPanelAvatar--fallback {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: rgba(47, 105, 101, 0.1);
  color: var(--theme-primary);
}

.accountName {
  color: var(--theme-ink);
  font-weight: 700;
  margin-bottom: 2px;
}

.accountAction {
  width: 100%;
  margin-left: 0 !important;
}

.providerButton :deep(span) {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
}

.googleMark {
  color: #4285f4;
  font-weight: 800;
}

.accountWarning {
  margin: 0;
  color: var(--el-color-warning-dark-2);
  font-size: 12px;
}

@media (max-width: 680px) {
  .accountButton {
    min-width: 38px;
    width: 38px;
    padding: 0;
  }
  .accountLabel,
  .accountChevron {
    display: none;
  }
}
</style>
