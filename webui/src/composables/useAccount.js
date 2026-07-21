import { computed, ref } from 'vue'
import { isSupabaseConfigured, supabase } from '@/services/supabase'
import { migrateLocalHistory } from '@/services/userData'

const session = ref(null)
const initialized = ref(false)
const loading = ref(false)
const authError = ref('')
let initializePromise = null
let unsubscribe = null

const providerSetting = import.meta.env.VITE_AUTH_PROVIDERS
const configuredProviders = String(providerSetting === undefined ? 'github,google' : providerSetting)
  .split(',')
  .map((provider) => provider.trim().toLowerCase())
  .filter(Boolean)

const applySession = async (nextSession) => {
  const previousUserId = session.value?.user?.id
  session.value = nextSession || null
  if (nextSession?.user?.id && nextSession.user.id !== previousUserId) {
    await migrateLocalHistory().catch((error) => {
      console.warn('本地搜索记录迁移失败:', error?.message || error)
    })
  }
}

export const initializeAccount = async () => {
  if (initializePromise) return initializePromise
  initializePromise = (async () => {
    if (!isSupabaseConfigured) {
      initialized.value = true
      return null
    }
    loading.value = true
    try {
      const { data, error } = await supabase.auth.getSession()
      if (error) throw error
      await applySession(data.session)
      if (!unsubscribe) {
        const listener = supabase.auth.onAuthStateChange((_event, nextSession) => {
          window.setTimeout(() => void applySession(nextSession), 0)
        })
        unsubscribe = () => listener.data.subscription.unsubscribe()
      }
      return session.value
    } catch (error) {
      authError.value = error?.message || '账号状态加载失败'
      return null
    } finally {
      loading.value = false
      initialized.value = true
    }
  })()
  return initializePromise
}

export const signInWithProvider = async (provider) => {
  if (!isSupabaseConfigured || !configuredProviders.includes(provider)) {
    throw new Error('该登录方式尚未配置')
  }
  authError.value = ''
  const { error } = await supabase.auth.signInWithOAuth({
    provider,
    options: { redirectTo: `${window.location.origin}${window.location.pathname}` },
  })
  if (error) {
    authError.value = error.message
    throw error
  }
}

export const signOut = async () => {
  if (!supabase) return
  const { error } = await supabase.auth.signOut()
  if (error) throw error
  await applySession(null)
}

export const useAccount = () => {
  const user = computed(() => session.value?.user || null)
  const isAnonymous = computed(() => Boolean(user.value?.is_anonymous))
  const isSignedIn = computed(() => Boolean(user.value && !isAnonymous.value))
  const displayName = computed(() => {
    const metadata = user.value?.user_metadata || {}
    return metadata.full_name || metadata.name || metadata.user_name || user.value?.email || '已登录'
  })
  const avatarUrl = computed(() => user.value?.user_metadata?.avatar_url || '')

  return {
    session,
    user,
    initialized,
    loading,
    authError,
    isConfigured: isSupabaseConfigured,
    isAnonymous,
    isSignedIn,
    displayName,
    avatarUrl,
    providers: configuredProviders,
    initializeAccount,
    signInWithProvider,
    signOut,
  }
}
