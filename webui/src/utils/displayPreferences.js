import global from '@/global/global'

const DISPLAY_PREFERENCE_ENDPOINTS = new Set([
  '/api/keywordQuery',
  '/api/getTalkFromHash',
  '/api/getDialogueGroup',
  '/api/getSubtitleContext',
  '/api/nameSearch',
  '/api/npcDialogueSearch',
  '/api/npcDialogues',
  '/api/avatarSearch',
  '/api/avatarVoice',
  '/api/avatarVoiceSearch',
  '/api/avatarStory',
  '/api/avatarStorySearch',
  '/api/getReadableContent',
  '/api/getQuestDialogues',
  '/api/getEntityTexts',
  '/api/getTextEntitySources',
  '/api/catalogSearch',
])

const normalizePath = (url) => {
  try {
    return new URL(String(url || ''), 'https://local.invalid').pathname
  } catch {
    return String(url || '').split('?')[0]
  }
}

export const getDisplayPreferences = () => ({
  resultLanguages: Array.isArray(global.config.resultLanguages)
    ? global.config.resultLanguages.map(Number).filter(Number.isInteger)
    : [],
  sourceLanguage: Number(global.config.sourceLanguage),
  isMale: global.config.isMale,
})

export const getDisplayPreferencesFingerprint = () => {
  const preferences = getDisplayPreferences()
  return JSON.stringify([
    preferences.resultLanguages,
    preferences.sourceLanguage,
    preferences.isMale,
  ])
}

export const injectDisplayPreferences = (requestConfig) => {
  if (!global.runtime.cloudMode && !global.config.cloudMode) {
    return requestConfig
  }
  if (String(requestConfig?.method || 'get').toLowerCase() !== 'post') {
    return requestConfig
  }
  if (!DISPLAY_PREFERENCE_ENDPOINTS.has(normalizePath(requestConfig?.url))) {
    return requestConfig
  }
  if (!requestConfig.data || typeof requestConfig.data !== 'object' || Array.isArray(requestConfig.data)) {
    return requestConfig
  }

  requestConfig.data = {
    ...requestConfig.data,
    displayPreferences: getDisplayPreferences(),
  }
  return requestConfig
}
