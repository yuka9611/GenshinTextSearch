import { ref, computed, watch } from 'vue'
import global from '@/global/global'
import basicInfoApi from '@/api/basicInfo'

const toLanguageMap = (payload) => {
  if (!payload) return {}
  if (Array.isArray(payload)) {
    const mapped = {}
    payload.forEach(([code, name]) => {
      mapped[String(code)] = name
    })
    return mapped
  }
  if (typeof payload === 'object') {
    return Object.fromEntries(
      Object.entries(payload).map(([code, name]) => [String(code), name])
    )
  }
  return {}
}

const useLanguage = () => {
  const selectedInputLanguage = ref(String(global.config.defaultSearchLanguage ?? ''))

  const filteredLanguages = computed(() => {
    const filtered = {}
    for (const [key, value] of Object.entries(global.languages)) {
      if (value !== undefined && value !== null && value !== '') {
        filtered[key] = value
      }
    }
    return filtered
  })

  const supportedInputLanguage = computed(() => {
    return filteredLanguages.value
  })

  watch(() => global.config.defaultSearchLanguage, (newValue) => {
    if (newValue !== undefined && newValue !== null && newValue !== '') {
      selectedInputLanguage.value = String(newValue)
    }
  })

  const loadLanguages = async () => {
    try {
      const textLanguages = (await basicInfoApi.getImportedTextLanguages()).json
      global.languages = toLanguageMap(textLanguages)

      const voiceLanguages = (await basicInfoApi.getImportedVoiceLanguages()).json
      global.voiceLanguages = toLanguageMap(voiceLanguages)

      const config = (await basicInfoApi.getConfig()).json
      global.config = config
      if (config.defaultSearchLanguage !== undefined && config.defaultSearchLanguage !== null && config.defaultSearchLanguage !== '') {
        selectedInputLanguage.value = String(config.defaultSearchLanguage)
      }
    } catch (_) {
      console.error('failed to load language settings')
    }
  }

  return {
    selectedInputLanguage,
    supportedInputLanguage,
    filteredLanguages,
    loadLanguages
  }
}

export default useLanguage