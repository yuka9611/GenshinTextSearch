import { ref, computed, watch } from 'vue'
import global from '@/global/global'
import basicInfoApi from '@/api/basicInfo'

const useLanguage = () => {
  const selectedInputLanguage = ref(global.config.defaultSearchLanguage + '')

  // 过滤语言列表，移除undefined值
  const filteredLanguages = computed(() => {
    const filtered = {}
    for (const [key, value] of Object.entries(global.languages)) {
      if (value !== undefined && value !== null && value !== '') {
        filtered[key] = value
      }
    }
    return filtered
  })

  // 支持的输入语言，自动更新
  const supportedInputLanguage = computed(() => {
    return filteredLanguages.value
  })

  // 监听 global.config 的变化，更新 selectedInputLanguage
  watch(() => global.config.defaultSearchLanguage, (newValue) => {
    if (newValue) {
      selectedInputLanguage.value = newValue + ''
    }
  })

  // 加载语言列表
  const loadLanguages = async () => {
    try {
      // 直接从 API 加载语言列表，确保数据最新
      const textLanguages = (await basicInfoApi.getImportedTextLanguages()).json
      // 将语言列表转换为对象格式，键为字符串类型的语言代码
      const languagesObj = {}
      textLanguages.forEach(([code, name]) => {
        languagesObj[code.toString()] = name
      })
      global.languages = languagesObj
      const voiceLanguages = (await basicInfoApi.getImportedVoiceLanguages()).json
      // 将语音语言列表也转换为对象格式
      const voiceLanguagesObj = {}
      voiceLanguages.forEach(([code, name]) => {
        voiceLanguagesObj[code.toString()] = name
      })
      global.voiceLanguages = voiceLanguagesObj
      const config = (await basicInfoApi.getConfig()).json
      global.config = config
      if (config.defaultSearchLanguage) {
        selectedInputLanguage.value = config.defaultSearchLanguage + ''
      }
    } catch (_) {
      console.error('加载语言列表失败')
    }
  }

  return {
    // 状态
    selectedInputLanguage,
    supportedInputLanguage,
    filteredLanguages,
    
    // 方法
    loadLanguages
  }
}

export default useLanguage