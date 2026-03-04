import request from '@/utils/request'
import { withCache } from '@/utils/requestCache'

// Load imported text languages.
const getImportedTextLanguages = withCache(() => {
  return request.get('/api/getImportedTextLanguages')
})

// Load imported voice languages.
const getImportedVoiceLanguages = withCache(() => {
  return request.get('/api/getImportedVoiceLanguages')
})

const getAvailableVersions = () => {
  // Version aggregation can be heavy on large databases, but we need fresh data.
  return request.get('/api/getAvailableVersions', { timeout: 30000 })
}

const saveConfig = (resultLanguages, defaultSearchLanguage, sourceLanguage, isMale) => {
  const normalizedLanguages = []
  for (const code of resultLanguages) {
    normalizedLanguages.push(parseInt(code))
  }

  return request.post('/api/saveSettings', {
    config: {
      resultLanguages: normalizedLanguages,
      defaultSearchLanguage: parseInt(defaultSearchLanguage),
      sourceLanguage: parseInt(sourceLanguage),
      isMale,
    }
  })
}

const getConfig = () => {
  return request.get('/api/getSettings')
}

export default {
  getImportedTextLanguages,
  getImportedVoiceLanguages,
  getAvailableVersions,
  getConfig,
  saveConfig,
}