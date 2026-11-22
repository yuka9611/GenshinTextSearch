import { ref } from 'vue'
import basicInfoApi from '@/api/basicInfo'

const versionOptions = ref([])
const createdVersionOptions = ref([])
const updatedVersionOptions = ref([])
let loadVersionOptionsPromise = null

const sortVersionOptions = (versions) => {
  return [...new Set(versions)].sort((a, b) => {
    const left = String(a).split('.').map((part) => Number(part))
    const right = String(b).split('.').map((part) => Number(part))
    return (right[0] - left[0]) || (right[1] - left[1])
  })
}

const useVersion = () => {
  const loadVersionOptions = async () => {
    if (!loadVersionOptionsPromise) {
      loadVersionOptionsPromise = (async () => {
        try {
          const ans = await basicInfoApi.getAvailableVersionFilters()
          const filters = ans.json || {}
          createdVersionOptions.value = filters.created || []
          updatedVersionOptions.value = filters.updated || []
          versionOptions.value = sortVersionOptions([
            ...createdVersionOptions.value,
            ...updatedVersionOptions.value,
          ])
        } catch (error) {
          console.error('failed to load version options:', error)
          const status = error?.error?.response?.status ?? error?.response?.status ?? error?.errorCode
          if (![404, 405].includes(Number(status))) return
          try {
            const ans = await basicInfoApi.getAvailableVersions()
            versionOptions.value = ans.json || []
            createdVersionOptions.value = versionOptions.value
            updatedVersionOptions.value = versionOptions.value
          } catch (fallbackError) {
            console.error('failed to load fallback version options:', fallbackError)
          }
        } finally {
          loadVersionOptionsPromise = null
        }
      })()
    }
    await loadVersionOptionsPromise
  }

  return {
    versionOptions,
    createdVersionOptions,
    updatedVersionOptions,
    loadVersionOptions
  }
}

export default useVersion
