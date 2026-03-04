import { ref } from 'vue'
import basicInfoApi from '@/api/basicInfo'

const useVersion = () => {
  const versionOptions = ref([])

  const loadVersionOptions = async () => {
    try {
      const ans = await basicInfoApi.getAvailableVersions()
      versionOptions.value = ans.json || []
    } catch (error) {
      console.error('failed to load version options:', error)
      versionOptions.value = []
    }
  }

  return {
    versionOptions,
    loadVersionOptions
  }
}

export default useVersion