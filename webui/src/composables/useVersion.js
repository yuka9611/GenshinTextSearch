import { ref } from 'vue'
import basicInfoApi from '@/api/basicInfo'

const useVersion = () => {
  const versionOptions = ref([])

  // 加载版本选项
  const loadVersionOptions = async () => {
    try {
      console.log('开始加载版本选项')
      const ans = await basicInfoApi.getAvailableVersions()
      console.log('版本选项加载成功:', ans.json)
      versionOptions.value = ans.json || []
    } catch (error) {
      console.error('版本选项加载失败:', error)
      versionOptions.value = []
    }
  }

  return {
    // 状态
    versionOptions,
    
    // 方法
    loadVersionOptions
  }
}

export default useVersion