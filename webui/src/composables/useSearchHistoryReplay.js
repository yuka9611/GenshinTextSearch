import { nextTick, onActivated, onDeactivated, ref } from 'vue'
import { takeSearchHistoryReplay } from '../utils/searchHistoryReplay.js'

const useSearchHistoryReplay = ({ searchType, ready, restore, search }) => {
  const restoringHistory = ref(false)
  let activation = 0

  onDeactivated(() => { activation += 1 })
  onActivated(async () => {
    const currentActivation = ++activation
    const replay = takeSearchHistoryReplay(searchType)
    if (!replay) return
    try {
      // Language initialization applies the saved default. Restore afterwards so
      // that default cannot overwrite the language belonging to this search.
      await ready()
      if (activation !== currentActivation) return
      restoringHistory.value = true
      restore(replay)
      // Let filter watchers flush while restoration is still suppressed.
      await nextTick()
      restoringHistory.value = false
      if (activation === currentActivation) await search()
    } catch (error) {
      error?.defaultHandler?.()
      console.error('搜索记录重新搜索失败:', error)
    } finally {
      restoringHistory.value = false
    }
  })

  return { restoringHistory }
}

export default useSearchHistoryReplay
