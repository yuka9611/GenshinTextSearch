import { onActivated, ref, watch } from 'vue'
import global from '@/global/global'

const useDisplayPreferenceRefresh = (refresh, shouldRefresh = () => true) => {
  const handledRevision = ref(global.displayPreferencesRevision)

  const refreshIfNeeded = async () => {
    const revision = global.displayPreferencesRevision
    if (revision === handledRevision.value) return
    handledRevision.value = revision
    if (!shouldRefresh()) return
    await refresh()
  }

  watch(() => global.displayPreferencesRevision, refreshIfNeeded)
  onActivated(refreshIfNeeded)
}

export default useDisplayPreferenceRefresh
