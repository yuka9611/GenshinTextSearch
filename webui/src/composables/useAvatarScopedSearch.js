import { ref } from 'vue'

const normalizeAvatarId = (avatarId) => {
  const normalized = Number.parseInt(avatarId, 10)
  return Number.isNaN(normalized) ? null : normalized
}

const buildAvatarResultsFromEntries = (entries, fallbackAvatarName) => {
  const avatarMap = new Map()
  for (const entry of entries) {
    const avatarId = normalizeAvatarId(entry?.avatarId)
    if (avatarId === null || avatarMap.has(avatarId)) continue
    avatarMap.set(avatarId, {
      avatarId,
      name: (entry.avatarName || '').trim() || `${fallbackAvatarName} ${avatarId}`
    })
  }
  return Array.from(avatarMap.values()).sort((left, right) => left.name.localeCompare(right.name))
}

const buildMatchedAvatarIds = (avatars) => {
  return new Set(
    (avatars || [])
      .map((avatar) => normalizeAvatarId(avatar?.avatarId))
      .filter((avatarId) => avatarId !== null)
  )
}

const useAvatarScopedSearch = ({
  fallbackAvatarName,
  matchedAvatarLabel,
  globalAvatarLabel,
}) => {
  const avatarResults = ref([])
  const scopedEntries = ref([])
  const globalEntries = ref([])
  const useGlobalEntries = ref(false)
  const selectedAvatar = ref(null)

  const resetScopedState = () => {
    scopedEntries.value = []
    globalEntries.value = []
    useGlobalEntries.value = false
    selectedAvatar.value = null
  }

  const setAvatarMatches = (avatars) => {
    avatarResults.value = avatars || []
    return buildMatchedAvatarIds(avatarResults.value)
  }

  const beginGlobalResults = (hasAvatarKeyword) => {
    selectedAvatar.value = {
      avatarId: null,
      name: hasAvatarKeyword ? matchedAvatarLabel : globalAvatarLabel,
    }
  }

  const showGlobalEntries = (entries) => {
    scopedEntries.value = entries || []
    globalEntries.value = scopedEntries.value
    useGlobalEntries.value = true
    avatarResults.value = buildAvatarResultsFromEntries(scopedEntries.value, fallbackAvatarName)
  }

  const filterEntriesByMatchedAvatarIds = (entries, matchedAvatarIds) => {
    if (!matchedAvatarIds || matchedAvatarIds.size === 0) return entries || []
    return (entries || []).filter((entry) => matchedAvatarIds.has(normalizeAvatarId(entry?.avatarId)))
  }

  const selectAvatarFromGlobalEntries = (avatar) => {
    const avatarId = normalizeAvatarId(avatar?.avatarId)
    if (avatarId === null) return null
    selectedAvatar.value = { ...avatar, avatarId }
    scopedEntries.value = globalEntries.value.filter((entry) => normalizeAvatarId(entry?.avatarId) === avatarId)
    return avatarId
  }

  const setAvatarEntries = (avatar, entries) => {
    const avatarId = normalizeAvatarId(avatar?.avatarId)
    if (avatarId === null) return null
    selectedAvatar.value = { ...avatar, avatarId }
    scopedEntries.value = entries || []
    return avatarId
  }

  return {
    avatarResults,
    scopedEntries,
    globalEntries,
    useGlobalEntries,
    selectedAvatar,
    resetScopedState,
    setAvatarMatches,
    beginGlobalResults,
    showGlobalEntries,
    filterEntriesByMatchedAvatarIds,
    selectAvatarFromGlobalEntries,
    setAvatarEntries,
  }
}

export default useAvatarScopedSearch
