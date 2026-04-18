const UNKNOWN_VERSION_TEXT = '未知'

export const normalizeText = (value) => {
  if (!value) return ''
  return String(value).trim().toLowerCase()
}

export const normalizeVersion = (value) => normalizeText(value)

export const resolveVersionValue = (versionTag, rawVersion) => {
  if (versionTag) return String(versionTag).trim()
  if (rawVersion) return String(rawVersion).trim()
  return ''
}

export const getNormalizedEntryVersion = (entry, kind) => {
  if (kind === 'created') {
    return normalizeVersion(entry.createdVersion || entry.createdVersionRaw || '')
  }
  return normalizeVersion(entry.updatedVersion || entry.updatedVersionRaw || '')
}

export const isSameCreatedUpdatedVersion = (entry) => {
  const createdVersion = getNormalizedEntryVersion(entry, 'created')
  const updatedVersion = getNormalizedEntryVersion(entry, 'updated')
  if (!createdVersion || !updatedVersion) return false
  return createdVersion === updatedVersion
}

export const matchVersionFilters = (entry, createdVersionFilter, updatedVersionFilter) => {
  const createdFilter = normalizeVersion(createdVersionFilter)
  const updatedFilter = normalizeVersion(updatedVersionFilter)
  const createdValue = getNormalizedEntryVersion(entry, 'created')
  const updatedValue = getNormalizedEntryVersion(entry, 'updated')

  if (createdFilter && !createdValue.includes(createdFilter)) return false
  if (updatedFilter) {
    if (!updatedValue.includes(updatedFilter)) return false
    if (isSameCreatedUpdatedVersion(entry)) return false
  }
  return true
}

export const displayVersion = (entry, kind) => {
  const value = kind === 'created'
    ? resolveVersionValue(entry.createdVersion, entry.createdVersionRaw)
    : resolveVersionValue(entry.updatedVersion, entry.updatedVersionRaw)
  return value || UNKNOWN_VERSION_TEXT
}

export const showUpdatedVersionTag = (entry) => {
  const updatedValue = resolveVersionValue(entry.updatedVersion, entry.updatedVersionRaw)
  if (!updatedValue) return false
  const createdValue = resolveVersionValue(entry.createdVersion, entry.createdVersionRaw)
  return createdValue !== updatedValue
}
