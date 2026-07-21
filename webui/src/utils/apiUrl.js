const configuredBaseUrl = String(import.meta.env.VITE_AXIOS_BASE_URL || '').trim()

export const apiBaseUrl = configuredBaseUrl
  ? configuredBaseUrl.replace(/\/+$/, '') + '/'
  : ''

export function apiUrl(path) {
  if (!apiBaseUrl) {
    return path
  }
  return new URL(path, apiBaseUrl).toString()
}

export function apiFetch(path, options) {
  return fetch(apiUrl(path), options)
}
