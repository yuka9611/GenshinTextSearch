import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import axios from 'axios'

test('canceled searches are silent and release only their own loading state', async () => {
  let loadingCount = 0
  const errors = []
  globalThis.requestCancellationHarness = {
    loading: { startLoading() { loadingCount++ }, endLoading() { loadingCount-- } },
    message: { error: (message) => errors.push(message) },
  }
  const source = readFileSync(new URL('../src/utils/request.js', import.meta.url), 'utf8')
    .replace('from "axios"', `from ${JSON.stringify(import.meta.resolve('axios'))}`)
    .replace(/import loadingScreen[^\n]+/, 'const loadingScreen = globalThis.requestCancellationHarness.loading')
    .replace(/import \{ ElMessage \}[^\n]+/, 'const ElMessage = globalThis.requestCancellationHarness.message')
    .replace(/import router[^\n]+/, 'const router = { replace() {} }')
    .replace(/import \{ sanitizePayload, sanitizeText \}[^\n]+/, 'const sanitizePayload = (v) => v; const sanitizeText = (v) => v')
    .replace(/import \{ apiBaseUrl \}[^\n]+/, 'const apiBaseUrl = ""')
    .replace(/import \{ recordSearchFromResponse \}[^\n]+/, 'const recordSearchFromResponse = () => {}')
    .replace(/import \{ injectDisplayPreferences \}[^\n]+/, 'const injectDisplayPreferences = () => {}')
    .replaceAll('import.meta.env', '({})')
  const { default: request } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`)
  delete globalThis.requestCancellationHarness

  const pending = []
  request.defaults.adapter = (config) => new Promise((resolve, reject) => {
    pending.push({ config, reject, resolve: () => resolve({ config, data: { code: 200, data: {} }, status: 200, headers: {} }) })
    config.signal?.addEventListener('abort', () => reject(new axios.CanceledError('canceled', config)), { once: true })
  })
  const controller = new AbortController()
  const canceled = request.get('/api/keywordQuery', { signal: controller.signal })
  const canceledCheck = assert.rejects(canceled, (error) => axios.isCancel(error))
  const active = request.get('/api/other')
  const silent = request.get('/api/silent', { doNotShowLoadingScreen: true })
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(loadingCount, 2)
  controller.abort()
  await canceledCheck
  assert.equal(loadingCount, 1)
  assert.deepEqual(errors, [])
  pending[2].resolve()
  await silent
  assert.equal(loadingCount, 1)
  pending[1].resolve()
  await active
  assert.equal(loadingCount, 0)
  assert.deepEqual(errors, [])

  const timedOut = request.get('/api/getAvailableVersionFilters', { doNotShowLoadingScreen: true })
  const timeoutCheck = assert.rejects(timedOut, (error) => error.network && error.error.code === 'ECONNABORTED')
  await new Promise((resolve) => setImmediate(resolve))
  pending[3].reject(new axios.AxiosError('timeout of 30000ms exceeded', 'ECONNABORTED', pending[3].config))
  await timeoutCheck
  assert.equal(loadingCount, 0)
  assert.deepEqual(errors, ['版本筛选加载超时（30 秒），请重试'])
})

test('keyword query forwards cancellation options to the HTTP request', async () => {
  let received
  globalThis.keywordCancellationRequest = { post: (...args) => { received = args } }
  const source = readFileSync(new URL('../src/api/keywordQuery.js', import.meta.url), 'utf8')
    .replace(/import request[^\n]+/, 'const request = globalThis.keywordCancellationRequest')
    .replace(/import axios[^\n]+/, 'const axios = {}')
    .replace(/import \{ withCache \}[^\n]+/, 'const withCache = (fn) => fn')
    .replace(/import \{ apiUrl \}[^\n]+/, 'const apiUrl = (url) => url')
  const { default: api } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`)
  delete globalThis.keywordCancellationRequest
  const signal = new AbortController().signal
  api.queryByKeyword('test', 1, '', 1, 20, 'all', '', '', '', { signal })
  assert.equal(received[0], '/api/keywordQuery')
  assert.equal(received[1].keyword, 'test')
  assert.equal(received[2].signal, signal)
})
