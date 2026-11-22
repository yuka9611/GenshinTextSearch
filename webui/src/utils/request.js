import axios from "axios"
import loadingScreen from "@/global/loading"
import { ElMessage } from "element-plus"
import router from "@/router"
import { sanitizePayload, sanitizeText } from "@/utils/textSanitizer"
import { apiBaseUrl } from "@/utils/apiUrl"
import { recordSearchFromResponse } from "@/services/userData"
import { injectDisplayPreferences } from "@/utils/displayPreferences"

const MSG = Object.freeze({
    error: "错误",
    badRequest: "请求错误",
    permissionDenied: "权限不足",
    internalServerError: "服务器内部错误",
    dataFileMissing: "必要数据文件未加载",
    networkError: "网络错误，请检查网络连接",
    unsupportedMedia: "不支持的媒体类型或数据文件缺失",
    payloadTooLarge: "请求内容过大",
    networkErrorPrefix: "网络错误",
    unknownError: "未知错误",
})

const configuredTimeout = Number.parseInt(import.meta.env.VITE_API_TIMEOUT_MS || "30000", 10)
const apiTimeout = Number.isFinite(configuredTimeout) && configuredTimeout > 0
    ? configuredTimeout
    : 30000

const requestLabels = Object.freeze({
    '/api/keywordQuery': '文本查询',
    '/api/getAvailableVersionFilters': '版本筛选加载',
    '/api/getAvailableVersions': '版本列表加载',
    '/api/getImportedTextLanguages': '文本语言加载',
    '/api/getImportedVoiceLanguages': '语音语言加载',
    '/api/getSettings': '设置加载',
    '/api/getTalkFromHash': '对话详情加载',
    '/api/getDialogueGroup': '对话详情加载',
    '/api/nameSearch': '名称查询',
    '/api/npcDialogueSearch': 'NPC 对话查询',
    '/api/avatarVoiceSearch': '角色语音查询',
    '/api/avatarStorySearch': '角色故事查询',
    '/api/catalogSearch': '图鉴查询',
})

const service = axios.create({
    headers: {},
    timeout: apiTimeout,
})

if (apiBaseUrl) {
    service.defaults.baseURL = apiBaseUrl
}

service.interceptors.request.use((config) => {
    injectDisplayPreferences(config)
    if (!config.doNotShowLoadingScreen) {
        loadingScreen.startLoading()
    }
    return config
})

service.interceptors.response.use((response) => {
    if (!response.config?.doNotShowLoadingScreen) {
        loadingScreen.endLoading()
    }

    const sanitizedResponseData = sanitizePayload(response.data || {})
    response.data = sanitizedResponseData

    if (response.data.code !== 200) {
        return Promise.reject({
            network: false,
            response: response,
            errorCode: response.data.code,
            defaultHandler: (prefix) => {
                if (response.data.msg) {
                    ElMessage.error(`${prefix ? prefix : MSG.error}: ${response.data.msg}`)
                    return
                }

                switch (response.data.code) {
                    case 400:
                        ElMessage.error(MSG.badRequest)
                        return
                    case 403:
                        ElMessage.error(MSG.permissionDenied)
                        return
                    case 404:
                        router.replace("/error")
                        return
                    case 500:
                        ElMessage.error(MSG.internalServerError)
                        console.error(response.data)
                        return
                    case 550:
                        ElMessage.error(MSG.dataFileMissing)
                        return
                    default:
                        ElMessage.error(`${prefix ? prefix : MSG.error}: ${response.data.code}`)
                        return
                }
            },
        })
    }

    response.json = sanitizePayload(response.data.data)
    void recordSearchFromResponse(response.config, response.data)
    return response
}, (error) => {
    if (!error.config?.doNotShowLoadingScreen) {
        loadingScreen.endLoading()
    }

    if (axios.isCancel(error)) {
        return Promise.reject(error)
    }

    if (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT') {
        const path = new URL(error.config?.url || '', 'http://gts.invalid').pathname
        const label = requestLabels[path] || '请求'
        const seconds = (error.config?.timeout || apiTimeout) / 1000
        ElMessage.error(`${label}超时（${seconds} 秒），请重试`)
        return Promise.reject({ network: true, error })
    }

    if (error.code === "ERR_NETWORK") {
        error.network = true
        ElMessage.error(MSG.networkError)
        error.defaultHandler = () => {}
        return Promise.reject(error)
    }

    if (error.network === false) {
        return Promise.reject(error)
    }

    const status = error?.response?.status
    switch (status) {
        case 415:
            ElMessage.error(MSG.unsupportedMedia)
            break
        case 413:
            ElMessage.error(MSG.payloadTooLarge)
            break
        default:
            ElMessage.error(`${MSG.networkErrorPrefix}: ${sanitizeText(error.message || MSG.unknownError)}`)
            break
    }

    return Promise.reject({
        network: true,
        error: error,
    })
})

export default service
