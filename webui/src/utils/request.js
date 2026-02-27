import axios from "axios"
import loadingScreen from "@/global/loading"
import { ElMessage } from "element-plus"
import router from "@/router"
import { sanitizePayload, sanitizeText } from "@/utils/textSanitizer"

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

const service = axios.create({
    headers: {},
    timeout: 5000,
})

if (import.meta.env.VITE_AXIOS_BASE_URL) {
    service.defaults.baseURL = import.meta.env.VITE_AXIOS_BASE_URL
}

service.interceptors.request.use((config) => {
    if (!config.doNotShowLoadingScreen) {
        loadingScreen.startLoading()
    }
    return config
})

service.interceptors.response.use((response) => {
    loadingScreen.endLoading()

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
    return response
}, (error) => {
    loadingScreen.endLoading()

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
