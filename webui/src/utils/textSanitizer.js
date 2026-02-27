const REPLACEMENT_CHAR = /\uFFFD/g
const ZERO_WIDTH_CHARS = /[\u200B-\u200D\uFEFF]/g
const CONTROL_CHARS = /[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g
const BROKEN_CJK_E_JOIN = /([\u4E00-\u9FFF])E(?=[\u4E00-\u9FFF])/g

export const sanitizeText = (value) => {
    if (typeof value !== "string") return value
    return value
        .replace(REPLACEMENT_CHAR, "")
        .replace(ZERO_WIDTH_CHARS, "")
        .replace(CONTROL_CHARS, "")
        .replace(BROKEN_CJK_E_JOIN, "$1,")
}

export const sanitizePayload = (value) => {
    if (typeof value === "string") {
        return sanitizeText(value)
    }
    if (Array.isArray(value)) {
        return value.map((item) => sanitizePayload(item))
    }
    if (value && typeof value === "object") {
        const ans = {}
        Object.keys(value).forEach((key) => {
            ans[key] = sanitizePayload(value[key])
        })
        return ans
    }
    return value
}
