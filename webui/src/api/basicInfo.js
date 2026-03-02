import request from "@/utils/request";
import { withCache } from "@/utils/requestCache";

/**
 * 获得数据库中导入的TextMap语言列表
 */
const getImportedTextLanguages = withCache(() => {
    return request.get("/api/getImportedTextLanguages");
});


/**
 * 获得游戏安装的语音列表
 */
const getImportedVoiceLanguages = withCache(() => {
    return request.get("/api/getImportedVoiceLanguages");
});

const getAvailableVersions = () => {
    // Version aggregation can be heavy on large databases, but we need fresh data
    return request.get("/api/getAvailableVersions", { timeout: 30000 });
};

const saveConfig = (resultLanguages, defaultSearchLanguage, sourceLanguage, isMale) => {
    let tmp = []
    for(let code of resultLanguages){
        tmp.push(parseInt(code))
    }

    return request.post("/api/saveSettings", {
        'config' :{
            "resultLanguages": tmp,
            "defaultSearchLanguage": parseInt(defaultSearchLanguage),
            "sourceLanguage": parseInt(sourceLanguage),
            "isMale": isMale
        }
    })
}

const getConfig = () => {
    return request.get("/api/getSettings")
};

export default {
    getImportedTextLanguages,
    getImportedVoiceLanguages,
    getAvailableVersions,
    getConfig,
    saveConfig
}
