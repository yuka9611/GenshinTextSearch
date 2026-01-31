import request from "@/utils/request";
import axios from "axios";

const queryBaidu = (keyword) => {
    return {
        "k": "KEYWORD",
        "v": "KEYWORD EXPLAIN"
    }
    // return request.post("/api/baiduQuery", {
    //     keyword: keyword
    // });
};

const queryByKeyword = (keyword, langCode, speaker) => {
    // return {contents: [
    //         {
    //             "type": "Dialogue",
    //             "origin": "TASK NAME etc.",
    //             "voicePaths": [],
    //             "translates":{
    //                 1: "TRANSLATE_CHINESE",
    //                 4: "TRANSLATE_ENGLISH"
    //             }
    //         },
    //         {
    //             "type": "Fetter",
    //             "origin": "AVATAR NAME etc.",
    //             "voicePaths": ["VOICE_PATH2"],
    //             "translates":{
    //                 1: "TRANSLATE_CHINESE2",
    //                 4: "TRANSLATE_ENGLISH2"
    //             }
    //         }
    //     ],
    //     time: 0.01
    // }

    return request.post("/api/keywordQuery", {
        keyword: keyword,
        langCode: langCode,
        speaker: speaker
    });
};

/**
 *
 * @param voicePath
 * @param langCode
 * @return {Promise<ArrayBuffer|null>}
 */
const getVoiceOver = async (voicePath, langCode) => {

    let ans = await axios.post(request.defaults.baseURL ? request.defaults.baseURL: "" + "api/getVoiceOver", {
        voicePath: voicePath,
        langCode: parseInt(langCode)
    }, {
        responseType: 'arraybuffer',
    });

    if(ans.headers.has("Error")) {
        console.log("戳了")
        return null
    }

    return ans.data
};




const getTalkFromHash = (textHash, searchLang) => {
    return request.post("/api/getTalkFromHash", {
        "textHash": textHash,
        "searchLang": searchLang
    });
};

const getSubtitleContext = (fileName, subtitleId, searchLang) => {
    return request.post("/api/getSubtitleContext", {
        "fileName": fileName,
        "subtitleId": subtitleId,
        "searchLang": searchLang
    });
};

const searchByName = (keyword, langCode) => {
    return request.post("/api/nameSearch", {
        keyword: keyword,
        langCode: langCode
    });
};

const searchAvatar = (keyword, langCode) => {
    return request.post("/api/avatarSearch", {
        keyword: keyword,
        langCode: langCode
    });
};

const getAvatarVoices = (avatarId, searchLang) => {
    return request.post("/api/avatarVoice", {
        avatarId: avatarId,
        searchLang: searchLang
    });
};

const getAvatarStories = (avatarId, searchLang) => {
    return request.post("/api/avatarStory", {
        avatarId: avatarId,
        searchLang: searchLang
    });
};

const getReadableContent = (readableId, fileName, searchLang) => {
    return request.post("/api/getReadableContent", {
        readableId: readableId,
        fileName: fileName,
        searchLang: searchLang
    });
};

const getQuestDialogues = (questId, searchLang) => {
    return request.post("/api/getQuestDialogues", {
        questId: questId,
        searchLang: searchLang
    });
};


export default {
    queryBaidu,
    queryByKeyword,
    getVoiceOver,
    getTalkFromHash,
    getSubtitleContext,
    searchByName,
    searchAvatar,
    getAvatarVoices,
    getAvatarStories,
    getReadableContent,
    getQuestDialogues
};
