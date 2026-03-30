import request from "@/utils/request";
import axios from "axios";
import { withCache } from "@/utils/requestCache";

const queryBaidu = (keyword) => {
    return {
        "k": "KEYWORD",
        "v": "KEYWORD EXPLAIN"
    };
    // return request.post("/api/baiduQuery", {
    //     keyword: keyword
    // });
};

const queryByKeyword = (
    keyword,
    langCode,
    speaker,
    page = 1,
    pageSize = 50,
    voiceFilter = "all",
    createdVersion = "",
    updatedVersion = "",
) => {
    return request.post("/api/keywordQuery", {
        keyword: keyword,
        langCode: langCode,
        speaker: speaker,
        page: page,
        pageSize: pageSize,
        voiceFilter: voiceFilter,
        createdVersion: createdVersion,
        updatedVersion: updatedVersion,
    });
};

/**
 *
 * @param voicePath
 * @param langCode
 * @return {Promise<ArrayBuffer|null>}
 */
const getVoiceOver = async (voicePath, langCode) => {
    const voiceOverUrl = request.defaults.baseURL
        ? new URL("/api/getVoiceOver", request.defaults.baseURL).toString()
        : "/api/getVoiceOver";

    const ans = await axios.post(voiceOverUrl, {
        voicePath: voicePath,
        langCode: parseInt(langCode)
    }, {
        responseType: "arraybuffer",
    });

    if (ans.headers?.error === "True") {
        return null;
    }

    if (!ans.data || ans.data.byteLength === 0) {
        return null;
    }

    return ans.data;
};

const getTalkFromHash = withCache((textHash, searchLang, page = null, pageSize = 200) => {
    return request.post("/api/getTalkFromHash", {
        "textHash": textHash,
        "searchLang": searchLang,
        "page": page,
        "pageSize": pageSize,
    });
});

const getDialogueGroup = withCache((talkId, coopQuestId, dialogueIdFallback, searchLang, page = 1, pageSize = 200) => {
    return request.post("/api/getDialogueGroup", {
        talkId: talkId,
        coopQuestId: coopQuestId,
        dialogueIdFallback: dialogueIdFallback,
        searchLang: searchLang,
        page: page,
        pageSize: pageSize,
    });
});

const getSubtitleContext = withCache((fileName, subtitleId, searchLang) => {
    return request.post("/api/getSubtitleContext", {
        "fileName": fileName,
        "subtitleId": subtitleId,
        "searchLang": searchLang
    });
});

const searchByName = (
    keyword,
    langCode,
    createdVersion = "",
    updatedVersion = "",
    questSourceType = "",
    speakerKeyword = "",
    readableCategory = "",
) => {
    return request.post("/api/nameSearch", {
        keyword: keyword,
        langCode: langCode,
        createdVersion: createdVersion,
        updatedVersion: updatedVersion,
        questSourceType: questSourceType,
        speakerKeyword: speakerKeyword,
        readableCategory: readableCategory,
    });
};

const searchNpcDialogues = (keyword, langCode, npcCreatedVersion = "", npcUpdatedVersion = "") => {
    return request.post("/api/npcDialogueSearch", {
        keyword: keyword,
        langCode: langCode,
        npcCreatedVersion: npcCreatedVersion,
        npcUpdatedVersion: npcUpdatedVersion,
    });
};

const getNpcDialogues = (npcIds, searchLang, dialogueCreatedVersion = "", dialogueUpdatedVersion = "", page = 1, pageSize = 20) => {
    return request.post("/api/npcDialogues", {
        npcIds: npcIds,
        searchLang: searchLang,
        dialogueCreatedVersion: dialogueCreatedVersion,
        dialogueUpdatedVersion: dialogueUpdatedVersion,
        page: page,
        pageSize: pageSize,
    });
};

const searchAvatar = (keyword, langCode) => {
    return request.post("/api/avatarSearch", {
        keyword: keyword,
        langCode: langCode
    });
};

const getAvatarVoices = withCache((avatarId, searchLang) => {
    return request.post("/api/avatarVoice", {
        avatarId: avatarId,
        searchLang: searchLang
    });
});

const searchAvatarVoices = (titleKeyword, createdVersion, updatedVersion, searchLang) => {
    return request.post("/api/avatarVoiceSearch", {
        titleKeyword: titleKeyword,
        createdVersion: createdVersion,
        updatedVersion: updatedVersion,
        searchLang: searchLang
    });
};

const getAvatarStories = withCache((avatarId, searchLang) => {
    return request.post("/api/avatarStory", {
        avatarId: avatarId,
        searchLang: searchLang
    });
});

const searchAvatarStories = (titleKeyword, createdVersion, updatedVersion, searchLang) => {
    return request.post("/api/avatarStorySearch", {
        titleKeyword: titleKeyword,
        createdVersion: createdVersion,
        updatedVersion: updatedVersion,
        searchLang: searchLang
    });
};

const getReadableContent = withCache((readableId, fileName, searchLang) => {
    return request.post("/api/getReadableContent", {
        readableId: readableId,
        fileName: fileName,
        searchLang: searchLang
    });
});

const getQuestDialogues = withCache((questId, searchLang, page = 1, pageSize = 200) => {
    return request.post("/api/getQuestDialogues", {
        questId: questId,
        searchLang: searchLang,
        page: page,
        pageSize: pageSize
    });
});

export default {
    queryBaidu,
    queryByKeyword,
    getVoiceOver,
    getTalkFromHash,
    getDialogueGroup,
    getSubtitleContext,
    searchByName,
    searchNpcDialogues,
    getNpcDialogues,
    searchAvatar,
    getAvatarVoices,
    searchAvatarVoices,
    getAvatarStories,
    searchAvatarStories,
    getReadableContent,
    getQuestDialogues
};
