<script setup>

import router from "@/router";
import { nextTick, onMounted, reactive, ref, watch } from "vue";
import { ElMenuItem, ElSubMenu } from "element-plus";
import global from "@/global/global";
import api from "@/api/basicInfo";

const menuItemClick = (ke) => {
    router.push(ke.index)
}

const menus = reactive({
    v: [
        { "title": "文本检索", "icon": "fi-rr-search", "path": "/" },
        { "title": "任务/阅读物查询", "icon": "fi-rr-book", "path": "/name-search" },
        { "title": "NPC 对话查询", "icon": "fi-rr-comment", "path": "/npc-dialogue-search" },
        { "title": "角色语音查询", "icon": "fi-rr-volume", "path": "/voice-search" },
        { "title": "角色故事查询", "icon": "fi-rr-book-open-cover", "path": "/story-search" },
        { "title": "图鉴搜索", "icon": "fi-rr-apps", "path": "/catalog-search" },
        { "title": "设置", "icon": "fi-rr-settings", "path": "/settings" },
    ]
});
const loadComplete = ref(true);

const getSidebarPath = () => {
    let path = router.currentRoute.value.path.split("/")
    if (path.length === 1) {
        return ""
    } else {
        return "/" + path[1];
    }


}

const menu = ref();
let contentDom = undefined;
const loaded = ref(false)
const initError = ref("")
const scrollPositions = new Map()
const detailOriginRouteKey = ref(null)
const detailRouteNames = new Set(["talkView", "entityView"])

const getContentDom = () => {
    if (contentDom && document.body.contains(contentDom)) {
        return contentDom
    }
    contentDom = document.querySelector(".content")
    return contentDom
}

const waitForContentPaint = async () => {
    await nextTick()
    await new Promise((resolve) => requestAnimationFrame(resolve))
}

const getInitErrorMessage = (error) => {
    const message = error?.response?.data?.msg
    if (message) {
        return message
    }
    if (error?.message) {
        return error.message
    }
    return "Failed to load application data."
}

onMounted(async () => {
    (() => {
        let menuItemNow = getSidebarPath();
        for (let item of menus.v) {
            if (!item.children) continue;
            for (let child of item.children) {
                if (child.path === menuItemNow) {
                    menu.value.open(item.path);
                }
            }
        }
        contentDom = document.querySelector(".content")
    })()

    try {
        global.languages = (await api.getImportedTextLanguages()).json
        global.voiceLanguages = (await api.getImportedVoiceLanguages()).json
        global.config = (await api.getConfig()).json
    } catch (error) {
        initError.value = getInitErrorMessage(error)
        if (error?.defaultHandler) {
            error.defaultHandler()
        } else {
            console.error("failed to initialize home page", error)
        }
    } finally {
        loaded.value = true
    }
})

watch(router.currentRoute, async (to, from) => {
    const currentContent = getContentDom()
    if (from?.fullPath && currentContent) {
        scrollPositions.set(from.fullPath, currentContent.scrollTop)
    }

    if (!detailRouteNames.has(String(from?.name || "")) && detailRouteNames.has(String(to?.name || ""))) {
        detailOriginRouteKey.value = from?.fullPath || null
    }

    await waitForContentPaint()

    const nextContent = getContentDom()
    if (!nextContent) {
        detailOriginRouteKey.value = detailRouteNames.has(String(from?.name || "")) ? null : detailOriginRouteKey.value
        return
    }

    if (detailRouteNames.has(String(from?.name || ""))) {
        const shouldRestoreScroll = !!detailOriginRouteKey.value && to?.fullPath === detailOriginRouteKey.value
        if (shouldRestoreScroll) {
            const savedTop = scrollPositions.get(to.fullPath) ?? 0
            nextContent.scrollTo({ left: 0, top: savedTop, behavior: "auto" })
        } else {
            nextContent.scrollTo({ left: 0, top: 0, behavior: "auto" })
        }
        detailOriginRouteKey.value = null
        return
    }

    nextContent.scrollTo({ left: 0, top: 0, behavior: "auto" })
})

</script>

<template>
    <div class="pageWrapper">
        <div class="headerHolder">
            <div class="leftTitle">
                <span class="titleIcon">
                    <i class="fi fi-rr-search"></i>
                </span>
                <span class="titleMain">原神文本搜索</span>
            </div>
        </div>
        <div class="contentHolder">
            <div class="sideBar">
                <div class="sideBarPanel">
                    <el-menu v-if="loadComplete" :default-active="getSidebarPath()" class="sideBarMenu" ref="menu">
                        <component v-for="item in menus.v" :is="item.children ? ElSubMenu : ElMenuItem" :index="item.path"
                            v-on="item.children ? {} : { click: menuItemClick }">
                            <template #title>
                                <i class="fi" :class="item.icon"></i>
                                <span>{{ item.title }}</span>
                            </template>
                            <el-menu-item v-if="item.children" v-for="child in item.children" :index="child.path"
                                @click="menuItemClick">
                                <i class="fi" :class="child.icon"></i>
                                <span>{{ child.title }}</span>
                            </el-menu-item>
                        </component>
                    </el-menu>
                </div>
            </div>

            <div class="content">
                <div v-if="loaded && initError" class="initError">
                    {{ initError }}
                </div>
                <router-view v-slot="{ Component }" v-else-if="loaded">
                    <keep-alive>
                        <component :is="Component" />
                    </keep-alive>
                </router-view>
            </div>
        </div>
    </div>
</template>


<style scoped>
.headerHolder {
    width: 100%;
    min-height: var(--header-height);
    box-sizing: border-box;
    padding: 10px 0;
    background:
        linear-gradient(135deg, rgba(36, 77, 74, 0.96), rgba(47, 105, 101, 0.92)),
        linear-gradient(90deg, rgba(183, 140, 79, 0.12), transparent 30%);
    display: flex;
    justify-content: flex-start;
    align-items: center;
    flex: 0 0 auto;
    box-shadow: 0 12px 24px rgba(31, 48, 45, 0.16);
}

.headerHolder>div {
    display: flex;
    align-items: center;
    margin: 0 var(--header-padding-x);
}

.pageWrapper {
    height: 100vh;
    width: 100%;
    min-width: 0;
    max-height: 100vh;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    background: transparent;
}

.leftTitle {
    gap: 12px;
    color: #fff;
}

.titleIcon {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 42px;
    height: 42px;
    border-radius: 14px;
    font-size: 18px;
    color: rgba(255, 250, 240, 0.96);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.14), rgba(255, 255, 255, 0.05));
    border: 1px solid rgba(255, 255, 255, 0.18);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.18);
}

.titleMain {
    font-family: var(--font-title);
    font-size: clamp(1.35rem, 1.8vw, 1.65rem);
    font-weight: 700;
    letter-spacing: 0.02em;
}

.line {
    border-left: #fff 1px solid;
    height: 1em;
    width: 1px;
    margin: 0 5px;
}

.contentHolder {
    display: flex;
    justify-items: stretch;
    flex-direction: var(--content-direction);
    flex: 1 1 auto;
    overflow: hidden;
    min-width: 0;
    min-height: 0;

}


.content {
    overflow-y: auto;
    overflow-x: hidden;
    background: transparent;
    flex: 1;
    min-height: 0;
    width: var(--content-width);
    -webkit-overflow-scrolling: var(--content-scroll);
    scrollbar-gutter: stable;
    padding-right: var(--content-scrollbar-padding);
    padding: 14px 22px 30px 0;
}

.content.dialogueContent {
    overflow-x: auto;
}

.initError {
    margin: 24px;
    padding: 16px 18px;
    border: 1px solid var(--el-color-danger-light-5);
    border-radius: 12px;
    background-color: var(--el-color-danger-light-9);
    color: var(--el-color-danger);
    line-height: 1.6;
}

.sideBar {
    width: var(--sidebar-width);
    min-width: var(--sidebar-min-width);
    max-width: var(--sidebar-max-width);
    flex: 0 0 auto;
    align-self: var(--sidebar-align-self);
    display: var(--sidebar-display);
    align-items: var(--sidebar-align-items);
    flex-wrap: var(--sidebar-wrap);
    border-bottom: var(--sidebar-border);
    gap: var(--sidebar-gap);
}

.sideBarPanel {
    margin: 14px 0 14px 18px;
    padding: 10px 10px 8px;
    border-radius: 24px;
    background:
        linear-gradient(180deg, rgba(255, 253, 248, 0.95), rgba(247, 240, 229, 0.95));
    border: 1px solid rgba(190, 164, 124, 0.36);
    box-shadow: 0 14px 26px rgba(44, 57, 54, 0.10);
}

.sideBar .sideBarMenu {
    border-right: none;
    display: var(--sidebarmenu-display);
    flex-direction: var(--sidebarmenu-direction);
    flex-wrap: var(--sidebarmenu-wrap);
    max-width: var(--sidebarmenu-max-width);
    padding: var(--sidebarmenu-padding);
    gap: var(--sidebarmenu-gap);
}

:deep(.sideBarMenu .el-menu-item),
:deep(.sideBarMenu .el-sub-menu__title) {
    padding: var(--sidebarmenu-item-padding);
    margin-bottom: 6px;
    min-height: 48px;
    color: var(--theme-text);
    border: 1px solid transparent;
    transition: background-color 0.18s ease, color 0.18s ease, transform 0.18s ease, border-color 0.18s ease;
}

:deep(.sideBarMenu .el-menu-item:hover),
:deep(.sideBarMenu .el-sub-menu__title:hover) {
    background: rgba(47, 105, 101, 0.08);
    color: var(--theme-primary);
    transform: translateX(2px);
}

:deep(.sideBarMenu .el-menu-item.is-active) {
    background: linear-gradient(135deg, rgba(47, 105, 101, 0.12), rgba(183, 140, 79, 0.10));
    color: var(--theme-primary-strong);
    border-color: rgba(47, 105, 101, 0.20);
    box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.36);
}

.sideBar .sideBarMenu i {
    margin-right: var(--sidebarmenu-icon-margin);
    font-size: 1.1em;
}

@media (max-width: 720px) {
    .headerHolder {
        padding: 8px 0;
    }

    .leftTitle {
        gap: 10px;
    }

    .titleIcon {
        width: 36px;
        height: 36px;
        border-radius: 12px;
        font-size: 15px;
    }

    .sideBarPanel {
        margin: 0;
        padding: 8px 8px 4px;
        border-radius: 20px;
    }

    .content {
        padding: 0 0 24px;
    }

    :deep(.sideBarMenu .el-sub-menu) {
        flex: 0 0 auto;
    }

    :deep(.sideBarMenu .el-menu-item),
    :deep(.sideBarMenu .el-sub-menu__title) {
        margin-bottom: 0;
        min-height: 42px;
    }
}
</style>
