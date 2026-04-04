<script setup>

import router from "@/router";
import { nextTick, onMounted, reactive, ref, watch, computed } from "vue";
import { ElMenuItem, ElSubMenu } from "element-plus";
import global from "@/global/global";
import api from "@/api/basicInfo";
import { toggleTheme, getTheme } from "@/assets/changeTheme";

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

const isDark = ref(getTheme() === 'dark')
const onToggleTheme = () => {
    const next = toggleTheme()
    isDark.value = next === 'dark'
    global.theme = next
}

const bottomNavTabs = [
    { title: "文本检索", icon: "fi-rr-search", path: "/" },
    { title: "任务查询", icon: "fi-rr-book", path: "/name-search" },
    { title: "NPC对话", icon: "fi-rr-comment", path: "/npc-dialogue-search" },
    { title: "语音", icon: "fi-rr-volume", path: "/voice-search" },
    { title: "更多", icon: "fi-rr-menu-dots", path: "__more__" },
]

const moreMenuItems = [
    { title: "角色故事查询", icon: "fi-rr-book-open-cover", path: "/story-search" },
    { title: "图鉴搜索", icon: "fi-rr-apps", path: "/catalog-search" },
    { title: "设置", icon: "fi-rr-settings", path: "/settings" },
]
const moreMenuVisible = ref(false)

const activeBottomPath = computed(() => {
    const seg = "/" + (router.currentRoute.value.path.split("/")[1] || "")
    const resolved = seg === "/" ? "/" : seg
    return resolved
})

const isMoreActive = computed(() => {
    return ['/story-search', '/catalog-search', '/settings'].includes(activeBottomPath.value)
})

const onBottomNavClick = (tab) => {
    if (tab.path === '__more__') {
        moreMenuVisible.value = !moreMenuVisible.value
    } else {
        moreMenuVisible.value = false
        router.push(tab.path)
    }
}

const onMoreItemClick = (item) => {
    moreMenuVisible.value = false
    router.push(item.path)
}

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
    <div class="pageWrapper" @click="moreMenuVisible = false">
        <div class="headerHolder">
            <div class="leftTitle">
                <span class="titleIcon">
                    <i class="fi fi-rr-search"></i>
                </span>
                <span class="titleMain">原神文本搜索</span>
            </div>
            <div class="headerRight">
                <button class="themeToggleBtn" @click="onToggleTheme" :title="isDark ? '切换至浅色模式' : '切换至暗黑模式'">
                    <i class="fi" :class="isDark ? 'fi-rr-sun' : 'fi-rr-moon'"></i>
                </button>
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
                                <span class="menuItemLabel">{{ item.title }}</span>
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

        <nav class="bottomNav" @click.stop>
            <div
                v-for="tab in bottomNavTabs"
                :key="tab.path"
                class="bottomNavItem"
                :class="{ active: tab.path === '__more__' ? isMoreActive : activeBottomPath === tab.path }"
                @click="onBottomNavClick(tab)"
            >
                <i class="fi" :class="tab.icon"></i>
                <span>{{ tab.title }}</span>
            </div>
            <Transition name="moreMenu">
                <div v-if="moreMenuVisible" class="morePopup" @click.stop>
                    <div
                        v-for="item in moreMenuItems"
                        :key="item.path"
                        class="morePopupItem"
                        :class="{ active: activeBottomPath === item.path }"
                        @click="onMoreItemClick(item)"
                    >
                        <i class="fi" :class="item.icon"></i>
                        <span>{{ item.title }}</span>
                    </div>
                </div>
            </Transition>
        </nav>
    </div>
</template>


<style scoped>
.headerHolder {
    width: 100%;
    min-height: var(--header-height);
    box-sizing: border-box;
    padding: 0;
    background:
        linear-gradient(135deg, rgba(36, 77, 74, 0.96), rgba(47, 105, 101, 0.92)),
        linear-gradient(90deg, rgba(183, 140, 79, 0.12), transparent 30%);
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex: 0 0 auto;
    box-shadow: 0 12px 24px rgba(31, 48, 45, 0.16);
    position: relative;
    z-index: 100;
}

.headerHolder>div {
    display: flex;
    align-items: center;
    margin: 0 var(--header-padding-x);
}

.headerRight {
    gap: 8px;
}

.themeToggleBtn {
    width: 38px;
    height: 38px;
    border-radius: 10px;
    border: 1px solid rgba(255, 255, 255, 0.18);
    background: rgba(255, 255, 255, 0.08);
    color: rgba(255, 255, 255, 0.85);
    font-size: 16px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    transition: background 0.15s ease, transform 0.15s ease;
    font-family: inherit;
}

.themeToggleBtn:hover {
    background: rgba(255, 255, 255, 0.18);
    color: #fff;
    transform: translateY(-1px);
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
    border-left: rgba(255, 255, 255, 0.45) 1px solid;
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
    padding: var(--content-padding);
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
    padding: var(--sidebar-padding);
    box-sizing: border-box;
    overflow-y: auto;
    align-self: var(--sidebar-align-self);
    display: var(--sidebar-display);
    align-items: var(--sidebar-align-items);
    flex-wrap: var(--sidebar-wrap);
    border-bottom: var(--sidebar-border);
    gap: var(--sidebar-gap);
}

.sideBarPanel {
    margin: 0;
    padding: var(--sidebar-panel-padding);
    border-radius: 24px;
    background:
        linear-gradient(180deg, rgba(255, 253, 248, 0.95), rgba(247, 240, 229, 0.95));
    border: 1px solid rgba(190, 164, 124, 0.36);
    box-shadow: 0 14px 26px rgba(44, 57, 54, 0.10);
    transition: background 0.25s ease, border-color 0.25s ease;
}



.sideBar .sideBarMenu {
    border-right: none;
    background-color: transparent;
    --el-menu-bg-color: transparent;
    display: var(--sidebarmenu-display);
    flex-direction: var(--sidebarmenu-direction);
    flex-wrap: var(--sidebarmenu-wrap);
    max-width: var(--sidebarmenu-max-width);
    padding: var(--sidebarmenu-padding);
    gap: var(--sidebarmenu-gap);
}

:deep(.sideBarMenu.el-menu) {
    background-color: transparent !important;
    --el-menu-bg-color: transparent;
}

:deep(.sideBarMenu .el-sub-menu .el-menu) {
    background-color: transparent !important;
}

:deep(.sideBarMenu .el-menu-item),
:deep(.sideBarMenu .el-sub-menu__title) {
    background-color: transparent;
    padding: var(--sidebarmenu-item-padding);
    margin-bottom: 6px;
    min-height: 48px;
    justify-content: flex-start;
    color: var(--theme-text);
    border: 1px solid transparent;
    transition: background-color 0.18s ease, color 0.18s ease, transform 0.18s ease, border-color 0.18s ease;
}

:deep(.sideBarMenu .el-menu-item:hover),
:deep(.sideBarMenu .el-sub-menu__title:hover) {
    background: rgba(47, 105, 101, 0.06);
    color: var(--theme-primary);
    transform: translateX(2px);
}

:deep(.sideBarMenu .el-menu-item.is-active) {
    background: #e1ede9;
    color: var(--theme-primary-strong);
    border-color: rgba(47, 105, 101, 0.26);
    font-weight: 600;
    position: relative;
}

:deep(.sideBarMenu .el-menu-item.is-active)::after {
    content: '';
    position: absolute;
    left: 0;
    top: 50%;
    transform: translateY(-50%);
    width: 3px;
    height: 60%;
    border-radius: 0 3px 3px 0;
    background: var(--theme-primary);
}



.sideBar .sideBarMenu i {
    margin-right: var(--sidebarmenu-icon-margin);
    font-size: 1.1em;
    width: 32px;
    height: 32px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 10px;
    transition: background 0.15s ease;
}

:deep(.sideBarMenu .el-menu-item:hover) i,
:deep(.sideBarMenu .el-menu-item.is-active) i {
    background: rgba(47, 105, 101, 0.10);
}

/* Narrow window — sidebar collapses to icon-only */
@media (max-width: 860px) {
    .sideBarPanel {
        padding: var(--sidebar-panel-padding);
        border-radius: 24px;
    }

    :deep(.sideBarMenu .el-menu-item),
    :deep(.sideBarMenu .el-sub-menu__title) {
        padding: 0 !important;
        justify-content: center;
        min-height: 46px;
        margin-bottom: 4px;
    }

    .menuItemLabel {
        display: none;
    }

    .sideBar .sideBarMenu i {
        margin-right: 0;
    }
}

.bottomNav {
    display: none;
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    height: 64px;
    z-index: 200;
    background: rgba(255, 253, 248, 0.92);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border-top: 1px solid rgba(190, 164, 124, 0.24);
    box-shadow: 0 -4px 16px rgba(44, 57, 54, 0.08);
    padding-bottom: env(safe-area-inset-bottom, 0);
    justify-content: space-around;
    align-items: center;
}

.bottomNavItem {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 3px;
    flex: 1;
    height: 100%;
    cursor: pointer;
    color: var(--theme-text-secondary, #666);
    font-size: 10px;
    font-weight: 500;
    user-select: none;
    transition: color 0.18s ease;
    -webkit-tap-highlight-color: transparent;
}

.bottomNavItem:active {
    opacity: 0.7;
}

.bottomNavItem i {
    font-size: 18px;
    transition: transform 0.18s ease;
}

.bottomNavItem.active {
    color: var(--theme-primary);
    font-weight: 600;
}

.bottomNavItem.active i {
    transform: scale(1.1);
}

.morePopup {
    position: absolute;
    bottom: calc(100% + 8px);
    right: 8px;
    min-width: 180px;
    background: rgba(255, 253, 248, 0.96);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border-radius: 16px;
    border: 1px solid rgba(190, 164, 124, 0.24);
    box-shadow: 0 12px 32px rgba(44, 57, 54, 0.14);
    padding: 8px;
    z-index: 210;
}

.morePopupItem {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 14px;
    border-radius: 10px;
    cursor: pointer;
    font-size: 13px;
    font-weight: 500;
    color: var(--theme-text-secondary, #666);
    transition: background 0.15s ease, color 0.15s ease;
}

.morePopupItem:hover {
    background: rgba(47, 105, 101, 0.06);
}

.morePopupItem.active {
    color: var(--theme-primary);
    font-weight: 600;
}

.morePopupItem i {
    font-size: 16px;
    width: 28px;
    height: 28px;
    border-radius: 8px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
}

.morePopupItem.active i {
    background: rgba(47, 105, 101, 0.10);
}

.moreMenu-enter-active,
.moreMenu-leave-active {
    transition: opacity 0.2s ease, transform 0.2s ease;
}
.moreMenu-enter-from,
.moreMenu-leave-to {
    opacity: 0;
    transform: translateY(8px);
}

/* Very narrow window — sidebar hidden */
@media (max-width: 680px) {
    .titleIcon {
        width: 36px;
        height: 36px;
        border-radius: 12px;
        font-size: 15px;
    }

    .content {
        padding: var(--content-padding);
    }

    .bottomNav {
        display: flex;
    }
}
</style>

<style>
/* Dark-mode overrides — must be unscoped because Vue scoped CSS
   compiles :global([data-theme="dark"]) .class into just [data-theme=dark],
   losing the component class selector entirely. */
[data-theme="dark"] .themeToggleBtn {
    border-color: rgba(255, 255, 255, 0.10);
    background: rgba(255, 255, 255, 0.06);
    color: rgba(255, 255, 255, 0.70);
}
[data-theme="dark"] .themeToggleBtn:hover {
    background: rgba(255, 255, 255, 0.14);
    color: rgba(255, 255, 255, 0.95);
}
[data-theme="dark"] .leftTitle {
    color: rgba(220, 230, 228, 0.92);
}
[data-theme="dark"] .titleIcon {
    color: rgba(200, 220, 215, 0.90);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.08), rgba(255, 255, 255, 0.02));
    border-color: rgba(255, 255, 255, 0.10);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06);
}
[data-theme="dark"] .line {
    border-left-color: rgba(255, 255, 255, 0.18);
}
[data-theme="dark"] .sideBarPanel {
    background: linear-gradient(180deg, rgba(30, 40, 37, 0.95), rgba(22, 32, 30, 0.95));
    border-color: var(--theme-border);
    box-shadow: 0 14px 26px rgba(0, 0, 0, 0.20);
}
[data-theme="dark"] .sideBarMenu .el-menu-item.is-active {
    background: rgba(74, 154, 149, 0.12);
    color: #3a8480;
    border-color: rgba(74, 154, 149, 0.30);
}
[data-theme="dark"] .sideBarMenu .el-menu-item:hover i,
[data-theme="dark"] .sideBarMenu .el-menu-item.is-active i {
    background: rgba(74, 154, 149, 0.12);
}
[data-theme="dark"] .bottomNav {
    background: rgba(22, 30, 28, 0.96);
    border-top-color: rgba(255, 255, 255, 0.08);
    box-shadow: 0 -4px 16px rgba(0, 0, 0, 0.25);
}
[data-theme="dark"] .bottomNavItem {
    color: rgba(200, 210, 208, 0.55);
}
[data-theme="dark"] .bottomNavItem.active {
    color: var(--theme-primary);
}
[data-theme="dark"] .morePopup {
    background: rgba(22, 30, 28, 0.96);
    border-color: rgba(255, 255, 255, 0.08);
    box-shadow: 0 12px 32px rgba(0, 0, 0, 0.30);
}
[data-theme="dark"] .morePopupItem {
    color: rgba(200, 210, 208, 0.55);
}
[data-theme="dark"] .morePopupItem:hover {
    background: rgba(74, 154, 149, 0.08);
}
[data-theme="dark"] .morePopupItem.active {
    color: var(--theme-primary);
}
[data-theme="dark"] .morePopupItem.active i {
    background: rgba(74, 154, 149, 0.12);
}
</style>
