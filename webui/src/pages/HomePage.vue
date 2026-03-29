<script setup>

import { changeTheme } from "@/assets/changeTheme";
import router from "@/router";
import { nextTick, onBeforeMount, onMounted, reactive, ref, watch } from "vue";
import UserInfoCard from "@/components/UserInfoCard.vue";
import globalData from "@/global/global"
import { ElMenuItem, ElSubMenu } from "element-plus";
import global from "@/global/global";
import api from "@/api/basicInfo";

function loginButtonClicked() {
    router.push("/login")
}

const menuItemClick = (ke) => {
    router.push(ke.index)
}


const notificationBox = ref();

const avatarClicked = () => {
    if (!isLogin.value) {
        router.push("/login")
    }
}

const menus = reactive({
    v: [
        { "title": "文本检索", "icon": "fi-rr-search", "path": "/" },
        { "title": "任务/阅读物查询", "icon": "fi-rr-book", "path": "/name-search" },
        { "title": "NPC 对话查询", "icon": "fi-rr-comment", "path": "/npc-dialogue-search" },
        { "title": "角色语音查询", "icon": "fi-rr-volume", "path": "/voice-search" },
        { "title": "角色故事查询", "icon": "fi-rr-book-open-cover", "path": "/story-search" },
        { "title": "设置", "icon": "fi-rr-settings", "path": "/settings" },
    ]
});

let userInfo = reactive({
    data: {
        user_phone: "",
        user_name: "未登录",
        user_id: 123456,
        user_group: "none",
        avatar_url: "/webstatic/defaultAvatar.jpg",
        unread_notification: false,
        verified: false
    }

});

const isLogin = ref(false);
const loadComplete = ref(true);
const gotUserInfo = ref(false)




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
const talkOriginRouteKey = ref(null)

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

    if (from?.name !== "talkView" && to?.name === "talkView") {
        talkOriginRouteKey.value = from?.fullPath || null
    }

    await waitForContentPaint()

    const nextContent = getContentDom()
    if (!nextContent) {
        talkOriginRouteKey.value = from?.name === "talkView" ? null : talkOriginRouteKey.value
        return
    }

    if (from?.name === "talkView") {
        const shouldRestoreScroll = !!talkOriginRouteKey.value && to?.fullPath === talkOriginRouteKey.value
        if (shouldRestoreScroll) {
            const savedTop = scrollPositions.get(to.fullPath) ?? 0
            nextContent.scrollTo({ left: 0, top: savedTop, behavior: "auto" })
        } else {
            nextContent.scrollTo({ left: 0, top: 0, behavior: "auto" })
        }
        talkOriginRouteKey.value = null
        return
    }

    nextContent.scrollTo({ left: 0, top: 0, behavior: "auto" })
})

</script>

<template>
    <div class="pageWrapper">
        <div class="headerHolder">
            <div class="leftTitle">
                <!--                <img alt="" src="../assets/logo.png">-->
                原神文本搜索
            </div>

        </div>
        <div class="contentHolder">
            <div class="sideBar">
                <div class="userInfoWrapper">
                    <UserInfoCard :user-info="userInfo.data" showAvatarBorder @click="avatarClicked"></UserInfoCard>
                </div>


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
    height: 50px;
    box-sizing: border-box;
    background-color: var(--el-color-primary);
    display: flex;
    justify-content: space-between;
    align-items: center;
    max-height: 50px;
    flex: 0 0 auto;
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
}

.leftTitle img {
    max-height: 50px;
    margin-right: 20px;
}

.rightTitle img {
    height: 60px;
}

.rightTitle>* {
    margin: 0 10px;
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
    background-color: var(--el-color-primary-light-9);
    flex: 1;
    min-height: 0;
    width: var(--content-width);
    -webkit-overflow-scrolling: var(--content-scroll);
    scrollbar-gutter: stable;
    padding-right: var(--content-scrollbar-padding);
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
}

.sideBar .sideBarMenu i {
    margin-right: var(--sidebarmenu-icon-margin);
    font-size: 1.1em;
}

.userInfoWrapper {
    padding: var(--user-info-padding);
    border-bottom: var(--user-info-border);
}

.leftTitle {
    color: #fff;
}

@media (max-width: 720px) {
    :deep(.sideBarMenu .el-sub-menu) {
        flex: 0 0 auto;
    }

    .userInfoWrapper {
        flex: 0 0 auto;
    }
}
</style>
