<script setup>
//这是个展示用户的头像、用户名、用户组的组件，具体参考首页左上角
// 传入一个对象，至少要有user_id、user_name、user_group、avatar_url、verified这5个属性
import globalData from "@/global/global.js"
import router from "@/router";
import { computed } from "vue";
const props = defineProps({
    userInfo: Object,
    showAvatarBorder: Boolean,
})

let userGroupNameDict = {
    "none": "点击登录",
    "normal": "普通用户",
    "admin": "管理员"
}

const displayName = computed(() => {
    return props.userInfo?.stu_name || props.userInfo?.user_name || "未登录"
})

const displayGroup = computed(() => {
    const group = props.userInfo?.grade || props.userInfo?.user_group || "none"
    return userGroupNameDict[group] || group
})

const avatarSrc = computed(() => {
    return props.userInfo?.avatar_url || "/webstatic/defaultAvatar.jpg"
})

const click = () => {
    if(props.userInfo.user_id === globalData.userInfo.user_id){
        if(globalData.login) router.push("/user")
    }else{
        router.push("/user/" + props.userInfo.user_id)
    }

}

</script>

<template>
    <div class="avatarHolder" @click="click">
        <el-avatar class="avatar" :size="58" :src="avatarSrc" :class="{showAvatarBorder: showAvatarBorder}"/>
        <div class="userInfoHolder">
            <div class="userEyebrow">旅行者档案</div>
            <div class="userName">{{ displayName }}</div>
            <div class="userGroup"><span>{{ displayGroup }}</span><i class="fi fi-ss-hexagon-check verifiedIcon" :class="{notVisible:!userInfo.verified}"></i></div>
        </div>
    </div>
</template>

<style scoped>

.avatarHolder{
    display: inline-flex;
    align-items: center;
    flex-direction: row;
    width: 100%;
    padding: 16px;
    border-radius: 22px;
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.72), rgba(243, 231, 211, 0.52));
    border: 1px solid rgba(190, 164, 124, 0.26);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.55);
    cursor: pointer;
    transition: transform 0.18s ease, box-shadow 0.18s ease;
}

.avatarHolder:hover {
    transform: translateY(-1px);
    box-shadow: 0 12px 22px rgba(44, 57, 54, 0.10);
}

.el-avatar{
    min-width: 58px;
}

.userInfoHolder{
    display: flex;
    flex-direction: column;
    gap: 2px;
}


.avatarHolder .avatar{
    margin-right: 14px;
}
.showAvatarBorder {
    border: 4px rgba(var(--theme-primary-rgb), 0.18) solid;
}

.userEyebrow {
    color: var(--theme-primary);
    font-size: 11px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
}

.avatarHolder .userName{
    font-weight: 700;
    font-size: 17px;
    color: var(--theme-ink);
}

.avatarHolder .userGroup{
    font-size: 12px;
    color: var(--theme-text-muted);
    line-height: 1.5;
}

.verifiedIcon{
    color: var(--theme-accent);
    visibility: visible;
    vertical-align: middle;
    margin-left: 5px;
}

.notVisible{
    visibility: hidden;
}

</style>
