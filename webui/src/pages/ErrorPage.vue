<script setup>
import {changeTheme} from "@/assets/changeTheme";
import router from "@/router";

const goBack = () =>{
    router.go(-1)
}

</script>

<template>
    <div class="pageWrapper">
        <div class="contentWrapper">
            <div class="errorBadge">Archive Missing</div>
            <div class="errorCode">404</div>
            <div class="errorMsg"><i class="fi fi-sr-info"></i><span> 啊呀，你请求的页面未找到。</span></div>
            <el-button type="primary" @click="goBack" round>返回上一页</el-button>
        </div>


    </div>
</template>


<style scoped>
.pageWrapper {
    height: 100vh;
    width: 100vw;
    box-sizing: border-box;
    background:
        radial-gradient(circle at top right, rgba(47, 105, 101, 0.16), transparent 24%),
        linear-gradient(180deg, rgba(255, 252, 247, 0.96), rgba(241, 231, 214, 0.95));
    display: flex;
    justify-content: center;
    align-items: center;
}

.contentWrapper{
    text-align: center;
    padding: 36px 42px;
    border-radius: 30px;
    background: linear-gradient(180deg, rgba(255, 253, 248, 0.98), rgba(248, 240, 228, 0.95));
    border: 1px solid rgba(190, 164, 124, 0.34);
    box-shadow: 0 24px 40px rgba(44, 57, 54, 0.14);
}

.errorBadge {
    display: inline-flex;
    align-items: center;
    min-height: 34px;
    padding: 0 14px;
    margin-bottom: 12px;
    border-radius: 999px;
    background: rgba(47, 105, 101, 0.08);
    color: var(--theme-primary);
    font-size: 12px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
}

.errorCode{
    font-size: clamp(6rem, 14vw, 9rem);
    font-weight: 700;
    color: var(--theme-primary);
    font-family: var(--font-title);
    line-height: 1;
}

.errorMsg{
    font-size: 20px;
    margin: 18px 0 22px;
    color: var(--theme-text-muted);
}
</style>
