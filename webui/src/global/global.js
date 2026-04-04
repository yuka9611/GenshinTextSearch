import { reactive } from "vue";

const globalState = reactive({
    languages: {},
    voiceLanguages: {},
    config: {},
    theme: document.documentElement.getAttribute('data-theme') || 'light',
});

export default globalState;
