import { reactive } from "vue";

const globalState = reactive({
    languages: {},
    voiceLanguages: {},
    config: {}
});

export default globalState;