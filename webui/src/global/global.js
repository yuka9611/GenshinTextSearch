import { reactive } from "vue";

const globalState = reactive({
    languages: {},
    voiceLanguages: {},
    config: {},
    runtime: {
        cloudMode: false,
        localFeaturesEnabled: true,
        settingsWritable: true,
        voicePlaybackEnabled: true,
    },
    displayPreferencesRevision: 0,
    theme: document.documentElement.getAttribute('data-theme') || 'light',
});

export default globalState;
