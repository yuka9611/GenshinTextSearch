import { ref } from 'vue'

const useAudioPlayer = () => {
  const voicePlayer = ref(null)
  const showPlayer = ref(false)
  const audio = ref([])
  let firstShowPlayer = true
  
  const onHidePlayerButtonClicked = () => {
    showPlayer.value = false
  }
  
  const onShowPlayerButtonClicked = () => {
    showPlayer.value = true
  }
  
  const onVoicePlay = (voiceUrl) => {
    if (firstShowPlayer) {
      showPlayer.value = true
      firstShowPlayer = false
    }

    if (audio.value.length > 0 && voiceUrl === audio.value[0]) {
      if (voicePlayer.value && voicePlayer.value.isPlaying) {
        voicePlayer.value.pause()
      } else if (voicePlayer.value) {
        voicePlayer.value.play()
      }
    } else {
      audio.value = [voiceUrl]
      setTimeout(() => {
        if (voicePlayer.value) {
          voicePlayer.value.play()
        }
      }, 0)
    }
  }
  
  const pauseAudio = () => {
    if (voicePlayer.value) {
      voicePlayer.value.pause()
    }
  }
  
  return {
    // 状态
    voicePlayer,
    showPlayer,
    audio,
    
    // 方法
    onHidePlayerButtonClicked,
    onShowPlayerButtonClicked,
    onVoicePlay,
    pauseAudio
  }
}

export default useAudioPlayer
