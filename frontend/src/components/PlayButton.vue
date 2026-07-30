<script setup>
import { ref, watch, onUnmounted } from 'vue'

const props = defineProps({
  audioData: {
    type: [ArrayBuffer, Uint8Array, String],
    required: true
  }
})

const audioRef = ref(null)
const audioUrl = ref('')
const isPlaying = ref(false)
const progress = ref(0)

const createBlobUrl = () => {
  if (audioUrl.value) URL.revokeObjectURL(audioUrl.value)

  let blob
  if (typeof props.audioData === 'string') {
    const binary = atob(props.audioData)
    const bytes = new Uint8Array(binary.length)
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i)
    }
    blob = new Blob([bytes], { type: 'audio/ogg; codecs=opus' })
  } else {
    blob = new Blob([props.audioData], { type: 'audio/ogg; codecs=opus' })
  }

  audioUrl.value = URL.createObjectURL(blob)
}

watch(() => props.audioData, createBlobUrl, { immediate: true })

const togglePlay = () => {
  if (!audioRef.value) return
  if (isPlaying.value) {
    audioRef.value.pause()
  } else {
    audioRef.value.play()
  }
}

const onTimeUpdate = () => {
  if (audioRef.value && audioRef.value.duration) {
    progress.value = (audioRef.value.currentTime / audioRef.value.duration) * 100
  }
}

const onEnded = () => {
  isPlaying.value = false
  progress.value = 0
}

onUnmounted(() => {
  if (audioUrl.value) URL.revokeObjectURL(audioUrl.value)
})
</script>

<template>
  <div class="audio-bubble">
    <audio
      ref="audioRef"
      :src="audioUrl"
      @play="isPlaying = true"
      @pause="isPlaying = false"
      @timeupdate="onTimeUpdate"
      @ended="onEnded"
    ></audio>

    <button @click="togglePlay" class="play-btn">
      <span v-if="isPlaying">⏸</span>
      <span v-else>▶</span>
    </button>

    <div class="progress-bar">
      <div class="fill" :style="{ width: progress + '%' }"></div>
    </div>
  </div>
</template>

<style scoped>
.audio-bubble {
  display: flex;
  align-items: center;
  gap: 12px;
  background: rgba(255, 255, 255, 0.05);
  padding: 8px 14px;
  border-radius: 12px;
  min-width: 200px;
}

.play-btn {
  background: #3b82f6;
  border: none;
  color: white;
  width: 32px;
  height: 32px;
  border-radius: 50%;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
}

.progress-bar {
  flex: 1;
  height: 4px;
  background: rgba(255, 255, 255, 0.2);
  border-radius: 2px;
  overflow: hidden;
}

.fill {
  height: 100%;
  background: #3b82f6;
  transition: width 0.1s linear;
}
